#!/usr/bin/env python3
"""Collect drive info from a REMOTE machine over ssh and merge it into data/smart.json.

Companion to smart_collect.py, which handles the machine netops runs on. This one
runs on the homeserver and reaches out, so nothing has to be installed or changed
on the remote box.

    python3 smart_pull.py <machine-id> [ssh-target]

The ssh target is looked up from config.json by machine id, so it lives in
exactly one place (and never in a tracked file). Pass it explicitly only to
override that.

Two tiers, decided automatically per run:

  capacity-only (default)  `lsblk` + `df` only. Needs no privileges at all — the
                           card shows size/used/percent and a verdict of
                           "unknown", which is honest: netops has no health data.
  full SMART (upgrade)     if `sudo -n smartctl` works on the remote, the same
                           record gains temperature, reallocated sectors, error
                           counts and a real verdict. Enable it with ONE pinned
                           sudoers line on the remote (see README).

The tier is never a flag to remember: the script always tries the upgrade and
silently falls back, so adding the sudoers rule later needs no change here.

`machine-id` MUST match the machine's id in config.json ("macair"), not the
remote's hostname ("tokyomacair") — _smart_blocks() joins on the config id to
borrow the display name, and a mismatch renders an orphan card.
"""
import fcntl
import json
import os
import subprocess
import sys
import time

import smart_collect as sc          # same directory: reuse its formatting helpers

CONFIG_PATH = os.environ.get("NETOPS_CONFIG",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))


def ssh_target(machine_id):
    """The machine's ssh target from config.json, or None. Keeps the real
    user@host out of this file and out of the cron script."""
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return None
    for m in cfg.get("machines") or []:
        if m.get("id") == machine_id:
            return m.get("ssh")
    return None

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "smart.json")
LOCK = os.path.join(os.path.dirname(OUT), ".smart.lock")
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6",
       "-o", "StrictHostKeyChecking=accept-new"]


def run(target, cmd, timeout=25):
    """Remote command -> stdout, or "" on any failure. Never raises: a drive
    report must never take down the cron chain that also feeds the local host."""
    try:
        p = subprocess.run(SSH + [target, cmd], capture_output=True,
                           text=True, timeout=timeout)
        return p.stdout if p.returncode == 0 else ""
    except Exception:
        return ""


def disk_usage(target):
    """{mountpoint: (used_bytes, total_bytes, pct)} from df -kP (POSIX output,
    stable columns). -k is 1024-byte blocks on both Linux and macOS."""
    out, res = run(target, "df -kP"), {}
    for line in out.splitlines()[1:]:
        f = line.split()
        if len(f) < 6 or not f[0].startswith("/dev/"):
            continue
        try:
            total, used = int(f[1]) * 1024, int(f[2]) * 1024
        except ValueError:
            continue
        if not total:
            continue
        res[f[5]] = (used, total, round(used / total * 100))
    return res


def blocks(target):
    """Physical disks from lsblk -J. Partitions stay nested as `children` so
    pick_use() can resolve what the disk is FOR."""
    out = run(target, "lsblk -J -b -o NAME,PATH,TYPE,TRAN,SIZE,MOUNTPOINT,MODEL,ROTA")
    try:
        return [b for b in json.loads(out).get("blockdevices", [])
                if b.get("type") == "disk"]
    except Exception:
        return []


def smart_for(target, path):
    """Full SMART for one device, or None when sudo/smartctl is unavailable.

    `sudo -n` never prompts, so a missing sudoers rule fails instantly instead
    of hanging the cron job on a password prompt.
    """
    out = run(target,
              f"sudo -n /usr/sbin/smartctl --json=c -H -A -i -d auto {path} 2>/dev/null")
    try:
        d = json.loads(out)
    except Exception:
        return None
    if any(m.get("severity") == "error"
           for m in (d.get("smartctl") or {}).get("messages") or []):
        return None
    return d


def kind_of(b, smart):
    if (smart or {}).get("device", {}).get("type") == "nvme" or b.get("tran") == "nvme":
        return "nvme"
    return "hdd" if b.get("rota") else "ssd"


def build_drive(b, usage, smart):
    """One lsblk block (+ optional SMART) -> the drive dict server.py renders."""
    kind = kind_of(b, smart)
    bus = (b.get("tran") or "").lower()
    use = sc.pick_use(b)
    size = b.get("size") or 0
    d = {
        "dev": b.get("name") or "?",
        "name": sc.friendly(b.get("model") or "", kind, bus),
        "model": (b.get("model") or "").strip(),
        "kind": {"hdd": "HDD", "ssd": "SSD", "nvme": "NVMe"}.get(kind, "Disco"),
        "bus": bus.upper() or "—",
        "size": sc.fmt_size(size),
        "use": use,
        # counters the UI prints; None renders as an em dash, never as a zero
        # we did not measure
        "realloc": None, "pending": None, "uncorrect": None,
        "crc": None, "errors": None, "hours": None, "age": "—",
        "wear": None, "temp": None, "passed": None, "critical": False,
    }
    u = usage.get(use) if use else None
    if u:
        d["used"], d["cap"], d["used_pct"] = sc.fmt_size(u[0]), sc.fmt_size(u[1]), u[2]

    if smart is None:
        # Honest degraded state. NOT "good": that would claim SMART says clean.
        d["verdict"] = "unknown"
        d["label"] = "SIN DATOS"
        d["note"] = ("Sin datos SMART (requiere root en la máquina remota) — "
                     "solo capacidad")
        return d

    # full SMART: hand the parsed document to smart_collect's own extractor so
    # the thresholds and wording stay identical to the local host's card
    try:
        sc.fill_from_smart(d, smart)
    except AttributeError:
        _fill_basic(d, smart)
    v = sc.verdict(d)
    d["verdict"], d["label"], d["note"] = v[0], v[1], v[2]
    return d


def _fill_basic(d, s):
    """Minimal SMART extraction, used when smart_collect exposes no shared
    helper. Covers both the NVMe log and the ATA attribute table."""
    d["passed"] = (s.get("smart_status") or {}).get("passed")
    d["temp"] = (s.get("temperature") or {}).get("current")
    nv = s.get("nvme_smart_health_information_log") or {}
    if nv:
        d["hours"] = nv.get("power_on_hours")
        d["wear"] = nv.get("percentage_used")
        d["errors"] = nv.get("media_errors")
        d["critical"] = bool(nv.get("critical_warning"))
        if d["temp"] is None:
            d["temp"] = nv.get("temperature")
    else:
        d["hours"] = (s.get("power_on_time") or {}).get("hours")
        for a in ((s.get("ata_smart_attributes") or {}).get("table") or []):
            raw = (a.get("raw") or {}).get("value")
            n = a.get("name") or ""
            if n == "Reallocated_Sector_Ct":
                d["realloc"] = raw
            elif n == "Current_Pending_Sector":
                d["pending"] = raw
            elif n == "Offline_Uncorrectable":
                d["uncorrect"] = raw
            elif "CRC" in n:
                d["crc"] = raw
    d["age"] = sc.human_age(d["hours"])


def merge(host_id, rec):
    """Read-modify-write data/smart.json under an exclusive lock.

    The lock matters: smart_collect.py does the same read-modify-write for the
    local host, and two unsynchronised writers means the second os.replace wins
    and the first host's key silently vanishes from the panel for an hour.
    """
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(LOCK, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            with open(OUT) as f:
                doc = json.load(f)
            if not isinstance(doc, dict) or not isinstance(doc.get("hosts"), dict):
                raise ValueError
        except (OSError, ValueError):
            doc = {"hosts": {}}
        doc["hosts"][host_id] = rec
        doc["t"] = int(time.time())
        tmp = OUT + ".tmp"
        with open(tmp, "w") as f:
            json.dump(doc, f, indent=1, ensure_ascii=False)
        os.replace(tmp, OUT)               # atomic: readers never see a torn file


def main():
    if len(sys.argv) not in (2, 3):
        print("usage: smart_pull.py <machine-id> [ssh-target]", file=sys.stderr)
        return 2
    host_id = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) == 3 else ssh_target(host_id)
    if not target:
        print(f"no ssh target for {host_id!r} in {CONFIG_PATH} "
              f"(a local machine has ssh:null and needs smart_collect.py, not this)",
              file=sys.stderr)
        return 2

    blks = blocks(target)
    if not blks:
        # unreachable, or lsblk missing (e.g. a macOS target): leave the previous
        # record alone rather than replacing it with an empty one
        print(f"{target}: no block devices readable; leaving smart.json alone",
              file=sys.stderr)
        return 1
    usage = disk_usage(target)
    drives, full = [], 0
    for b in blks:
        s = smart_for(target, b.get("path") or f'/dev/{b.get("name")}')
        full += s is not None
        drives.append(build_drive(b, usage, s))

    merge(host_id, {"host": host_id, "t": int(time.time()),
                    "ok": bool(drives), "drives": drives})
    tier = f"{full}/{len(drives)} with SMART" if full else "capacity-only (no root)"
    print(f"{OUT}: {host_id} {len(drives)} drives, {tier} "
          f"({', '.join(d['dev'] + '=' + d['verdict'] for d in drives)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
