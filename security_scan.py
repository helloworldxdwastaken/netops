#!/usr/bin/env python3
"""ClamAV sweep -> data/clamscan.json for the netops SEGURIDAD screen.

Modes:
  (default)  daily : files modified in the last RECENT_DAYS under DOWNLOADS and
                     HOME (minus caches/VCS/venvs) — catches what just arrived.
  --deep           : weekly full pass of DOWNLOADS + HOME + /etc.

Run by netops-clamscan.timer (daily 04:15, Mon-Sat) and
netops-clamscan-deep.timer (Sun 05:00) as user tokyo, niced to idle — the
timers deliberately never overlap so only one 1.2 GB signature db is ever
resident. Install/enable via security_setup.sh.

Never raises: every failure mode still writes a JSON verdict with an "err"
field, because a scan that dies silently would leave the board trusting
week-old results. server.py turns "err" amber and infected>0 red.

Size caps: clamscan skips files over MAX_FILE. That is intentional — the
multi-GB video files are poor malware carriers and would turn the sweep into
a day-long disk churn; executables/scripts/archives (the real vectors) are
far below the cap.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time

HOME = "/home/tokyo"
DOWNLOADS = "/media/downloads"
OUT = "/home/tokyo/netops/data/clamscan.json"
SIG_DIR = "/var/lib/clamav"
RECENT_DAYS = 3
MAX_FILE = "256M"
MAX_HITS = 50            # names kept in the JSON; the count is always exact
SIG_WAIT_S = 3600        # first boot: freshclam may still be downloading the db
EXCLUDE = {".cache", ".git", "node_modules", "__pycache__", "venv", ".venv",
           ".npm", ".cargo", "Trash", "lost+found"}


HIST = OUT.replace(".json", "-history.json")
HIST_KEEP = 60           # ~2 months of dailies; the board charts the last 30


def write(res):
    res["duration_s"] = int(time.time() - res["ts"])
    tmp = OUT + ".tmp"
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(tmp, "w") as f:
        json.dump(res, f)
    os.replace(tmp, OUT)   # atomic: server.py never sees a half-written file
    if not res.get("err"):     # only real sweeps belong on the history chart
        try:
            with open(HIST) as f:
                h = json.load(f)
            if not isinstance(h, list):
                h = []
        except Exception:
            h = []
        h.append({k: res.get(k) for k in
                  ("ts", "mode", "scanned", "infected", "duration_s")})
        with open(HIST + ".tmp", "w") as f:
            json.dump(h[-HIST_KEEP:], f)
        os.replace(HIST + ".tmp", HIST)
    print(json.dumps(res))


def sigs_ready():
    try:
        return any(f.endswith((".cvd", ".cld")) for f in os.listdir(SIG_DIR))
    except OSError:
        return False


def recent_files(roots):
    """Regular files under roots touched in the last RECENT_DAYS."""
    cutoff = time.time() - RECENT_DAYS * 86400
    out = []
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in EXCLUDE]
            for name in filenames:
                p = os.path.join(dirpath, name)
                try:
                    st = os.lstat(p)
                except OSError:
                    continue
                if st.st_mode & 0o170000 == 0o100000 and st.st_mtime >= cutoff:
                    out.append(p)
    return out


def main():
    deep = "--deep" in sys.argv
    res = {"ts": int(time.time()), "mode": "deep" if deep else "daily",
           "scanned": 0, "infected": 0, "hits": []}

    if not shutil.which("clamscan"):
        res["err"] = "clamscan not installed (run security_setup.sh)"
        write(res)
        return 1
    waited = 0
    while not sigs_ready() and waited < SIG_WAIT_S:   # freshclam's first fetch
        time.sleep(60)
        waited += 60
    if not sigs_ready():
        res["err"] = "no signature db after %dmin — is clamav-freshclam running?" \
                     % (SIG_WAIT_S // 60)
        write(res)
        return 1

    roots = [p for p in (DOWNLOADS, HOME) if os.path.isdir(p)]
    cmd = ["nice", "-n", "15", "ionice", "-c", "3", "clamscan",
           "--infected", "--recursive", "--stdout",
           "--max-filesize=" + MAX_FILE, "--max-scansize=" + MAX_FILE]
    if deep:
        cmd += [r"--exclude-dir=/(\.git|\.cache|node_modules|__pycache__|"
                r"\.?venv|\.npm|\.cargo)$"]
        cmd += roots + (["/etc"] if os.path.isdir("/etc") else [])
    else:
        targets = recent_files(roots)
        if not targets:
            write(res)     # nothing new arrived: honestly clean, 0 files
            return 0
        lst = OUT + ".targets"
        with open(lst, "w") as f:
            f.write("\n".join(targets))
        cmd += ["--file-list=" + lst]

    p = subprocess.run(cmd, capture_output=True, text=True)
    out = p.stdout or ""
    hits = re.findall(r"^(.*): .+ FOUND$", out, re.M)
    m_sc = re.search(r"^Scanned files:\s*(\d+)", out, re.M)
    m_in = re.search(r"^Infected files:\s*(\d+)", out, re.M)
    m_kn = re.search(r"^Known viruses:\s*(\d+)", out, re.M)
    res["scanned"] = int(m_sc.group(1)) if m_sc else None
    res["infected"] = int(m_in.group(1)) if m_in else len(hits)
    res["known"] = int(m_kn.group(1)) if m_kn else None
    res["hits"] = hits[:MAX_HITS]
    if p.returncode not in (0, 1):     # 1 = infected found: still a valid scan
        # rc=2 also fires when a listed file vanished or was unreadable between
        # the walk and the scan — routine on a live homedir (session files,
        # caches). Those must not amber the board every day: if the scan still
        # produced its summary and every error line is that kind, count them as
        # skipped instead of failing the sweep. Anything else stays an err.
        transient = ("No such file or directory", "Can't access file",
                     "Can't open file", "Permission denied")
        errlines = [l.strip() for l in (p.stderr or "").splitlines() if l.strip()]
        real = [l for l in errlines if not any(w in l for w in transient)]
        # scanned must be POSITIVE to bless the run: "every target vanished"
        # (rc=2, all-transient, Scanned files: 0) is a sweep that scanned
        # nothing, not a clean one — that stays an err/amber
        if errlines and not real and res["scanned"]:
            res["skipped"] = len(errlines)
        else:
            src = "\n".join(real) or (p.stderr or out).strip()
            res["err"] = src.splitlines()[-1][:200] \
                         if src else "clamscan rc=%d" % p.returncode
    write(res)
    return 0


if __name__ == "__main__":
    sys.exit(main())
