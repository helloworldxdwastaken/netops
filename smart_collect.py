#!/usr/bin/env python3
"""SMART collector for the NETOPS board. Stdlib only, no deps.

Writes ~/netops/data/smart.json, which server.py reads PER REQUEST — so a fresh
run shows up on the board without restarting netops.service.

Why a container: smartmontools can't be installed on this host (no root), but
`tokyo` is in the `docker` group, so a --privileged container with /dev bound in
can read the disks. The image (netops-smart:latest, built from smart-image/) has
smartctl preinstalled, so a run is ~1s and never touches the network.

Run:      python3 smart_collect.py          -> refresh this host's entry
Selftest: python3 smart_collect.py --selftest
Schedule: hourly, from `crontab -l` (SMART moves slowly; hourly is plenty).

Output shape — a top-level dict keyed by HOSTNAME so more machines can report
into the same file later (each run only ever rewrites its own key):

  {"t": 1787132184,
   "hosts": {"homeserver": {"host": "homeserver", "t": ..., "ok": true,
                            "drives": [{...}, ...]}}}
"""
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data", "smart.json")
IMAGE = "netops-smart:latest"
HOST_ID = os.environ.get("NETOPS_HOST_ID") or socket.gethostname().split(".")[0]
DOCKER = shutil.which("docker") or "/usr/bin/docker"   # cron has a bare PATH
HOURS_YEAR = 8766.0     # 365.25 * 24
HOURS_MONTH = 730.5

# model_name tokens that add nothing to a friendly name ("Samsung Portable SSD
# T7 Shield" -> "Samsung T7 Shield")
_FILLER = {"portable", "ssd", "hdd", "nvme", "solid", "state", "drive", "disk",
           "external", "internal", "series"}
_BRANDS = {"SAMSUNG": "Samsung", "TOSHIBA": "Toshiba", "SEAGATE": "Seagate",
           "WDC": "WD", "WD": "WD", "HGST": "HGST", "CRUCIAL": "Crucial",
           "KINGSTON": "Kingston", "SANDISK": "SanDisk", "INTEL": "Intel",
           "MICRON": "Micron", "HITACHI": "Hitachi", "PNY": "PNY",
           "CORSAIR": "Corsair", "ADATA": "ADATA", "TEAM": "Team",
           "PSSD": "Samsung"}   # lsblk reports the T7 as bare "PSSD T7 Shield"


# --- pure helpers (covered by --selftest) ------------------------------------
def _is_partno(tok):
    """'MZ7TY256HDHP-000L7' / 'MQ04UBD200' -> True; 'T7', 'Shield' -> False.

    An opaque part number is a longish blob mixing letters and digits; short
    model words ('T7') and plain words ('Shield') are kept for the friendly name.
    """
    t = tok.strip("()")
    return len(t) >= 6 and any(c.isdigit() for c in t) and any(c.isalpha() for c in t)


def friendly(model, kind, bus):
    """Marketing-ish name: 'Samsung SSD', 'Toshiba USB HDD', 'Samsung T7 Shield'.

    Brand + whatever human-readable model words survive; when nothing survives
    (the model is just a part number) fall back to brand + bus + kind.
    """
    toks = (model or "").split()
    if not toks:
        return {"hdd": "Disco duro", "ssd": "SSD", "nvme": "SSD NVMe"}.get(kind, "Disco")
    brand = _BRANDS.get(toks[0].upper(), toks[0].title())
    tail = [t for t in toks[1:]
            if t.lower() not in _FILLER and not _is_partno(t)]
    if tail:
        return " ".join([brand] + tail)
    kindlbl = {"hdd": "HDD", "ssd": "SSD", "nvme": "SSD"}.get(kind, "Disco")
    return " ".join([brand] + (["USB"] if bus == "usb" else []) + [kindlbl])


def human_age(hours):
    """Powered-on hours -> '~6.2 años' / '~2 años' / '~8 meses' / '~12 días'."""
    if hours is None:
        return "—"
    y = hours / HOURS_YEAR
    if y >= 1:
        s = f"{y:.1f}".rstrip("0").rstrip(".")
        return f"~{s} año" if s == "1" else f"~{s} años"
    m = round(hours / HOURS_MONTH)
    if m >= 1:
        return "~1 mes" if m == 1 else f"~{m} meses"
    d = max(round(hours / 24.0), 0)
    return "~1 día" if d == 1 else f"~{d} días"


def fmt_size(b):
    """bytes -> board style: '1.8T', '238G'. Mirrors server.py's _fmt."""
    if not b:
        return "—"
    for unit, div in (("T", 1024**4), ("G", 1024**3), ("M", 1024**2)):
        if b >= div:
            return f"{b / div:.1f}{unit}"
    return f"{b}B"


def verdict(d):
    """Drive dict -> (key, label, one-line plain-language note).

    Thresholds (the owner's spec):
      replace_now  SMART FAILED, or wear > 95%
      replace_soon pending > 0, uncorrectable > 0, wear > 85% (or NVMe critical
                   warning / spare below threshold)
      watch        reallocated > 0 but nothing pending/uncorrectable — the disk
                   remapped bad sectors and got away with it
      good         clean, but minor CRC/link errors or middling wear
      excellent    PASSED, all zeros, low wear
    """
    ral, pen = d.get("realloc") or 0, d.get("pending") or 0
    unc, crc = d.get("uncorrect") or 0, d.get("crc") or 0
    wear = d.get("wear")
    if not d.get("passed"):
        return ("replace_now", "CAMBIAR YA",
                "SMART reporta FALLO: el disco puede morir en cualquier momento. "
                "Copia los datos ahora.")
    if wear is not None and wear > 95:
        return ("replace_now", "CAMBIAR YA",
                f"Ha gastado el {wear}% de su vida de escritura. Cámbialo ya.")
    if pen or unc:
        bad = pen + unc
        return ("replace_soon", "CAMBIAR PRONTO",
                f"{bad} sector{'es' if bad != 1 else ''} no se puede{'n' if bad != 1 else ''} "
                "leer ni reubicar: el disco está empezando a fallar. Haz copia y planifica el cambio.")
    if d.get("critical"):
        return ("replace_soon", "CAMBIAR PRONTO",
                "El propio disco está avisando de un problema interno. Haz copia y planifica el cambio.")
    if wear is not None and wear > 85:
        return ("replace_soon", "CAMBIAR PRONTO",
                f"Ha gastado el {wear}% de su vida de escritura. Ve preparando el recambio.")
    if ral:
        return ("watch", "VIGILAR",
                f"{ral} sector{'es' if ral != 1 else ''} dañado{'s' if ral != 1 else ''} "
                f"{'fueron' if ral != 1 else 'fue'} reubicado{'s' if ral != 1 else ''}; "
                "ahora mismo no hay nada ilegible — conviene vigilarlo.")
    if crc:
        return ("good", "BUENO",
                f"Sin sectores dañados; solo {crc} error{'es' if crc != 1 else ''} de "
                "enlace (cable/USB), que no tocan los datos.")
    if wear is not None and wear > 60:
        return ("good", "BUENO",
                f"Sin sectores dañados; lleva el {wear}% de su vida de escritura.")
    return ("excellent", "EXCELENTE",
            "Cero sectores dañados y cero errores de lectura. Como nuevo.")


def _raw(attr):
    """SMART attribute -> raw counter as int. Reads the leading number of the
    raw STRING ('43 (Min/Max 16/53)' -> 43): raw.value packs extras on some
    attributes (temperature, spin-up), the printed string never does."""
    s = str(attr.get("raw", {}).get("string", "")).strip()
    m = re.match(r"-?\d+", s)
    if m:
        return int(m.group(0))
    v = attr.get("raw", {}).get("value")
    return v if isinstance(v, int) else 0


def parse_drive(js, blk):
    """smartctl --json output + the lsblk row -> one board-ready drive dict."""
    attrs = {a["id"]: a for a in js.get("ata_smart_attributes", {}).get("table", [])}
    nvme = js.get("nvme_smart_health_information_log", {})
    proto = (js.get("device", {}) or {}).get("protocol", "")
    rota = bool(blk.get("rota"))
    bus = blk.get("tran") or ""
    kind = "hdd" if rota else ("nvme" if proto == "NVMe" or nvme else "ssd")

    def a(i):
        return _raw(attrs[i]) if i in attrs else None

    if nvme:   # NVMe has no ATA attribute table; map the log onto the same shape
        realloc, pending = None, None
        uncorrect = nvme.get("media_errors")
        crc = None
        spare, thr = nvme.get("available_spare"), nvme.get("available_spare_threshold")
        critical = bool(nvme.get("critical_warning")) or (
            isinstance(spare, int) and isinstance(thr, int) and spare < thr)
    else:
        realloc, pending, uncorrect, crc = a(5), a(197), a(198), a(199)
        critical = False

    # wear: smartctl 7.4+ normalises ATA wear-levelling AND NVMe percentage_used
    # into endurance_used; fall back to the raw sources, N/A for spinning rust.
    wear = (js.get("endurance_used") or {}).get("current_percent")
    if wear is None:
        wear = nvme.get("percentage_used")
    if wear is None and not rota:
        for wid in (177, 173, 231, 233):   # wear-levelling / life-left, normalised
            if wid in attrs and isinstance(attrs[wid].get("value"), int):
                wear = 100 - attrs[wid]["value"]
                break
    if rota:
        wear = None

    hours = (js.get("power_on_time") or {}).get("hours")
    if hours is None:
        hours = a(9)
    errs = [x for x in (pending, uncorrect, crc) if isinstance(x, int)]
    model = js.get("model_name") or blk.get("model") or ""
    d = {
        "dev": blk.get("name", ""),
        "name": friendly(model, kind, bus),
        "model": model,
        "kind": {"hdd": "HDD", "ssd": "SSD", "nvme": "SSD NVMe"}[kind],
        "bus": (bus or "").upper(),
        "size": fmt_size(blk.get("size") or 0),
        "use": blk.get("use") or "sin montar",
        "passed": bool((js.get("smart_status") or {}).get("passed")),
        "realloc": realloc,
        "pending": pending,
        "uncorrect": uncorrect,
        "crc": crc,
        "errors": sum(errs) if errs else None,
        "hours": hours,
        "age": human_age(hours),
        "wear": wear,
        "temp": (js.get("temperature") or {}).get("current"),
        "critical": critical,
    }
    d["verdict"], d["label"], d["note"] = verdict(d)
    return d


def pick_use(blk):
    """Disk row -> what it's FOR: '/', '/media', '/music'. Root wins over its
    own /boot/efi + swap siblings; otherwise every real mount, joined."""
    mounts = [m for m in [blk.get("mountpoint")] +
              [c.get("mountpoint") for c in blk.get("children", []) or []]
              if m and m != "[SWAP]"]
    if "/" in mounts:
        return "/"
    return " · ".join(dict.fromkeys(mounts)) or ""


def split_json(out):
    """'@@DEV /dev/sda\\n{json}\\n@@DEV ...' -> {dev: parsed}. Bad blobs skipped.

    `smartctl --json=c` prints its object with NO trailing newline, so the next
    marker can land glued to it ('}@@DEV /dev/sdc') — the newline before a marker
    is optional here, or every disk after the first is silently lost.
    """
    res = {}
    for dev, blob in re.findall(r"@@DEV (\S+)\n(.*?)(?=\n?@@DEV |\Z)", out, re.S):
        try:
            res[dev] = json.loads(blob.strip())
        except ValueError:
            continue
    return res


# --- collection --------------------------------------------------------------
def _run(cmd, timeout=120):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout
    except Exception:
        return ""


def disks():
    """Physical disks with the mount that explains them. Skips loop/zram/md."""
    out = _run(["lsblk", "-J", "-b", "-o",
                "NAME,PATH,TYPE,TRAN,SIZE,MOUNTPOINT,MODEL,ROTA"], 20)
    try:
        rows = json.loads(out or "{}").get("blockdevices", [])
    except ValueError:
        return []
    res = []
    for b in rows:
        if b.get("type") != "disk" or (b.get("name") or "").startswith(("loop", "zram", "ram")):
            continue
        b["use"] = pick_use(b)
        res.append(b)
    return res


def smart_all(paths):
    """ONE privileged container reads every disk (a run per disk would cost a
    container start each). Output is marker-delimited so partial failures on one
    disk can't swallow the rest."""
    if not paths:
        return {}
    script = "; ".join(
        f'echo; echo "@@DEV {p}"; smartctl --json=c -H -A -i -d auto {p} || true'
        for p in paths)
    return split_json(_run([DOCKER, "run", "--rm", "--privileged", "-v", "/dev:/dev",
                            IMAGE, "sh", "-c", script], 180))


def collect():
    blks = disks()
    smart = smart_all([b["path"] for b in blks])
    drives = []
    for b in blks:
        js = smart.get(b["path"])
        if not js or not (js.get("smart_status") or js.get("ata_smart_attributes")):
            continue   # no SMART (card reader, virtual disk) -> just omit it
        try:
            drives.append(parse_drive(js, b))
        except Exception:
            continue
    return {"host": HOST_ID, "t": int(time.time()), "ok": bool(drives),
            "drives": drives}


def main():
    rec = collect()
    if not rec["drives"]:
        print("no SMART data collected; leaving the previous file alone", file=sys.stderr)
        return 1
    try:                                   # merge: never clobber other hosts' keys
        with open(OUT) as f:
            doc = json.load(f)
        if not isinstance(doc, dict) or not isinstance(doc.get("hosts"), dict):
            raise ValueError
    except (OSError, ValueError):
        doc = {"hosts": {}}
    doc["hosts"][HOST_ID] = rec
    doc["t"] = int(time.time())
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    tmp = OUT + ".tmp"
    with open(tmp, "w") as f:
        json.dump(doc, f, indent=1, ensure_ascii=False)
    os.replace(tmp, OUT)                   # atomic: readers never see a torn file
    print(f"{OUT}: {len(rec['drives'])} drives "
          f"({', '.join(d['dev'] + '=' + d['verdict'] for d in rec['drives'])})")
    return 0


def _selftest():
    # friendly names: the three drives on the homeserver today
    assert friendly("SAMSUNG MZ7TY256HDHP-000L7", "ssd", "sata") == "Samsung SSD"
    assert friendly("TOSHIBA MQ04UBD200", "hdd", "usb") == "Toshiba USB HDD"
    assert friendly("Samsung Portable SSD T7 Shield", "nvme", "usb") == "Samsung T7 Shield"
    assert friendly("", "hdd", "sata") == "Disco duro"
    assert _is_partno("MQ04UBD200") and _is_partno("MZ7TY256HDHP-000L7")
    assert not _is_partno("T7") and not _is_partno("Shield")

    # powered-on hours -> human age (never a raw hour count on the board)
    assert human_age(54313) == "~6.2 años"
    assert human_age(17874) == "~2 años"
    assert human_age(11718) == "~1.3 años"
    assert human_age(8766) == "~1 año"
    assert human_age(2000) == "~3 meses"
    assert human_age(730) == "~1 mes"
    assert human_age(48) == "~2 días"
    assert human_age(None) == "—"

    assert fmt_size(2000398934016) == "1.8T"
    assert fmt_size(256060514304) == "238.5G"
    assert fmt_size(0) == "—"

    # raw counters read from the printed string, not the packed raw.value
    assert _raw({"raw": {"value": 4295016465, "string": "43 (Min/Max 16/53)"}}) == 43
    assert _raw({"raw": {"value": 7, "string": "7"}}) == 7
    assert _raw({"raw": {"value": 5, "string": ""}}) == 5

    # verdicts
    base = {"passed": True, "realloc": 0, "pending": 0, "uncorrect": 0,
            "crc": 0, "wear": 0}
    assert verdict(base)[0] == "excellent"
    assert verdict({**base, "crc": 4})[0] == "good"
    assert verdict({**base, "wear": 70})[0] == "good"
    assert verdict({**base, "realloc": 7})[0] == "watch"          # the sdc case
    assert verdict({**base, "realloc": 7, "crc": 4})[0] == "watch"
    assert verdict({**base, "realloc": 7, "pending": 1})[0] == "replace_soon"
    assert verdict({**base, "uncorrect": 2})[0] == "replace_soon"
    assert verdict({**base, "wear": 90})[0] == "replace_soon"
    assert verdict({**base, "critical": True})[0] == "replace_soon"
    assert verdict({**base, "wear": 99})[0] == "replace_now"
    assert verdict({**base, "passed": False})[0] == "replace_now"
    assert verdict({**base, "passed": False, "realloc": 0})[1] == "CAMBIAR YA"
    # singular/plural in the plain-language note
    assert "1 sector dañado fue reubicado" in verdict({**base, "realloc": 1})[2]
    assert "7 sectores dañados fueron reubicados" in verdict({**base, "realloc": 7})[2]

    # mount picking: root wins over its own efi/swap siblings
    assert pick_use({"mountpoint": None, "children": [
        {"mountpoint": "/boot/efi"}, {"mountpoint": "/"}, {"mountpoint": "[SWAP]"}]}) == "/"
    assert pick_use({"mountpoint": None, "children": [{"mountpoint": "/media"}]}) == "/media"
    assert pick_use({"mountpoint": None, "children": []}) == ""

    # marker splitting survives a disk whose smartctl output is garbage
    got = split_json('@@DEV /dev/sda\n{"a": 1}\n@@DEV /dev/sdb\nnot json\n'
                     '@@DEV /dev/sdc\n{"b": 2}\n')
    assert got == {"/dev/sda": {"a": 1}, "/dev/sdc": {"b": 2}}, got
    # ...and survives smartctl's missing trailing newline gluing marker to json
    got = split_json('@@DEV /dev/sda\n{"a": 1}@@DEV /dev/sdc\n{"b": 2}')
    assert got == {"/dev/sda": {"a": 1}, "/dev/sdc": {"b": 2}}, got

    # end-to-end parse of a real sdc-shaped payload -> the Watch verdict
    js = {"device": {"protocol": "ATA"}, "model_name": "TOSHIBA MQ04UBD200",
          "smart_status": {"passed": True}, "power_on_time": {"hours": 17874},
          "temperature": {"current": 43},
          "ata_smart_attributes": {"table": [
              {"id": 5, "name": "Reallocated_Sector_Ct", "value": 100, "raw": {"value": 7, "string": "7"}},
              {"id": 197, "value": 100, "raw": {"value": 0, "string": "0"}},
              {"id": 198, "value": 100, "raw": {"value": 0, "string": "0"}},
              {"id": 199, "value": 200, "raw": {"value": 4, "string": "4"}}]}}
    d = parse_drive(js, {"name": "sdc", "rota": True, "tran": "usb",
                         "size": 2000398934016, "use": "/media"})
    assert d["name"] == "Toshiba USB HDD" and d["kind"] == "HDD"
    assert (d["realloc"], d["pending"], d["uncorrect"], d["crc"]) == (7, 0, 0, 4)
    assert d["errors"] == 4 and d["age"] == "~2 años" and d["temp"] == 43
    assert d["wear"] is None                      # spinning disk -> no wear number
    assert d["verdict"] == "watch" and d["label"] == "VIGILAR"
    print("smart_collect selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
        sys.exit(0)
    sys.exit(main())
