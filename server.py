#!/usr/bin/env python3
"""NETOPS board — live MULTI-MACHINE homelab dashboard. Stdlib only, no deps.

Run:      python3 server.py            -> http://localhost:8787
Selftest: python3 server.py --selftest

Monitors several machines. Per machine it shows a HOST STATUS header (CPU load,
RAM used/total, uptime, disk free, CPU temp) and that machine's services (up/down/degraded
+ RAM). A machine is reached either locally ("ssh": None) or over SSH
("ssh": "user@host") — same collectors run either way. Service status/RAM come
from `docker` (containerized) or the listening process by port (native).

Edit MACHINES below to add machines or services; a machine that can't be reached
renders as "offline" without taking the rest of the page down.
"""
import base64
import calendar
import concurrent.futures
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import shlex
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8787
REFRESH_MS = 3000
CACHE_TTL = 2.0  # seconds; avoids piling up docker/ssh calls on overlapping requests

# --- Local config: title, machines, service catalog, auth --------------------
# Real values live in config.json next to this file (gitignored — never
# committed). Copy config.example.json to config.json and edit it for your
# setup; see README.md. If config.json is missing or has no "machines" key,
# the small demo list below ships instead, so the board still runs out of the
# box on a fresh clone.
CONFIG_PATH = os.environ.get("NETOPS_CONFIG",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))


def _load_config():
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"netops: {CONFIG_PATH} is invalid JSON ({e}) — using the "
              f"built-in demo config instead", file=sys.stderr)
        return {}


_CFG = _load_config()
TITLE = _CFG.get("title", "NETOPS // HOMELAB DASHBOARD")

# --- Machines: each = display + how to reach it + its service catalog ---------
# ssh:   None  -> run commands locally (netops runs ON this machine)
#        "u@h" -> prefix every command with `ssh u@h '<cmd>'`
# catalog entries reuse the classic schema:
#   port:  native process; status/RAM from whatever LISTENs on that TCP port.
#   match: container-name prefixes (all matching containers summed into one line).
# Schema + a fuller worked example live in config.example.json.
_DEMO_MACHINES = [
    {
        "id": "local", "name": "LOCAL MACHINE", "role": "DEMO",
        "ssh": None,  # None = run on this machine; "user@host" = over ssh
        "catalog": [
            {"name": "example-app", "cat": "APPS", "port": 8000, "url": "example.com"},
        ],
    },
]
MACHINES = _CFG.get("machines") or _DEMO_MACHINES
CAT_ORDER = ["AUTOMATIZACIÓN", "IA & AGENTES", "APPS", "CONTENIDO",
             "SITIOS PÚBLICOS", "INFRA & DATOS", "OTROS"]

# Which machine THIS instance runs on -> that machine is checked locally (ssh=None),
# every other over its address above.
#
# NETOPS_LOCAL wins when set; otherwise the system hostname is matched against the
# machine ids. There is deliberately NO hardcoded fallback: a wrong guess makes the
# board probe some OTHER machine's ports on THIS box and, finding nothing, render
# that machine permanently offline while its real ssh target is never contacted.
# (That is exactly what a `"tokyo"` default did here once netops moved to the
# homeserver — the M1 showed offline for weeks while ssh to it worked fine.)
# No match -> nothing is forced, and only the machine declared "ssh": None above
# is treated as local.
def _detect_local():
    host = socket.gethostname().split(".")[0].lower()
    for m in MACHINES:
        if m["id"].lower() == host:
            return m["id"]
    return None


LOCAL_ID = os.environ.get("NETOPS_LOCAL") or _detect_local()
for _m in MACHINES:
    if _m["id"] == LOCAL_ID:
        _m["ssh"] = None

# OrbStack's docker CLI isn't on the default (non-login) PATH over ssh.
_SSH_PREFIX = 'export PATH="$HOME/.orbstack/bin:$PATH"; '

_UNITS = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4,
          "KB": 1000, "MB": 1000**2, "GB": 1000**3}
_MEM_RE = re.compile(r"([\d.]+)\s*([KMGTP]?i?B)", re.I)


def _run(cmd, ssh=None, timeout=15):
    """Run argv `cmd` locally, or remotely when `ssh` target is set. '' on failure.

    Remote runs go through a shell, so argv is joined with shlex.quote (cmd is
    always hardcoded config — never request input) and the OrbStack PATH prefix
    is added so docker resolves. BatchMode/ConnectTimeout keep a dead host from
    hanging the request; it just fails fast and the machine renders offline.
    """
    if ssh:
        remote = _SSH_PREFIX + " ".join(shlex.quote(a) for a in cmd)
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6", ssh, remote]
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout
    except Exception:
        return ""


def _run_rc(cmd, ssh=None, timeout=15):
    """Like _run, but returns (rc, stdout, stderr).

    _run swallows both the exit code and stderr, which is fine for polling (a
    dead host just renders offline) but useless for an update: every failure
    came back as the same "compose pull/up failed" with no way to see why.
    """
    if ssh:
        remote = _SSH_PREFIX + " ".join(shlex.quote(a) for a in cmd)
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6", ssh, remote]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout}s"
    except Exception as e:                      # noqa: BLE001 - never raise at a caller
        return 1, "", str(e)


def _stream_run(cmd, ssh=None, timeout=900, on_line=None):
    """Run a command, handing each output line to on_line as it appears.

    stderr is merged into stdout on purpose: docker compose writes its whole
    progress narration ("Pulling", "Pulled", "Container x Started") to stderr,
    so that stream IS the status report. Capturing it only at the end is what
    made a two-minute image pull indistinguishable from a hang.

    Returns (rc, joined_output). Never raises.
    """
    if ssh:
        remote = _SSH_PREFIX + " ".join(shlex.quote(a) for a in cmd)
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6", ssh, remote]
    try:
        pr = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, bufsize=1)
    except Exception as e:                      # noqa: BLE001
        return 1, str(e)
    buf = []
    killer = threading.Timer(timeout, pr.kill)  # a silent hang still dies
    killer.start()
    try:
        for line in pr.stdout:
            line = line.rstrip()
            if not line:
                continue
            buf.append(line)
            if on_line:
                try:
                    on_line(line)
                except Exception:               # a bad listener must not kill the run
                    pass
        pr.wait()
    except Exception as e:                      # noqa: BLE001
        buf.append(str(e))
    finally:
        killer.cancel()
    rc = pr.returncode if pr.returncode is not None else 1
    if rc and not buf:
        buf.append(f"exit {rc}")
    return rc, "\n".join(buf)


# compose's own wording -> one short phrase for the board
def _step_of(line):
    """compose's wording -> a translation KEY the browser words itself."""
    l = line.lower()
    if "pulling" in l or "downloading" in l or "extracting" in l or "waiting" in l:
        return "pulling"
    if "pulled" in l or "download complete" in l:
        return "pulled"
    if "recreat" in l or "creating" in l or "created" in l:
        return "recreating"
    if "starting" in l or "started" in l:
        return "starting"
    if "stopping" in l or "stopped" in l or "removing" in l:
        return "stopping"
    if "running" in l:
        return "uptodate"
    if "error" in l or "failed" in l:
        return "failed"
    return None


def _parse_mem(s):
    """'215.2MiB' -> bytes. Docker uses 1024-based XiB units."""
    m = _MEM_RE.search(s or "")
    if not m:
        return 0
    return int(float(m.group(1)) * _UNITS.get(m.group(2).upper(), 1))


def _fmt(b):
    """bytes -> board style: '454.8M', '1.2G', '0B'."""
    if b >= 1024**3:
        return f"{b / 1024**3:.1f}G"
    if b >= 1024**2:
        return f"{b / 1024**2:.1f}M"
    if b >= 1024:
        return f"{b / 1024:.1f}K"
    return f"{int(b)}B"


# --- host status parsers (macOS) ---------------------------------------------
def _parse_loadavg(s):
    """'{ 1.52 1.74 1.82 }' (sysctl -n vm.loadavg) -> 1-min load float."""
    nums = re.findall(r"[\d.]+", s or "")
    return float(nums[0]) if nums else 0.0


def _mem_used(vmstat, total):
    """`vm_stat` output -> bytes in use = (active+wired+compressed)*pagesize.

    Page size is read from the header (16K on Apple silicon, 4K on Intel).
    Mirrors Activity Monitor's 'Memory Used'; capped at total for safety.
    """
    m = re.search(r"page size of (\d+)", vmstat or "")
    if not m:
        return 0
    psize = int(m.group(1))

    def pages(label):
        mm = re.search(re.escape(label) + r":\s+(\d+)", vmstat)
        return int(mm.group(1)) if mm else 0

    used = (pages("Pages active") + pages("Pages wired down")
            + pages("Pages occupied by compressor")) * psize
    return min(used, total)


def _parse_uptime(s):
    """`uptime` -> human uptime span, e.g. '3 days, 14:22' or '5 mins'."""
    m = re.search(r"up\s+(.*?),\s+\d+\s+user", s or "")
    return m.group(1).strip() if m else "?"


def _parse_df(s):
    """`df -k /` -> free bytes (Available column, in 1024-blocks)."""
    lines = (s or "").strip().splitlines()
    if len(lines) < 2:
        return 0
    parts = lines[1].split()
    return int(parts[3]) * 1024 if len(parts) > 3 and parts[3].isdigit() else 0


def _parse_thermal(s):
    """`cat /sys/class/thermal/thermal_zone*/temp` -> max °C, or None.

    Zones report millidegrees ('52000'); the board wants the hottest zone.
    """
    vals = [int(t) / 1000 for t in (s or "").split() if t.isdigit()]
    return round(max(vals)) if vals else None


def _parse_sensors_temp(s):
    """`sensors` output -> max °C across '+NN.N°C' readings, or None.

    Only readings (':  +52.0°C') count; '(high/crit = +80.0°C)' thresholds don't.
    """
    vals = [float(x) for x in re.findall(r":\s*\+(\d+(?:\.\d+)?)°C", s or "")]
    return round(max(vals)) if vals else None


def _temp_c(ssh, osname="darwin"):
    """CPU temp in °C, or None when unavailable (UI renders '—'). Never raises.

    Linux reads the thermal zones (a shell expands the glob; _run runs argv
    bare locally but through a shell over ssh), falling back to `sensors` when
    zones are unreadable. macOS has no temp source without sudo (pmset thermlog
    only reports throttling, not °C), so darwin honestly reports None.
    """
    if osname != "linux":
        return None
    t = _parse_thermal(_run(["sh", "-c",
                             "cat /sys/class/thermal/thermal_zone*/temp 2>/dev/null"], ssh))
    if t is not None:
        return t
    return _parse_sensors_temp(_run(["sh", "-c", "sensors 2>/dev/null"], ssh))


def _host_status(ssh, osname="darwin", mid=""):
    """Host stats dict, or {'online': False} if unreachable. macOS or Linux.

    The mem-total call doubles as the reachability probe: no numeric answer ==
    host down (or ssh failed), so we short-circuit the rest. df/loadavg parsers
    are shared (Available col + first load number look the same on both OSes).
    Temp is best-effort: None renders '—' instead of failing the block.
    """
    if osname == "linux":
        meminfo = _run(["cat", "/proc/meminfo"], ssh)
        mt = re.search(r"MemTotal:\s+(\d+)", meminfo)
        if not mt:
            return {"online": False}
        total = int(mt.group(1)) * 1024
        ma = re.search(r"MemAvailable:\s+(\d+)", meminfo)
        # no MemAvailable (ancient kernel) -> report 0 used, not a false 100% bar
        used = (total - int(ma.group(1)) * 1024) if ma else 0
        load = _parse_loadavg(_run(["cat", "/proc/loadavg"], ssh))
        up = re.sub(r"^up\s+", "", _run(["uptime", "-p"], ssh).strip()) or "?"
        free = _parse_df(_run(["df", "-k", "/"], ssh))
    else:
        memtotal = _run(["sysctl", "-n", "hw.memsize"], ssh).strip()
        if not memtotal.isdigit():
            return {"online": False}
        total = int(memtotal)
        used = _mem_used(_run(["vm_stat"], ssh), total)
        load = _parse_loadavg(_run(["sysctl", "-n", "vm.loadavg"], ssh))
        up = _parse_uptime(_run(["uptime"], ssh))
        free = _parse_df(_run(["df", "-k", "/"], ssh))
    cpu, iowait = _cpu_pct(ssh, osname, mid)
    net_rx, net_tx = _net_rate(ssh, osname, mid)
    temps = _all_temps(ssh, osname)
    # RAPL only exists for the box we run on; everyone else is modelled
    cpu_w = _rapl_watts() if ssh is None else None
    watts, measured = _watts_for(mid, cpu, cpu_w)
    return {"online": True, "load": f"{load:.2f}",
            "ram": f"{_fmt(used)} / {_fmt(total)}",
            "ram_bytes": used, "ram_total": total,
            "ram_pct": round(used / total * 100) if total else 0,
            "uptime": up, "disk": _fmt(free), "disk_bytes": free,
            "temp_c": _temp_c(ssh, osname),
            "cpu_pct": cpu, "io_pct": iowait, "cpu_approx": osname != "linux",
            "net_rx": net_rx, "net_tx": net_tx,
            "temps": temps, "watts": watts, "watts_measured": measured,
            "cpu_watts": cpu_w}


# --- vitals & power (modern view) --------------------------------------------
# Everything here is best-effort: any probe that fails returns None and the UI
# renders "—". Nothing in this section may raise or block a board refresh.

RAPL_ENERGY = "/sys/class/powercap/intel-rapl:0/energy_uj"
RAPL_MAX = "/sys/class/powercap/intel-rapl:0/max_energy_range_uj"

# Electricity tariff: auto-detected from the system's timezone (best-effort,
# no network call), falling back to a rough global average when detection
# fails. Rates vary by provider/plan/season and this is at best a country
# AVERAGE, so the frontend always shows a warning for an auto-detected rate
# (never for one set explicitly below) — the fix is a "power" key in
# config.json: {"kwh_price": 0.xx, "currency": "$", "tariff_note": "..."}.
# The bill's fixed monthly charges are per-household, not caused by these
# machines, so they are deliberately NOT added to the figures below.
#
# (price_per_kwh, currency_symbol, country_name) — rough 2025-ish residential
# averages in LOCAL currency, deliberately approximate; extend as needed.
_COUNTRY_TARIFFS = {
    "IL": (0.65, "₪", "Israel"), "US": (0.16, "$", "United States"),
    "GB": (0.28, "£", "United Kingdom"), "DE": (0.40, "€", "Germany"),
    "FR": (0.23, "€", "France"), "ES": (0.25, "€", "Spain"),
    "IT": (0.28, "€", "Italy"), "NL": (0.40, "€", "Netherlands"),
    "PT": (0.24, "€", "Portugal"), "IE": (0.35, "€", "Ireland"),
    "CA": (0.13, "$", "Canada"), "AU": (0.30, "$", "Australia"),
    "JP": (26.0, "¥", "Japan"), "BR": (0.75, "R$", "Brazil"),
    "IN": (7.0, "₹", "India"), "MX": (2.2, "$", "Mexico"),
    "PL": (0.75, "zł", "Poland"), "SE": (1.8, "kr", "Sweden"),
    "CH": (0.28, "CHF", "Switzerland"), "ZA": (2.3, "R", "South Africa"),
}
_TARIFF_DEFAULT = (0.20, "$", "unknown country")   # rough global-average fallback

# IANA timezone -> ISO country code, covering only _COUNTRY_TARIFFS above.
# Best-effort heuristic (a shared timezone can span countries); that's fine —
# it only ever selects an already-approximate default, and always with the
# "auto-detected, verify" warning attached.
_TZ_COUNTRY = {
    "Asia/Jerusalem": "IL", "Asia/Tel_Aviv": "IL",
    "America/New_York": "US", "America/Chicago": "US", "America/Denver": "US",
    "America/Los_Angeles": "US", "America/Phoenix": "US", "America/Anchorage": "US",
    "Europe/London": "GB", "Europe/Berlin": "DE", "Europe/Paris": "FR",
    "Europe/Madrid": "ES", "Europe/Rome": "IT", "Europe/Amsterdam": "NL",
    "Europe/Lisbon": "PT", "Europe/Dublin": "IE",
    "America/Toronto": "CA", "America/Vancouver": "CA",
    "Australia/Sydney": "AU", "Australia/Melbourne": "AU", "Asia/Tokyo": "JP",
    "America/Sao_Paulo": "BR", "Asia/Kolkata": "IN", "Asia/Calcutta": "IN",
    "America/Mexico_City": "MX", "Europe/Warsaw": "PL", "Europe/Stockholm": "SE",
    "Europe/Zurich": "CH", "Africa/Johannesburg": "ZA",
}


def _tz_name():
    """'Asia/Jerusalem'-style zone name, best-effort across distros.

    /etc/localtime -> zoneinfo symlink is the most portable source (works
    even where Debian's convenience /etc/timezone file is absent, as on a
    stock Debian install that only ever went through `timedatectl`); TZ env
    and time.tzname (an ambiguous abbreviation like "IST") are last resorts.
    """
    try:
        link = os.readlink("/etc/localtime")
        if "zoneinfo/" in link:
            tz = link.split("zoneinfo/", 1)[1]
            for prefix in ("posix/", "right/"):
                if tz.startswith(prefix):
                    tz = tz[len(prefix):]
            return tz
    except Exception:
        pass
    try:
        with open("/etc/timezone") as f:
            return f.read().strip()
    except Exception:
        pass
    return os.environ.get("TZ") or (time.tzname[0] if time.tzname else None)


def _detect_country():
    """Best-effort ISO country code from the system timezone. None on failure
    or when the timezone isn't in _TZ_COUNTRY — never authoritative, always
    just a starting point for _TARIFF_DEFAULT. Pure given its inputs."""
    tz = _tz_name()
    return _TZ_COUNTRY.get(tz) if tz else None


def _tariff():
    """(kwh_price, currency, note, auto) from config.json's "power" key, or
    auto-detected from the system timezone. `auto` tells the frontend whether
    to show the "verify this" warning — never true once the user has set an
    explicit rate."""
    p = _CFG.get("power") or {}
    if p.get("kwh_price") is not None:
        return (float(p["kwh_price"]), p.get("currency", "$"),
                p.get("tariff_note", "manual rate (config.json)"), False)
    price, currency, country = _COUNTRY_TARIFFS.get(_detect_country(), _TARIFF_DEFAULT)
    return (price, currency, f"{country} average, auto-detected", True)


KWH_PRICE, CURRENCY, TARIFF_NOTE, TARIFF_AUTO = _tariff()

# Watts when we cannot measure. base = board+RAM+fans+PSU loss, cpu_max = the
# chip's sustained draw at 100%, disks = spinning/idle drive draw.
# homeserver: i5-10500T (35W TDP) + 2 SSD + 1 USB HDD in a ThinkCentre SFF.
POWER_MODEL = {
    "homeserver": {"base": 14.0, "cpu_max": 35.0, "disks": 4.0},
    "tokyo":      {"base": 12.0, "cpu_max": 60.0, "disks": 1.0},
    "macair":     {"base": 8.0,  "cpu_max": 15.0, "disks": 1.0},
}
_POWER_DEFAULT = {"base": 12.0, "cpu_max": 30.0, "disks": 1.0}

# rolling watts series behind the consumption chart. Sampled on build (which is
# itself cached), throttled so the line represents a real time axis rather than
# however often someone happened to load the page.
_pwr = {"hist": [], "mhist": {}, "t": 0.0}   # mhist: {machine_id: [w|None] aligned to hist}
PWR_EVERY = 5.0
PWR_LEN = 180          # 15 min at 5s

# Live link throughput: bytes actually moving right now, from /proc/net/dev
# deltas between polls. Virtual interfaces are excluded — veth/docker/bridge
# counters mirror container traffic that already crosses the uplink, and
# tailscale0 is tunnelled over it too, so counting them double-counts.
_NET_SKIP = ("lo", "veth", "docker", "br-", "tailscale", "virbr", "tun", "wg")
_net_prev = {}         # mid -> (rx, tx, monotonic)

# A delta is only meaningful over a sane window. Below MIN the counter has barely
# moved and the result is quantisation noise; above MAX the "reading" is really a
# long-run average from before an outage.
MIN_SAMPLE_GAP = 0.4
MAX_SAMPLE_GAP = 120.0

_net_ct = {}           # "ssh/container" -> (rx, tx, monotonic) for per-container rates
_cpu_prev = {}         # mid -> (idle, iowait, total, monotonic) from the previous poll
_cpu_lock = threading.Lock()
_rapl_prev = {"uj": None, "t": None}
_speed = {"down": None, "up": None, "t": 0.0, "err": None}
SPEED_EVERY = 3600.0   # a speed test moves real bytes; once an hour is plenty


def _cpu_pct(ssh, osname, mid):
    """(cpu_pct, iowait_pct) 0-100 each, or (None, None).

    Linux: the delta of /proc/stat between two polls — the raw file is cumulative
    since boot, so a single read says nothing. The first poll after a restart has
    no predecessor and returns None rather than a fabricated number.

    iowait is reported SEPARATELY rather than folded into either figure. It is
    time the CPU sat idle waiting on disk, so counting it as busy overstates load
    — but silently counting it as idle is worse here: this box runs ~31% lifetime
    iowait, so streaming from the USB HDD drove the CPU gauge to ~0% and made a
    working machine look asleep. Showing both explains what is actually happening.

    macOS: derived from load average / core count. That is a proxy, not true
    utilisation, and the UI labels it approximate. No iowait equivalent -> None.
    """
    try:
        if osname == "linux":
            line = _run(["cat", "/proc/stat"], ssh).splitlines()
            if not line or not line[0].startswith("cpu "):
                return None, None
            v = [int(x) for x in line[0].split()[1:]]
            if len(v) < 5:
                return None, None
            idle, io, total = v[3], v[4], sum(v)   # idle alone; iowait tracked apart
            now = time.monotonic()
            with _cpu_lock:
                prev = _cpu_prev.get(mid)
                _cpu_prev[mid] = (idle, io, total, now)
            if not prev or len(prev) != 4:
                return None, None
            gap = now - prev[3]
            # Too short: the jiffy delta quantises into meaningless 0/50/100 steps.
            # Too long: a machine back from an outage would report a multi-hour
            # average as if it were the current load. Both report nothing instead.
            if gap < MIN_SAMPLE_GAP or gap > MAX_SAMPLE_GAP:
                return None, None
            di, dio, dt = idle - prev[0], io - prev[1], total - prev[2]
            if dt <= 0:
                return None, None
            return (max(0, min(100, round((1 - di / dt) * 100))),
                    max(0, min(100, round(dio / dt * 100))))
        ncpu = _run(["sysctl", "-n", "hw.ncpu"], ssh).strip()
        if not ncpu.isdigit() or int(ncpu) == 0:
            return None, None
        load = _parse_loadavg(_run(["sysctl", "-n", "vm.loadavg"], ssh))
        return max(0, min(100, round(load / int(ncpu) * 100))), None
    except Exception:
        return None, None


# Zones that are NOT sensors. INT3400 is Intel's DPTF policy device: Linux
# publishes it as thermal_zone0 so it sorts first, but it reports a fixed
# placeholder (a constant 20C here) and never measures anything. Showing it
# put a fake reading at the head of the thermal panel.
_TEMP_SKIP = ("int3400", "acpitz-virtual")
# Only rename what the name actually tells us. x86_pkg_temp is the CPU package
# sensor and pch_* is the chipset; SEN3/4/5 are Lenovo board sensors whose exact
# location is undocumented, so they keep their raw names rather than a guess.
_TEMP_NAME = {"x86_pkg_temp": "CPU", "coretemp": "CPU", "b0d4": "CPU ACPI"}
_TEMP_RANK = {"CPU": 0, "CPU ACPI": 3}


def _temp_label(raw):
    k = raw.strip().lower()
    if k in _TEMP_NAME:
        return _TEMP_NAME[k]
    if k.startswith("pch"):
        return "CHIPSET"
    return raw.strip().upper()


def _net_rate(ssh, osname, mid):
    """(rx_bytes_s, tx_bytes_s), or (None, None).

    Linux only: macOS would need another allowlisted command on the remote and
    reports None rather than a guess. First poll after a restart has nothing to
    diff against, so it also returns None instead of a fabricated spike.
    """
    if osname != "linux":
        return None, None
    try:
        out = _run(["cat", "/proc/net/dev"], ssh)
        rx = tx = 0
        for ln in (out or "").splitlines()[2:]:
            name, _, rest = ln.partition(":")
            name = name.strip()
            if not rest or any(name.startswith(k) for k in _NET_SKIP):
                continue
            f = rest.split()
            if len(f) < 9:
                continue
            rx += int(f[0]); tx += int(f[8])
        now = time.monotonic()
        prev = _net_prev.get(mid)
        _net_prev[mid] = (rx, tx, now)
        if not prev:
            return None, None
        dt = now - prev[2]
        if dt < MIN_SAMPLE_GAP or dt > MAX_SAMPLE_GAP:
            return None, None
        drx, dtx = rx - prev[0], tx - prev[1]
        if drx < 0 or dtx < 0:          # counter reset (interface bounced)
            return None, None
        return round(drx / dt), round(dtx / dt)
    except Exception:
        return None, None


def _all_temps(ssh, osname):
    """[{name, c}] for every readable sensor, CPU first, non-sensors dropped.

    macOS exposes no temperature without sudo, so darwin returns [].
    """
    if osname != "linux":
        return []
    out = _run(["sh", "-c",
                "for z in /sys/class/thermal/thermal_zone*/; do "
                "printf '%s:%s\\n' \"$(cat $z/type 2>/dev/null)\" "
                "\"$(cat $z/temp 2>/dev/null)\"; done"], ssh)
    temps = []
    for ln in (out or "").splitlines():
        name, _, raw = ln.strip().partition(":")
        if not name or not raw.strip().lstrip("-").isdigit():
            continue
        if any(k in name.lower() for k in _TEMP_SKIP):
            continue
        c = int(raw.strip()) / 1000.0
        if -20 < c < 130:                       # drop obviously bogus readings
            temps.append({"name": _temp_label(name), "c": round(c, 1)})
    # CPU first, chipset next, board sensors after — the panel shows the top few
    temps.sort(key=lambda t: (_TEMP_RANK.get(t["name"], 1 if t["name"] == "CHIPSET" else 2),
                              -t["c"]))
    return temps


def _rapl_watts():
    """MEASURED CPU package watts from the Intel RAPL counter, or None.

    Local machine only. energy_uj is root-readable by default (it was restricted
    upstream as a side-channel mitigation), so this returns None until a udev
    rule grants read access — the caller then falls back to the model.
    """
    try:
        with open(RAPL_ENERGY) as f:
            uj = int(f.read().strip())
    except Exception:
        return None
    now = time.monotonic()
    prev_uj, prev_t = _rapl_prev["uj"], _rapl_prev["t"]
    _rapl_prev["uj"], _rapl_prev["t"] = uj, now
    if prev_uj is None or prev_t is None:
        return None
    dt = now - prev_t
    if dt <= 0.5:                                # too short to be meaningful
        return None
    duj = uj - prev_uj
    if duj < 0:                                  # counter wrapped
        try:
            with open(RAPL_MAX) as f:
                duj += int(f.read().strip())
        except Exception:
            return None
    w = duj / 1e6 / dt
    return round(w, 1) if 0 <= w < 500 else None


def _watts_for(mid, cpu_pct, measured_cpu_w=None):
    """(watts, is_measured). Model unless RAPL gave us the CPU term."""
    mdl = POWER_MODEL.get(mid, _POWER_DEFAULT)
    fixed = mdl["base"] + mdl["disks"]
    if measured_cpu_w is not None:
        return round(fixed + measured_cpu_w, 1), True
    if cpu_pct is None:
        return None, False
    return round(fixed + mdl["cpu_max"] * (cpu_pct / 100.0), 1), False


# --- energy ledger ----------------------------------------------------------
# Watts is an instantaneous reading; cost is an integral. Without a store the
# board can only ever say "if it drew this forever it would cost X" — a
# projection, not a bill. This accumulates real watt-hours into daily buckets so
# month-to-date is measured rather than assumed. SQLite because it is stdlib and
# survives restarts; one row per day stays tiny (a few KB a year).
ENERGY_DB = os.environ.get("NETOPS_ENERGY_DB") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "energy.db")
ENERGY_EVERY = 30.0


def _energy_db():
    con = sqlite3.connect(ENERGY_DB, timeout=5)
    con.execute("CREATE TABLE IF NOT EXISTS days("
                "day TEXT PRIMARY KEY, wh REAL NOT NULL, secs REAL NOT NULL)")
    # the consumption chart's series, so a restart does not blank the graph
    con.execute("CREATE TABLE IF NOT EXISTS samples("
                "ts REAL PRIMARY KEY, watts REAL NOT NULL)")
    # per-machine series for the same chart window (one row per machine per tick)
    con.execute("CREATE TABLE IF NOT EXISTS msamples("
                "ts REAL NOT NULL, id TEXT NOT NULL, watts REAL NOT NULL,"
                "PRIMARY KEY(ts, id))")
    return con


def _energy_add(watts, dt):
    """Integrate `watts` held over `dt` seconds into today's bucket."""
    con = None
    try:
        con = _energy_db()
        with con:                               # commits the transaction
            con.execute(
                "INSERT INTO days(day,wh,secs) VALUES(?,?,?) "
                "ON CONFLICT(day) DO UPDATE SET wh=wh+excluded.wh, secs=secs+excluded.secs",
                (time.strftime("%Y-%m-%d"), watts * dt / 3600.0, dt))
    except Exception:
        pass                                    # a ledger write must never break the board
    finally:
        if con is not None:
            con.close()                         # `with con` does NOT close it


def _align_msamples(ts_list, mrows):
    """{id: [watts-or-None aligned to ts_list]} from (ts, id, watts) rows.

    Pure so the selftest can drive it. ts equality is exact: the same float is
    written to samples and msamples inside one _pwr_persist call, and sqlite
    REAL round-trips doubles losslessly.
    """
    idx = {ts: i for i, ts in enumerate(ts_list)}
    out = {}
    for ts, mid, w in mrows:
        i = idx.get(ts)
        if i is None:
            continue
        out.setdefault(mid, [None] * len(ts_list))[i] = w
    return out


def _pwr_persist(w, byid=None):
    """Append one watts sample and drop anything older than the chart window."""
    con = None
    try:
        con = _energy_db()
        now = time.time()
        with con:
            con.execute("INSERT OR REPLACE INTO samples(ts,watts) VALUES(?,?)", (now, w))
            con.execute("DELETE FROM samples WHERE ts < ?", (now - PWR_LEN * PWR_EVERY,))
            for mid, mw in (byid or {}).items():
                con.execute("INSERT OR REPLACE INTO msamples(ts,id,watts) VALUES(?,?,?)",
                            (now, mid, mw))
            con.execute("DELETE FROM msamples WHERE ts < ?", (now - PWR_LEN * PWR_EVERY,))
    except Exception:
        pass
    finally:
        if con is not None:
            con.close()


def _pwr_restore():
    """Refill the chart series from disk at startup.

    Without this the graph was the one part of the Consumo screen that really did
    start from scratch on every restart — the cost cards persisted, so the chart
    blanking looked like data loss even though nothing was lost. Samples older
    than the window are ignored, so a long outage shows a short series rather
    than a misleading flat line stitched across the gap.
    """
    con = None
    try:
        con = _energy_db()
        cutoff = time.time() - PWR_LEN * PWR_EVERY
        rows = con.execute("SELECT ts, watts FROM samples WHERE ts > ? ORDER BY ts",
                           (cutoff,)).fetchall()[-PWR_LEN:]
        _pwr["hist"] = [float(r[1]) for r in rows]
        ts_list = [r[0] for r in rows]
        mrows = con.execute("SELECT ts, id, watts FROM msamples WHERE ts > ?",
                            (cutoff,)).fetchall()
        _pwr["mhist"] = _align_msamples(ts_list, mrows)
    except Exception:
        pass
    finally:
        if con is not None:
            con.close()


def _energy_stats():
    """Measured energy so far, plus a month projection built from it.

    The projection is the AVERAGE WATTS actually recorded this month, extended
    over the whole month — not the instantaneous draw extrapolated. That matters:
    a spike while a movie transcodes should not multiply into the monthly figure.
    It also self-corrects: on day 1 it rests on a few hours, by day 20 on most of
    the month, so the number tightens on its own as days accumulate.

    `coverage` is recorded seconds divided by seconds elapsed this month. Below 1
    the board was down for part of it, and the projection assumes the missing
    time looked like the recorded time — so the UI shows coverage rather than
    hiding the assumption.
    """
    try:
        now = time.time()
        lt = time.localtime(now)
        day = time.strftime("%Y-%m-%d", lt)
        con = _energy_db()
        try:
            g = lambda pat: con.execute(              # noqa: E731
                "SELECT COALESCE(SUM(wh),0), COALESCE(SUM(secs),0), COUNT(*) "
                "FROM days WHERE day LIKE ?", (pat,)).fetchone()
            d_wh, d_s, _ = g(day)
            m_wh, m_s, m_n = g(day[:7] + "%")
            y_wh, y_s, y_n = g(day[:4] + "%")
            first = con.execute("SELECT MIN(day) FROM days").fetchone()[0]
        finally:
            con.close()
        r = KWH_PRICE
        out = {"today_kwh": round(d_wh / 1000, 3), "today_cost": round(d_wh / 1000 * r, 2),
               "today_hours": round(d_s / 3600, 1),
               "month_kwh": round(m_wh / 1000, 2), "month_cost": round(m_wh / 1000 * r, 2),
               "month_days": m_n,
               "year_kwh": round(y_wh / 1000, 1), "year_cost": round(y_wh / 1000 * r, 2),
               "year_days": y_n, "since": first}
        dim = calendar.monthrange(lt.tm_year, lt.tm_mon)[1]
        m_start = time.mktime((lt.tm_year, lt.tm_mon, 1, 0, 0, 0, 0, 0, -1))
        elapsed = max(1.0, now - m_start)
        out["month_days_total"] = dim
        out["month_elapsed_days"] = round(elapsed / 86400, 2)
        out["coverage"] = round(min(1.0, m_s / elapsed), 3)
        if m_s > 60:                              # under a minute of data projects nothing
            avg_w = m_wh / (m_s / 3600.0)
            proj_kwh = avg_w * 24 * dim / 1000.0
            out["month_avg_w"] = round(avg_w, 1)
            out["month_proj_kwh"] = round(proj_kwh, 1)
            out["month_proj_cost"] = round(proj_kwh * r, 2)
            out["year_proj_kwh"] = round(avg_w * 24 * 365.25 / 1000.0, 1)
            out["year_proj_cost"] = round(avg_w * 24 * 365.25 / 1000.0 * r, 2)
        return out
    except Exception:
        return None


def _energy_loop():
    last = time.monotonic()
    while True:
        time.sleep(ENERGY_EVERY)
        now = time.monotonic()
        dt, last = now - last, now
        # A restart loses at most one interval; keeping ENERGY_EVERY short bounds
        # that. What it can never recover is time the service was not running --
        # `coverage` in _energy_stats reports exactly how much that was.
        try:
            w = (get_data().get("power") or {}).get("watts")
            # a dt far from the interval means the box slept or stalled; skip it
            # rather than book a huge phantom block of energy
            if w is not None and 0 < dt < ENERGY_EVERY * 4:
                _energy_add(w, dt)
        except Exception:
            pass


def _cost(watts):
    """Watts held steady -> {day, month, year} at KWH_PRICE, plus kWh/month."""
    if watts is None:
        return None
    kwh_day = watts * 24 / 1000.0
    return {"day": round(kwh_day * KWH_PRICE, 2),
            "month": round(kwh_day * 30.44 * KWH_PRICE, 2),
            "year": round(kwh_day * 365.25 * KWH_PRICE, 2),
            "kwh_month": round(kwh_day * 30.44, 1),
            "kwh_year": round(kwh_day * 365.25, 1)}


def _speedtest():
    """Measure real throughput against Cloudflare. Moves ~25MB down, ~8MB up."""
    try:
        rc, out = _stream_run(["curl", "-s", "-o", "/dev/null",
                               "-w", "%{speed_download}",
                               "--max-time", "45",
                               "https://speed.cloudflare.com/__down?bytes=25000000"],
                              None, timeout=60)
        down = float(out.strip().splitlines()[-1]) * 8 / 1e6 if rc == 0 and out.strip() else None
    except Exception:
        down = None
    try:
        rc, out = _stream_run(["sh", "-c",
                               "head -c 8000000 /dev/zero | curl -s -o /dev/null "
                               "-w '%{speed_upload}' --max-time 45 -X POST "
                               "--data-binary @- https://speed.cloudflare.com/__up"],
                              None, timeout=60)
        up = float(out.strip().splitlines()[-1]) * 8 / 1e6 if rc == 0 and out.strip() else None
    except Exception:
        up = None
    _speed.update(down=round(down, 1) if down else None,
                  up=round(up, 1) if up else None,
                  t=time.time(), err=None if (down or up) else "sin respuesta")


def _speed_loop():
    while True:
        _speedtest()
        time.sleep(SPEED_EVERY)


# --- service status ----------------------------------------------------------
def _rollup(containers):
    """List of {run, unhealthy, done} -> 'up' | 'degraded' | 'down'.

    'done' = one-shot container that exited cleanly (code 0), e.g. a migration
    job; those are expected to be stopped, so they don't count against a service.
    """
    live = [c for c in containers if not c.get("done")]
    if not live:
        return "down"
    running = [c for c in live if c["run"]]
    if not running:
        return "down"
    if len(running) < len(live) or any(c["unhealthy"] for c in running):
        return "degraded"
    return "up"


def _docker_state(ssh=None):
    """name -> {run, unhealthy, mem, image} for every container (running or not)."""
    state = {}
    for line in _run(["docker", "ps", "-a", "--format",
                       "{{.Names}}\t{{.State}}\t{{.Status}}\t{{.Image}}"], ssh).splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        name, st, status = parts[0], parts[1], parts[2]
        state[name] = {"run": st == "running",
                       "unhealthy": "(unhealthy)" in status,
                       "done": st != "running" and "Exited (0)" in status,
                       "mem": 0, "image": parts[3] if len(parts) > 3 else ""}
    # NetIO rides along on the stats call that was already being made, so
    # per-container throughput costs nothing extra.
    now = time.monotonic()
    for line in _run(["docker", "stats", "--no-stream", "--format",
                       "{{.Name}}\t{{.MemUsage}}\t{{.NetIO}}"], ssh).splitlines():
        parts = line.split("\t")
        if len(parts) < 2 or parts[0] not in state:
            continue
        name = parts[0]
        state[name]["mem"] = _parse_mem(parts[1].split("/")[0])
        if len(parts) < 3:
            continue
        rx_s, _, tx_s = parts[2].partition("/")
        rx, tx = _parse_mem(rx_s), _parse_mem(tx_s)
        # A container sharing the host's network namespace has no veth pair for
        # docker to count, so it reports a flat 0B/0B forever. That is NOT idle —
        # it is unmeasurable, and the two must never look the same on screen.
        if rx == 0 and tx == 0:
            state[name]["net_measurable"] = False
            continue
        state[name]["net_measurable"] = True
        key = (ssh or "local") + "/" + name
        prev = _net_ct.get(key)
        _net_ct[key] = (rx, tx, now)
        if not prev:
            continue
        dt = now - prev[2]
        drx, dtx = rx - prev[0], tx - prev[1]
        if dt < MIN_SAMPLE_GAP or dt > MAX_SAMPLE_GAP or drx < 0 or dtx < 0:
            continue                            # restarted container or a silly window
        state[name]["net_rx"] = round(drx / dt)
        state[name]["net_tx"] = round(dtx / dt)
    return state


def _native_state(port, ssh=None, osname="darwin"):
    """(up, mem_bytes) for whatever is LISTENing on a TCP port.

    Linux uses `ss` (lsof often isn't installed); the listener is visible to any
    user but its PID/RSS only if we own it — so a foreign-user service reads as
    up with mem 0 rather than falsely down. macOS keeps lsof.
    """
    if osname == "linux":
        addrs = [ln.split()[3] for ln in _run(["ss", "-tlnH"], ssh).splitlines()
                 if len(ln.split()) >= 4]
        if not any(a.endswith(f":{port}") for a in addrs):
            return False, 0
        m = re.search(r"pid=(\d+)", _run(["ss", "-tlnpH", f"sport = :{port}"], ssh))
        if not m:
            return True, 0
        rss = _run(["ps", "-o", "rss=", "-p", m.group(1)], ssh).strip()
        return True, int(rss) * 1024 if rss.isdigit() else 0
    pids = _run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"], ssh).split()
    if not pids:
        return False, 0
    rss = _run(["ps", "-o", "rss=", "-p", pids[0]], ssh).strip()
    return True, int(rss) * 1024 if rss.isdigit() else 0


CF_CONFIG = "/etc/cloudflared/config.yml"


def _parse_cf_ingress(text):
    """{'hosts': [{'host': h, 'port': p}], 'n': N} from a cloudflared config.

    Pure (text in, dict out) so the selftest can feed it. Tolerates the
    service line being http://127.0.0.1:PORT or http://localhost:PORT; the
    catch-all http_status entry is skipped.
    """
    hosts, cur = [], None
    for ln in text.splitlines():
        m = re.search(r"^\s*-\s*hostname:\s*(\S+)", ln)
        if m:
            cur = m.group(1)
            continue
        m = re.search(r"service:\s*https?://(?:127\.0\.0\.1|localhost):(\d+)", ln)
        if m and cur:
            hosts.append({"host": cur, "port": int(m.group(1))})
            cur = None
    return {"hosts": hosts, "n": len(hosts)}


def _http_check(target):
    """Public HTTP status code of https://target (HEAD, GET fallback); 0 = no response.

    This is the REAL end-to-end check — DNS -> Cloudflare -> tunnel -> service — so
    it catches a dead site whose local port is still listening. Runs from netops's
    own host (not over ssh); the public URL is the same from anywhere.
    """
    for method in ("HEAD", "GET"):
        try:
            req = urllib.request.Request("https://" + target, method=method,
                                         headers={"User-Agent": "netops"})
            with urllib.request.urlopen(req, timeout=6) as r:
                if method == "HEAD" and not _http_ok(r.status):
                    continue       # odd HEAD answer — let GET give the verdict
                return r.status
        except urllib.error.HTTPError as e:
            # plenty of origins mishandle HEAD (stdlib servers 501 it, ntfy
            # 404s /v1/health on HEAD) — an error code on HEAD is not a
            # verdict, only GET's answer is final
            if method == "HEAD":
                continue
            return e.code          # server answered (401/404/502…) — a real code
        except Exception:
            continue               # DNS/connect/timeout — try GET, else 0
    return 0


def _http_ok(code):
    """A public URL is 'up' if the server actually responded (2xx/3xx or auth-gated).
    0 (no response), 404, and 5xx (origin down / bad gateway) count as down."""
    return 200 <= code < 400 or code in (401, 403, 405, 429)


def _probe_urls(machines):
    """Parallel public HTTP check of every service that has a url -> {target: code}."""
    targets = list({svc.get("health", svc["url"])
                    for m in machines for svc in m["catalog"] if svc.get("url")})
    if not targets:
        return {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        return dict(zip(targets, ex.map(_http_check, targets)))


def _collect_categories(catalog, docker, ssh, osname="darwin", http=None, outdated=()):
    """catalog + docker state -> (categories, online, degraded, down, ram_bytes).

    A service with a `url` takes its status from the PUBLIC http check (`http`
    map); the local port/container still supplies RAM. No url -> local status.
    `outdated` = images with a newer remote digest -> flags svc["update"].
    """
    http = http or {}
    matched = set()
    cats = {}  # cat -> [service dict]
    for svc in catalog:
        upd = []
        hits = {}
        # Container claiming comes FIRST, even when the entry also has `port`:
        # otherwise port-only/port+match services never mark their containers as
        # cataloged and the OTROS sweep below double-lists them.
        if "match" in svc:
            hits = {n: c for n, c in docker.items()
                    if any(n.startswith(p) for p in svc["match"])}
            matched.update(hits)
        if hits:
            status = _rollup(list(hits.values()))
            mem = sum(c["mem"] for c in hits.values() if c["run"])
            upd = sorted({c["image"] for c in hits.values()
                          if c.get("image") in outdated})
        elif "port" in svc:  # no container found -> fall back to the listener
            up, mem = _native_state(svc["port"], ssh, osname)
            status = "up" if up else "down"
        else:
            status, mem = "down", 0
        note = None
        if svc.get("url"):
            # Public reachability decides "up" — it is what a user actually gets.
            # But a service whose process is healthy and whose PUBLIC ROUTE is
            # broken is not the same failure as a dead one, and needs a different
            # fix (tunnel/DNS/origin binding, not the app). Flattening both to
            # "down" sent us hunting a dead Ollama that was running the whole
            # time behind a 502. Report it as degraded and say so.
            if _http_ok(http.get(svc.get("health", svc["url"]), 0)):
                status = "up"
            elif status == "up":
                status, note = "degraded", "route_down"
            else:
                status = "down"
        d = {"name": svc["name"], "status": status, "mem": mem, "ram": _fmt(mem),
             "url": svc.get("url"), "update": upd, "note": note}
        if hits and ssh is None:
            d["act"] = True    # local docker service -> restart/logs buttons
        cats.setdefault(svc["cat"], []).append(d)

    # Anything running but not in the catalog -> OTROS, so nothing is hidden.
    for name, c in docker.items():
        if c["run"] and name not in matched:
            cats.setdefault("OTROS", []).append(
                {"name": name, "status": "up", "mem": c["mem"], "ram": _fmt(c["mem"])})

    categories, online, degraded, down, total = [], 0, 0, 0, 0
    for cat in CAT_ORDER:
        if cat not in cats:
            continue
        svcs = cats[cat]
        categories.append({"name": cat, "services": svcs,
                           "up": sum(s["status"] == "up" for s in svcs),
                           "total": len(svcs)})
        for s in svcs:
            online += s["status"] == "up"
            degraded += s["status"] == "degraded"
            down += s["status"] == "down"
            total += s["mem"]
    return categories, online, degraded, down, total


def _build_machine(m, http=None):
    """One machine -> {id,name,role,host,categories,summary}. Never raises."""
    empty = {"online": 0, "degraded": 0, "down": 0, "count": 0, "ram": "0B", "ram_bytes": 0}
    # "local" is authoritative: LOCAL_ID stamps ssh=None on the machine netops
    # runs on. The topology used to find the hub with /home/i.test(id), which
    # picks the wrong box the moment a machine is renamed or another id merely
    # contains "home".
    base = {"id": m["id"], "name": m["name"], "role": m["role"],
            "local": m.get("ssh") is None}
    host = _host_status(m["ssh"], m.get("os", "darwin"), m["id"])
    if not host["online"]:
        return {**base, "host": {"online": False}, "categories": [], "summary": dict(empty)}
    # ponytail: host stats are 4 serial ssh round-trips; batch into one `ssh 'a;b;c'`
    # call if Tailscale latency makes refresh sluggish. Fine while cached (2s).
    needs_docker = any("match" in s for s in m["catalog"])
    docker = _docker_state(m["ssh"]) if needs_docker else {}
    running = sum(1 for c in docker.values() if c.get("run"))
    # ranked talkers for the network panel, plus an honest count of the ones
    # docker simply cannot see
    talk = sorted(((n, c) for n, c in docker.items()
                   if c.get("run") and (c.get("net_rx") or c.get("net_tx"))),
                  key=lambda kv: -(kv[1].get("net_rx", 0) + kv[1].get("net_tx", 0)))
    net_top = [{"name": n, "rx": c.get("net_rx", 0), "tx": c.get("net_tx", 0)}
               for n, c in talk[:6]]
    net_blind = sum(1 for c in docker.values()
                    if c.get("run") and c.get("net_measurable") is False)
    cats, on, deg, dn, ram = _collect_categories(m["catalog"], docker, m["ssh"],
                                                 m.get("os", "darwin"), http,
                                                 _updates.get(m["id"], set()))
    return {**base, "host": host, "categories": cats,
            "summary": {"online": on, "degraded": deg, "down": dn,
                        "count": on + deg + dn, "ram": _fmt(ram), "ram_bytes": ram,
                        "containers": running},
            "net_top": net_top, "net_blind": net_blind}


def _build():
    http = _probe_urls(MACHINES)  # one parallel pass of public HTTP checks
    # Machines are probed concurrently: serially, one unreachable host burned its
    # full 6s ssh ConnectTimeout before the next machine was even contacted, and
    # that dominated every build. Order of the result list is preserved.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(2, len(MACHINES))) as ex:
        machines = list(ex.map(lambda m: _build_machine(m, http), MACHINES))
    on = deg = dn = ram = 0
    for mb in machines:
        s = mb["summary"]
        on += s["online"]; deg += s["degraded"]; dn += s["down"]; ram += s["ram_bytes"]
    # fleet-wide power: only machines we could actually reach contribute, so an
    # offline box does not silently inflate or deflate the electricity figures.
    per, total_w, modelled = [], 0.0, False
    for mb in machines:
        h = mb.get("host") or {}
        if not h.get("online") or h.get("watts") is None:
            continue
        total_w += h["watts"]
        modelled = modelled or not h.get("watts_measured")
        per.append({"id": mb["id"], "name": mb["name"], "watts": h["watts"],
                    "measured": bool(h.get("watts_measured")),
                    "cost": _cost(h["watts"])})
    total_w = round(total_w, 1) if per else None
    now = time.time()
    if total_w is not None and now - _pwr["t"] >= PWR_EVERY:
        _pwr["t"] = now
        _pwr["hist"].append(total_w)
        del _pwr["hist"][:-PWR_LEN]
        # per-machine points, padded with None so every series stays aligned
        # with hist (a machine that is offline this tick gets a gap, not a 0)
        byid = {p["id"]: p["watts"] for p in per}
        for mid in set(_pwr["mhist"]) | set(byid):
            lst = _pwr["mhist"].setdefault(mid, [])
            lst += [None] * (len(_pwr["hist"]) - 1 - len(lst))
            lst.append(byid.get(mid))
            del lst[:-PWR_LEN]
        _pwr_persist(total_w, byid)
    containers = sum(mb["summary"].get("containers", 0) for mb in machines)
    return {"machines": machines,
            "summary": {"online": on, "degraded": deg, "down": dn,
                        "count": on + deg + dn, "machines": len(MACHINES),
                        "ram": _fmt(ram), "ram_bytes": ram,
                        "containers": containers,
                        "cats": len({c["name"] for mb in machines
                                     for c in mb.get("categories", [])})},
            "speed": {"down": _speed["down"], "up": _speed["up"],
                      "t": _speed["t"], "err": _speed["err"],
                      "age": int(time.time() - _speed["t"]) if _speed["t"] else None},
            "power": {"watts": total_w, "per": per, "modelled": modelled,
                      "hist": list(_pwr["hist"]),
                      "mhist": {k: list(v) for k, v in _pwr["mhist"].items()},
                      "every": PWR_EVERY,
                      "rate": KWH_PRICE, "currency": CURRENCY,
                      "note": TARIFF_NOTE, "auto": TARIFF_AUTO,
                      "cost": _cost(total_w), "actual": _energy_stats(),
                      "offline": [mb["name"] for mb in machines
                                  if not (mb.get("host") or {}).get("online")]},
            "library_apps": [a for a in _ARR_APPS if _arr_conf(a)]}


# (t, data) as ONE tuple so a reader can never pair one thread's data with
# another thread's timestamp — a single dict read is atomic in CPython.
_cache = {"v": (0.0, None)}
_build_lock = threading.Lock()


def get_data():
    """Cached board. One build at a time; everyone else gets the last good one.

    The timestamp is taken when the build FINISHES, not when it starts. Stamping
    it beforehand made the entry ~11s old the instant it was stored (a build costs
    far more than CACHE_TTL), so the cache never once hit and every poll launched
    another full ssh/docker fan-out — several permanently in flight at a 3s
    refresh. A completion stamp alone is not enough either: with a build longer
    than the TTL, concurrent callers would still all miss and all build. Hence
    the non-blocking single-flight lock.
    """
    t, data = _cache["v"]
    if data is not None and time.monotonic() - t < CACHE_TTL:
        return data
    if not _build_lock.acquire(blocking=False):
        _, data = _cache["v"]
        if data is not None:
            return data                    # a build is running; serve what we have
        with _build_lock:                  # cold start only: wait for the first one
            return _cache["v"][1]
    try:
        d = _build()
        _cache["v"] = (time.monotonic(), d)
        return d
    finally:
        _build_lock.release()


# --- uptime history: a little heartbeat bar per service (replaces Uptime Kuma) -
# Ring buffer of recent up/down samples per service, filled by a background
# thread so history accrues even when nobody's viewing. Persisted to STATE_FILE
# (see _save_state) every ~30s and reloaded at startup, so a netops restart keeps
# the bars + update chips instead of resetting them.
_history = {}          # "machineid/name" -> list[int]  (1=up, 0=not up), recent last
HIST_LEN = 48          # samples kept per service
HIST_EVERY = 15.0      # seconds between samples (48*15s ≈ 12 min window).
                       # Was 5s — that meant a probe of EVERY service (plus
                       # GET+HEAD against the public URLs) 12x/min, a constant
                       # background tax on an already-loaded box.
STATE_FILE = os.path.expanduser("~/.netops_state.json")
STATE_EVERY = 2        # persist every 2nd sample (~30s); file is a few KB


def _record():
    data = get_data()
    for m in data["machines"]:
        for c in m.get("categories", []):
            for s in c["services"]:
                dq = _history.setdefault(f'{m["id"]}/{s["name"]}', [])
                dq.append(1 if s["status"] == "up" else 0)
                del dq[:-HIST_LEN]
    # An unreachable machine reports no services at all; from here its catalog
    # services are down, so record honest 0s instead of letting their bars freeze
    # on the last good sample (same false-green rule as the restart reset).
    online = {m["id"] for m in data["machines"] if m.get("host", {}).get("online")}
    for m in MACHINES:
        if m["id"] in online:
            continue
        for s in m["catalog"]:
            dq = _history.setdefault(f'{m["id"]}/{s["name"]}', [])
            dq.append(0)
            del dq[:-HIST_LEN]


def _hist_loop():
    n = 0
    while True:
        try:
            _record()
            n += 1
            if n % STATE_EVERY == 0:
                _save_state()
        except Exception:
            pass
        time.sleep(HIST_EVERY)


def _save_state():
    """Atomically persist _history + _updates to STATE_FILE. Best-effort.

    Only ever called from the hist thread, so _history (which _record mutates in
    place) is never serialized mid-mutation. _updates is mutated by other threads
    only via WHOLESALE reassignment (never in-place), so a dict() snapshot here
    can't tear; a stray RuntimeError (key added mid-iterate) just skips one save.
    """
    with _updates_lock:  # consistent snapshot; can't tear against a concurrent RMW/sweep
        updates_snap = {k: sorted(v) for k, v in _updates.items()}
    try:
        blob = json.dumps({"t": time.time(),
                           "history": dict(_history),
                           "updates": updates_snap})
    except (TypeError, RuntimeError):
        return
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(blob)
        os.replace(tmp, STATE_FILE)  # atomic on POSIX — no torn file if we die mid-write
    except OSError:
        pass


def _load_state():
    """Restore _history + _updates at startup. Missing/corrupt file -> start fresh.

    Runs before the socket binds, so it must never raise: valid-but-wrong-shape
    JSON (a list/scalar/null) is rejected too, else launchd crash-loops on it.
    Heartbeat bars are only restored if the save is fresher than the window they
    represent — a longer gap (box was off) would paint false-green over an outage,
    so we honestly reset to "…"; update flags always restore (the startup sweep
    re-checks them within minutes anyway).
    """
    try:
        with open(STATE_FILE) as f:
            d = json.load(f)
    except (OSError, ValueError):
        return
    if not isinstance(d, dict):
        return
    saved = d.get("t", 0)
    fresh = isinstance(saved, (int, float)) and time.time() - saved <= HIST_LEN * HIST_EVERY
    hist, upd = d.get("history"), d.get("updates")
    # Guard the nested containers too, not just the top-level shape: a truthy
    # non-dict history/updates ('x', [1,2], 5, true) would make .items() raise on
    # this pre-bind boot path -> launchd crash-loop. Filter set() to str so an
    # unhashable element (a nested list/dict) can't raise either.
    if fresh and isinstance(hist, dict):
        for k, v in hist.items():
            if isinstance(v, list):
                _history[k] = [1 if x else 0 for x in v][-HIST_LEN:]
    if isinstance(upd, dict):
        for k, v in upd.items():
            if isinstance(v, list):
                _updates[k] = {x for x in v if isinstance(x, str)}


# --- image updates: flag docker services whose floating tag moved upstream ----
# Compares each running container image's local digest vs the registry's digest
# for the same tag; a floating tag (:latest/:stable) that moved = update ready.
# Checked in a background thread (registry round-trips are slow + rate-limited).
UPD_EVERY = 6 * 3600   # seconds between registry sweeps
_updates = {}          # machine id -> set of images with a newer remote digest
_updates_lock = threading.Lock()   # atomic read-modify-write of _updates across threads
def _claimed(names, prefixes):
    """Container names claimed by a catalog match list (prefix rule — the same
    one _collect_categories uses). Pure, selftest-fed."""
    return [n for n in names if any(n.startswith(p) for p in prefixes or [])]


def _local_containers(svc):
    """Live LOCAL container names for a catalog entry, [] when none/remote."""
    rc, out, _ = _run_rc(["docker", "ps", "--format", "{{.Names}}"], timeout=10)
    return _claimed(out.split() if rc == 0 else [], svc.get("match"))


_inflight = set()      # "mid/svc" update ops running (server-side double-click guard)
_inflight_lock = threading.Lock()  # makes the check-and-add atomic

# An update runs in a background thread and the browser polls it. Holding the
# POST open for the whole pull does not work through the tunnel: Cloudflare cuts
# an origin response off at ~100s, so any slow image returned 524 to the browser
# while the update carried on server-side — the board showed a failure for an
# update that actually succeeded. Returning a job id immediately fixes that and
# is what lets the UI narrate progress instead of showing a frozen "…".
_jobs = {}             # id -> {state, step, log[], svc, mid, code, msg, t}
_jobs_lock = threading.Lock()
_JOB_TTL = 900         # finished jobs stay readable this long, then are swept
_UPD_SH = r"""docker ps --format '{{.Image}}' | sort -u | while read -r img; do
  case "$img" in *@sha256:*) continue ;; esac
  local=$(docker image inspect "$img" --format '{{index .RepoDigests 0}}' 2>/dev/null | sed 's/.*@//')
  [ -n "$local" ] || continue
  remote=$(docker buildx imagetools inspect "$img" --format '{{.Manifest.Digest}}' 2>/dev/null)
  [ -n "$remote" ] || continue
  [ "$local" = "$remote" ] || echo "$img"
done"""

# --- auth: username + password -> short-lived session token ------------------
# The board is public (via tunnel), so POST /api/update is the trust boundary.
# The PASSWORD is never stored in source or in the browser: only a salted
# PBKDF2-SHA256 hash is used, loaded from config.json's "auth" key (gitignored
# — never committed). Clients log in ONCE at /api/login (over the HTTPS
# tunnel) to get a random, expiring session token that gates /api/update.
# Generate credentials with:
#   python3 -c "import hashlib,os,json; s=os.urandom(16); \
#   print(json.dumps({'user':'you','salt_hex':s.hex(), \
#   'hash_hex':hashlib.pbkdf2_hmac('sha256',b'<newpass>',s,200000).hex(), \
#   'iters':200000}))"
# and put the result under "auth" in config.json. See README.md.
_auth_cfg = _CFG.get("auth") or {}
if not _auth_cfg:
    print("=" * 78, file=sys.stderr)
    print("netops: no \"auth\" in config.json — falling back to admin/admin.",
          file=sys.stderr)
    print("        Generate real credentials before exposing this past "
          "localhost — see README.md 'Authentication'.", file=sys.stderr)
    print("=" * 78, file=sys.stderr)
_AUTH_USER = _auth_cfg.get("user", "admin")
_AUTH_ITERS = _auth_cfg.get("iters", 200_000)
_AUTH_SALT = (bytes.fromhex(_auth_cfg["salt_hex"]) if _auth_cfg.get("salt_hex")
              else b"\x00" * 16)
_AUTH_HASH = (bytes.fromhex(_auth_cfg["hash_hex"]) if _auth_cfg.get("hash_hex")
              else hashlib.pbkdf2_hmac("sha256", b"admin", _AUTH_SALT, _AUTH_ITERS))
_SESSION_TTL = 12 * 3600           # a login is good for 12h, then re-auth
_sessions = {}                     # token -> expiry (monotonic seconds)
_sessions_lock = threading.Lock()
_LOGIN_MAX, _LOGIN_WINDOW = 8, 300  # >8 failed logins / 5 min from one client -> locked out
_login_fails = {}                  # client id -> (count, first-fail monotonic)
_login_lock = threading.Lock()


def _parse_basic(header):
    """'Basic base64(user:pass)' -> (user, pass), or (None, None). Never raises."""
    try:
        scheme, _, b64 = (header or "").partition(" ")
        if scheme.lower() != "basic":
            return None, None
        raw = base64.b64decode(b64, validate=True).decode("utf-8")
        user, sep, pw = raw.partition(":")   # password may contain ':'
        return (user, pw) if sep else (None, None)
    except Exception:
        return None, None


def _check_login(user, password):
    """Constant-time-ish user+password check. Always hashes so a wrong username
    and a wrong password cost the same (no which-was-wrong timing leak)."""
    if not user or not password:
        return False
    u_ok = hmac.compare_digest(user.encode("utf-8"), _AUTH_USER.encode("utf-8"))
    calc = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), _AUTH_SALT, _AUTH_ITERS)
    p_ok = hmac.compare_digest(calc, _AUTH_HASH)
    return u_ok and p_ok


def _new_session():
    """Random opaque token, stored server-side with a TTL. Sweeps expired ones."""
    now = time.monotonic()
    tok = secrets.token_urlsafe(32)
    with _sessions_lock:
        for k in [k for k, e in _sessions.items() if e < now]:
            _sessions.pop(k, None)
        _sessions[tok] = now + _SESSION_TTL
    return tok


def _session_valid(tok):
    if not tok:
        return False
    now = time.monotonic()
    with _sessions_lock:
        exp = _sessions.get(tok)
        if exp is None:
            return False
        if exp < now:
            _sessions.pop(tok, None)
            return False
        return True


def _login_blocked(cid):
    with _login_lock:
        rec = _login_fails.get(cid)
        if not rec:
            return False
        count, first = rec
        if time.monotonic() - first > _LOGIN_WINDOW:  # window elapsed -> forgiven
            _login_fails.pop(cid, None)
            return False
        return count >= _LOGIN_MAX


def _login_fail(cid):
    with _login_lock:
        count, first = _login_fails.get(cid, (0, time.monotonic()))
        if time.monotonic() - first > _LOGIN_WINDOW:
            count, first = 0, time.monotonic()
        _login_fails[cid] = (count + 1, first)


def _login_reset(cid):
    with _login_lock:
        _login_fails.pop(cid, None)


# The board itself is gated, not just /api/update: an unauthenticated GET used to
# hand out every hostname, internal port, drive serial and — via "update" — a list
# of exactly which images were running out of date. The session rides in an
# HttpOnly cookie so page loads carry it and no script can read it back.
_COOKIE = "netops_session"


def _cookie_token(header):
    """'foo=1; netops_session=xyz' -> 'xyz', or '' when absent. Never raises."""
    for part in (header or "").split(";"):
        k, _, v = part.strip().partition("=")
        if k == _COOKIE:
            return v.strip()
    return ""


def _check_updates(m):
    out = _run(["sh", "-c", _UPD_SH], m["ssh"], timeout=240)
    new = {ln.strip() for ln in out.splitlines() if ln.strip()}
    with _updates_lock:
        _updates[m["id"]] = new


def _upd_loop():
    while True:
        for m in MACHINES:
            if any("match" in s for s in m["catalog"]):
                try:
                    _check_updates(m)
                except Exception:
                    pass
        time.sleep(UPD_EVERY)


def _apply_update(m, svc, dry=False, on_step=None, on_line=None):
    """docker compose pull + up -d for ONE compose service. (code, msg).

    Scoped to the single service on purpose. Every app here shares one compose
    project (16 services in /home/tokyo/noxa), so the old bare
    `docker compose pull && docker compose up -d` pulled all 16 images and would
    have restarted the whole stack to update one app — and it could never
    succeed, because that project contains locally-built images with no registry
    to pull from, so `pull` always exited non-zero and `&& up -d` never ran.
    That is the "compose pull/up failed" every update reported.

    dry=True runs compose in --dry-run mode: resolves and reports what WOULD
    happen, touching nothing. Used to verify the pipeline without shipping.
    """
    step = on_step or (lambda _s: None)
    step("locating")
    docker = _docker_state(m["ssh"])
    hits = [n for n in docker if any(n.startswith(p) for p in svc["match"])]
    if not hits:
        return 409, "no containers matched"
    # working_dir AND the compose service name, from the container's own labels
    fmt = ('{{index .Config.Labels "com.docker.compose.project.working_dir"}}\n'
           '{{index .Config.Labels "com.docker.compose.service"}}')
    _, out, _ = _run_rc(["docker", "inspect", hits[0], "--format", fmt], m["ssh"])
    parts = (out or "").strip().split("\n")
    wd = parts[0].strip() if parts else ""
    sname = parts[1].strip() if len(parts) > 1 else ""
    if not wd or "<no value>" in wd or not sname or "<no value>" in sname:
        return 409, "container is not compose-managed"
    q = shlex.quote
    flag = " --dry-run" if dry else ""
    step("pulling")
    rc, out = _stream_run(["sh", "-c",
                           f"cd {q(wd)} && docker compose{flag} pull {q(sname)} && "
                           f"docker compose{flag} up -d {q(sname)} && echo NETOPS_OK"],
                          m["ssh"], timeout=900, on_line=on_line)
    if "NETOPS_OK" not in out:
        lines = [ln.strip() for ln in out.splitlines()
                 if ln.strip() and "NETOPS_OK" not in ln]
        why = lines[-1][:300] if lines else f"exit {rc}"
        return 500, f"compose falló: {why}"
    if dry:
        plan = [ln.strip() for ln in out.splitlines()
                if ln.strip() and "NETOPS_OK" not in ln]
        return 200, ("simulación ok — " + " | ".join(plan[-6:]))[:400] if plan else "simulación ok"
    # pulled == local now matches remote; clear the flag without a full re-sweep.
    # Under the lock so a concurrent updater/sweep of this machine isn't lost, and
    # reassign (not difference_update) so _save_state never sees a set mid-mutation.
    with _updates_lock:
        _updates[m["id"]] = _updates.get(m["id"], set()) - {docker[n].get("image") for n in hits}
    _cache["v"] = (0.0, _cache["v"][1])  # next fetch rebuilds with fresh container state
    return 200, "updated"


# --- drive health (SMART): collected out of process, read at serve time ------
# smart_collect.py (hourly, from cron) writes data/smart.json via a privileged
# smartmontools container — smartmontools can't be installed on this host. The
# board only READS that file, and re-reads it whenever its mtime moves, so a
# fresh collector run shows up WITHOUT restarting netops.
SMART_FILE = os.environ.get("NETOPS_SMART_FILE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "smart.json")
SMART_STALE = 6 * 3600     # older than this -> the timestamp turns amber
_smart_cache = {"mtime": None, "doc": {}}


def _start_update(m, svc, dry=False):
    """Kick off an update in the background. -> job id. Frees _inflight when done."""
    jid = secrets.token_urlsafe(12)
    key = f'{m["id"]}/{svc["name"]}'
    now = time.time()
    with _jobs_lock:
        for k in [k for k, j in _jobs.items()
                  if j["state"] != "running" and now - j["t"] > _JOB_TTL]:
            _jobs.pop(k, None)
        _jobs[jid] = {"state": "running", "step": "preparing", "log": [],
                      "svc": svc["name"], "mid": m["id"], "dry": bool(dry), "t": now}

    def _set(**kw):
        with _jobs_lock:
            j = _jobs.get(jid)
            if j:
                j.update(kw)

    def on_step(text):
        _set(step=text)

    def on_line(line):
        with _jobs_lock:
            j = _jobs.get(jid)
            if not j:
                return
            j["log"].append(line)
            del j["log"][:-60]          # keep the tail, not the whole pull
            st = _step_of(line)
            if st:
                j["step"] = st

    def run():
        try:
            code, msg = _apply_update(m, svc, dry=dry, on_step=on_step, on_line=on_line)
        except Exception as e:          # noqa: BLE001 - a crash must still end the job
            code, msg = 500, str(e)
        finally:
            with _inflight_lock:
                _inflight.discard(key)
        _set(state="done" if code == 200 else "failed", code=code, msg=msg,
             step="done" if code == 200 else "failed", t=time.time())

    threading.Thread(target=run, daemon=True).start()
    return jid


def _ago(secs):
    """seconds -> 'hace unos segundos' / 'hace 12 min' / 'hace 2 h' / 'hace 3 d'."""
    s = max(int(secs), 0)
    if s < 90:
        return "hace unos segundos"
    if s < 5400:
        return f"hace {round(s / 60)} min"
    if s < 48 * 3600:
        return f"hace {round(s / 3600)} h"
    return f"hace {round(s / 86400)} d"


def _read_smart():
    """Parsed data/smart.json, re-read only when the file changes. {} on any
    problem — a missing or half-written file must never take the board down."""
    try:
        mtime = os.path.getmtime(SMART_FILE)
    except OSError:
        return {}
    if mtime != _smart_cache["mtime"]:
        try:
            with open(SMART_FILE) as f:
                doc = json.load(f)
        except (OSError, ValueError):
            doc = {}
        _smart_cache["doc"] = doc if isinstance(doc, dict) else {}
        _smart_cache["mtime"] = mtime
    return _smart_cache["doc"]


_du_cache = {"t": 0.0, "v": {}}
DU_TTL = 30.0          # disk usage moves slowly; no need to fork df per request


def _disk_usage(ssh=None):
    """mountpoint -> {used, total, pct}. {} on any problem.

    Cached: _smart_blocks runs per REQUEST (it is outside the board cache so a
    fresh smart.json shows up without a restart), and forking df on every poll
    would undo that cheapness.
    """
    now = time.monotonic()
    if _du_cache["v"] and now - _du_cache["t"] < DU_TTL:
        return _du_cache["v"]
    res = {}
    for ln in (_run(["df", "-kP"], ssh) or "").splitlines()[1:]:
        f = ln.split()
        if len(f) < 6:
            continue
        try:
            total, used = int(f[1]) * 1024, int(f[2]) * 1024
        except ValueError:
            continue
        if total <= 0:
            continue
        res[f[5]] = {"used": used, "total": total,
                     "pct": round(used / total * 100)}
    if res:
        _du_cache["t"], _du_cache["v"] = now, res
    return res


def _smart_blocks():
    """-> [{mid,name,stamp,stale,drives}] — one card's worth per reporting host.

    The file is keyed by HOSTNAME so more machines can report into it later
    (MacBook, build Mac) with no change here: a key matching a MACHINES id
    borrows that machine's display name, an unknown one still renders under its
    own hostname rather than disappearing.
    """
    hosts = _read_smart().get("hosts")
    if not isinstance(hosts, dict):
        return []
    now = time.time()
    out = []
    for hid, rec in sorted(hosts.items()):
        if not isinstance(rec, dict):
            continue
        drives = [d for d in (rec.get("drives") or []) if isinstance(d, dict)]
        if not drives:
            continue
        m = next((x for x in MACHINES if x["id"] == hid), None)
        # How full each drive is, matched by the mount point SMART already
        # reports. Only for the box we run on: reading df over ssh per request
        # is not worth it, and a drive with no matching mount (unmounted, or a
        # remote host) simply keeps showing size alone.
        du = _disk_usage() if (m and m.get("ssh") is None) else {}
        for d in drives:
            u = du.get(d.get("use"))
            if u:
                d["used"] = _fmt(u["used"])
                d["cap"] = _fmt(u["total"])
                d["used_pct"] = u["pct"]
        t = rec.get("t") if isinstance(rec.get("t"), (int, float)) else 0
        age = now - t
        out.append({"mid": m["id"] if m else hid,
                    "name": m["name"] if m else str(hid).upper(),
                    "stamp": _ago(age), "age_s": int(max(0, age)),
                    "stale": age > SMART_STALE,
                    "drives": drives})
    return out


# --- security posture (screen 4 · SEGURIDAD) ---------------------------------
# Four cheap LOCAL checks refreshed by a background thread; serving a page just
# copies the latest result, so a render never waits on systemctl or sudo. The
# moving parts live OUTSIDE this process on purpose — they are enabled systemd
# units, so they keep protecting the box across netops restarts AND reboots:
#   firewall   nftables.service (enabled oneshot, RemainAfterExit) loads
#              /etc/nftables.conf. Verified against the KERNEL via
#              `sudo -n nft list table inet noxafw` — security_setup.sh installs
#              a NOPASSWD rule for exactly that read-only command
#              (/etc/sudoers.d/netops-security). Without the rule the unit
#              state is the fallback signal ("active" = boot-time load ran).
#   signatures clamav-freshclam daemon keeps /var/lib/clamav/daily.* current.
#   scan       netops-clamscan.timer (daily, recent files) and
#              netops-clamscan-deep.timer (weekly full sweep) run
#              security_scan.py, which writes data/clamscan.json.
#   updates    unattended-upgrades driven by apt-daily-upgrade.timer; last-run
#              from the /var/lib/apt/periodic stamp files.
SEC_EVERY = 60.0
SCAN_FILE = os.environ.get("NETOPS_SCAN_FILE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "clamscan.json")
SCAN_HIST_FILE = SCAN_FILE.replace(".json", "-history.json")
SIG_DIR = "/var/lib/clamav"
SCAN_FRESH = 36 * 3600     # daily timer + slack; older means it missed a run
SCAN_STALE = 8 * 86400     # even the weekly deep sweep has lapsed
SIG_FRESH = 3 * 86400      # freshclam polls hourly; 3 days behind = broken
SEC_BLOCK_LEN = 181        # 60s samples of the fw drop counter -> 3h of deltas
APT_EVERY = 1800           # `apt list --upgradable` cadence (~0.2s, but no need
                           # to fork apt every minute for data that moves daily)

# blocked: ring of cumulative drop-counter readings (deltas are charted);
# sig/apt/ver: slow probes cached on mtime / their own clock.
_sec = {"v": None, "blocked": [], "sig": {"key": None, "v": None},
        "apt": {"t": 0.0, "v": None}, "engine": None}


def _unit_active(unit):
    """systemd's own word for the unit: active / inactive / failed / unknown."""
    _, out, _ = _run_rc(["systemctl", "is-active", unit], timeout=10)
    return out.strip() or "unknown"


def _scan_status(j, now):
    """Classify a parsed data/clamscan.json into the scan card.

    Pure function (file already read, clock passed in) so the selftest can
    drive every verdict without a filesystem. Malware found is the one hard
    red; a scan that is merely late degrades first and only goes red once even
    the weekly sweep window has passed.
    """
    if not isinstance(j, dict) or not isinstance(j.get("ts"), (int, float)):
        return {"status": "down", "age_s": None, "never": True}
    age = int(max(0, now - j["ts"]))
    out = {"status": "up", "age_s": age, "never": False,
           "mode": j.get("mode") or "daily",
           "scanned": j.get("scanned"), "infected": j.get("infected") or 0,
           "hits": [str(h) for h in (j.get("hits") or [])][:8],
           "duration_s": j.get("duration_s"), "err": j.get("err")}
    if out["infected"]:
        out["status"] = "down"
    elif j.get("err"):
        out["status"] = "degraded"
    elif age > SCAN_STALE:
        out["status"] = "down"
    elif age > SCAN_FRESH:
        out["status"] = "degraded"
    return out


def _sig_age(now):
    """Age in seconds of the newest ClamAV signature db, None when none exists."""
    best = None
    try:
        for f in os.listdir(SIG_DIR):
            if f.endswith((".cvd", ".cld")):
                m = os.path.getmtime(os.path.join(SIG_DIR, f))
                best = m if best is None or m > best else best
    except OSError:
        return None
    return int(max(0, now - best)) if best is not None else None


def _parse_nft_table(text):
    """Digest of `nft list table inet noxafw` -> facts for the firewall cards.

    Pure text -> dict so the selftest can feed it a canned ruleset. Extracted:
      rules/chains/policy  structure counts + the input chain's policy word
      sets                 [(name, #elements)] — the port sets the config uses
      blocked/probes       packets from the two labelled counters ("default
                           policy" = everything the chain refused; "admin" =
                           the probe counter sitting on @watched_admin_tcp)
      accepts              {port: "open"|"lan"} from accept rules with a
                           numeric dport; "lan" when the rule is source-scoped
                           (saddr). Ports of a saddr-scoped set rule inherit
                           "lan" via the set name. Feeds the surface card.
      watched              set() of ports the config explicitly deny-counts
    """
    out = {"rules": 0, "chains": 0, "policy": None, "sets": [], "blocked": None,
           "probes": None, "accepts": {}, "watched": set()}
    set_elems = {}
    for m in re.finditer(r"\tset (\w+) \{(.*?)\n\t\}", text, re.S):
        elems = re.search(r"elements = \{([^}]*)\}", m.group(2))
        ports = [int(p) for p in re.findall(r"\d+", elems.group(1))] if elems else []
        set_elems[m.group(1)] = ports
        out["sets"].append({"name": m.group(1), "ports": len(ports)})
    in_set = False
    for raw in text.splitlines():
        if raw.startswith("\tset "):
            in_set = True
        if in_set:                       # set internals are data, not rules
            in_set = raw.rstrip() != "\t}"
            continue
        s = raw.strip()
        if not s or s.startswith(("table", "}", "#")):
            continue
        if s.startswith("chain "):
            out["chains"] += 1
            continue
        pol = re.search(r"hook \w+ .*policy (\w+)", s)
        if pol:                          # chain header, not a rule
            if "hook input" in s:
                out["policy"] = pol.group(1)
            continue
        out["rules"] += 1
        cnt = re.search(r"counter packets (\d+)", s)
        if cnt and "accept" not in s:
            n = int(cnt.group(1))
            if "admin" in s:
                out["probes"] = n
            elif "default policy" in s or out["blocked"] is None:
                out["blocked"] = n
        if s.endswith("accept") and "dport" in s:
            lan = "saddr" in s
            for p in re.findall(r"dport (\d+)", s):
                out["accepts"].setdefault(int(p), "lan" if lan else "open")
            for sm in re.findall(r"dport @(\w+)", s):
                for p in set_elems.get(sm, ()):
                    out["accepts"].setdefault(p, "lan" if lan else "open")
    for ports in set_elems.values():     # a set no accept rule uses = deny list
        if ports and not any(p in out["accepts"] for p in ports):
            out["watched"].update(ports)
    return out


def _classify_ports(text, accepts, watched, names):
    """`ss -H -tulnp` -> the surface card: what listens where, and what the
    firewall does about it. Pure (text in, dict out) for the selftest.

    Sockets bound to every interface are the surface; each gets the firewall's
    verdict for its port: "open" (accept rule, unrestricted), "lan" (accept
    scoped to a local source), "blocked" (explicitly deny-counted), or
    "policy" (no accept rule -> the chain's drop policy eats it). Everything
    else is summarised as counts: loopback/LAN/tailnet/docker binds are not
    reachable from the WAN in the first place.
    """
    rows, counts, seen = [], {"local": 0, "lan": 0, "tail": 0, "docker": 0}, set()
    for ln in text.splitlines():
        parts = ln.split()
        if len(parts) < 5:
            continue
        proto, laddr = parts[0], parts[4]
        host, _, port = laddr.rpartition(":")
        if not port.isdigit():
            continue
        port = int(port)
        if host.startswith(("127.", "[::1]")) or host == "::1":
            cls = "local"
        elif "%tailscale" in host or host.startswith(("100.", "fd7a:")):
            cls = "tail"
        elif "%br-" in host or "%docker" in host or re.match(
                r"172\.(1[6-9]|2\d|3[01])\.", host):
            cls = "docker"
        elif host.startswith("192.168."):
            cls = "lan"
        else:                                      # *, 0.0.0.0, [::], ::
            cls = "all"
        if (proto, port, cls) in seen:
            continue
        seen.add((proto, port, cls))
        if cls != "all":
            counts[cls] += 1
            continue
        proc = re.search(r'users:\(\("([^"]+)"', ln)
        fw = ("blocked" if port in watched else accepts.get(port, "policy"))
        rows.append({"port": port, "proto": proto,
                     "name": names.get(port) or (proc.group(1) if proc else ""),
                     "fw": fw})
    rows.sort(key=lambda r: ({"open": 0, "lan": 1, "blocked": 2, "policy": 2}
                             .get(r["fw"], 3), r["port"]))
    shown = [r for r in rows if r["fw"] in ("open", "lan")]
    hidden = len(rows) - len(shown)
    return {"rows": shown + [r for r in rows if r["fw"] not in ("open", "lan")][:4],
            "more": max(0, hidden - 4), "public": len(rows), **counts}


def _sig_info():
    """Signature-db facts via `sigtool --info`, re-read only when a db changes.

    Header-only reads (~10ms each), but there is still no reason to fork three
    processes a minute for files freshclam touches a few times a day.
    """
    paths = [p for p in (os.path.join(SIG_DIR, f) for f in
                         ("daily.cld", "daily.cvd", "main.cld", "main.cvd",
                          "bytecode.cld", "bytecode.cvd")) if os.path.exists(p)]
    try:
        key = tuple((p, os.path.getmtime(p)) for p in paths)
    except OSError:
        key = None
    if key and key == _sec["sig"]["key"]:
        return _sec["sig"]["v"]
    total, daily_ver = 0, None
    for p in paths:
        _, out, _ = _run_rc(["sigtool", "--info", p], timeout=15)
        n = re.search(r"^Signatures:\s*(\d+)", out, re.M)
        total += int(n.group(1)) if n else 0
        v = re.search(r"^Version:\s*(\d+)", out, re.M)
        if v and "daily" in p:
            daily_ver = int(v.group(1))
    v = {"total": total or None, "daily_ver": daily_ver} if paths else None
    _sec["sig"] = {"key": key, "v": v}
    return v


def _apt_pending():
    """(total, security, names) pending upgrades, refreshed every APT_EVERY."""
    if time.monotonic() - _sec["apt"]["t"] < APT_EVERY and _sec["apt"]["v"]:
        return _sec["apt"]["v"]
    _, out, _ = _run_rc(["apt", "list", "--upgradable"], timeout=30)
    pkgs = re.findall(r"^([\w.+-]+)/(\S+)", out, re.M)
    sec = [p for p, origin in pkgs if "security" in origin]
    v = {"total": len(pkgs), "security": len(sec),
         "names": (sec + [p for p, o in pkgs if "security" not in o])[:6]}
    _sec["apt"] = {"t": time.monotonic(), "v": v}
    return v


def _security_check():
    now = time.time()

    # firewall — ask the kernel when the sudoers rule allows it; otherwise fall
    # back to the unit. rc!=0 is ambiguous (no sudo rule vs table truly absent),
    # so stderr decides: sudo refusals mention the password/policy, nft's own
    # "No such file or directory" means we DID look and the table is gone.
    unit = _unit_active("nftables")
    rc, out, err = _run_rc(["sudo", "-n", "/usr/sbin/nft", "list", "table",
                            "inet", "noxafw"], timeout=10)
    accepts, watched = {}, set()
    if rc == 0:
        p = _parse_nft_table(out)
        accepts, watched = p.pop("accepts"), p.pop("watched")
        fw = {"status": "up", "verified": True, "unit": unit, **p}
        if fw["blocked"] is not None:
            _sec["blocked"].append(fw["blocked"])
            del _sec["blocked"][:-SEC_BLOCK_LEN]
            # chart series: drops per sample; a reload resets the kernel
            # counter, which would chart as a huge negative spike -> clamp
            fw["bhist"] = [max(0, b - a) for a, b in
                           zip(_sec["blocked"], _sec["blocked"][1:])]
    elif "password" in err.lower() or "not allowed" in err.lower() \
            or "may not run" in err.lower():
        fw = {"status": "up" if unit == "active" else "down",
              "verified": False, "unit": unit, "rules": None}
    else:
        fw = {"status": "down", "verified": True, "unit": unit, "rules": 0}

    # network surface — what listens vs what the firewall lets through. Port
    # names come from this machine's own service catalog, with a couple of
    # well-known system ports the catalog has no reason to carry.
    names = {22: "sshd", 41641: "tailscale", 6881: "torrent", 6771: "torrent"}
    for m in MACHINES:
        if m.get("ssh") is None:
            names.update({s["port"]: s["name"] for s in m["catalog"]
                          if s.get("port")})
    _, ssout, _ = _run_rc(["ss", "-H", "-tulnp"], timeout=10)
    surface = _classify_ports(ssout, accepts, watched, names) if ssout else None

    # antivirus signatures
    have_av = os.path.exists("/usr/bin/clamscan")
    fresh = _unit_active("clamav-freshclam")
    sage = _sig_age(now)
    if not have_av:
        av = {"status": "down", "installed": False, "svc": fresh, "age_s": None}
    elif sage is None:
        av = {"status": "down", "installed": True, "svc": fresh, "age_s": None}
    else:
        av = {"status": "up" if fresh == "active" and sage <= SIG_FRESH
              else "degraded",
              "installed": True, "svc": fresh, "age_s": sage}
    if have_av:
        av["db"] = _sig_info()
        if _sec["engine"] is None:
            _, vout, _ = _run_rc(["clamscan", "--version"], timeout=15)
            _sec["engine"] = (vout.split("/")[0].replace("ClamAV", "").strip()
                              or None) if vout else None
        av["engine"] = _sec["engine"]

    # last scan + its history (small file the scan script appends to)
    try:
        with open(SCAN_FILE) as f:
            j = json.load(f)
    except Exception:
        j = None
    scan = _scan_status(j, now)
    if isinstance(j, dict):
        scan["known"] = j.get("known")
    # a oneshot unit reads "activating" for its whole run; either unit counts
    scan["running"] = next((k for k, u in (("deep", "netops-clamscan-deep.service"),
                                           ("daily", "netops-clamscan.service"))
                            if _unit_active(u) in ("active", "activating")), None)
    try:
        with open(SCAN_HIST_FILE) as f:
            hist = json.load(f)
        scan["hist"] = [h for h in hist if isinstance(h, dict)][-30:]
    except Exception:
        scan["hist"] = []

    # automatic security patches
    have_uu = os.path.exists("/usr/bin/unattended-upgrade")
    timer = _unit_active("apt-daily-upgrade.timer")
    try:
        with open("/etc/apt/apt.conf.d/20auto-upgrades") as f:
            conf_ok = bool(re.search(r'Unattended-Upgrade\s+"1"', f.read()))
    except OSError:
        conf_ok = False
    stamp = None
    for p in ("/var/lib/apt/periodic/unattended-upgrades-stamp",
              "/var/lib/apt/periodic/upgrade-stamp"):
        try:
            m = os.path.getmtime(p)
            stamp = m if stamp is None or m > stamp else stamp
        except OSError:
            pass
    upd = {"status": ("up" if conf_ok and timer == "active" else "degraded")
           if have_uu else "down",
           "installed": have_uu, "timer": timer, "conf": conf_ok,
           "age_s": int(max(0, now - stamp)) if stamp is not None else None,
           "pending": _apt_pending() if have_uu else None}

    sec = {"firewall": fw, "sigs": av, "scan": scan, "updates": upd}
    worst = [p["status"] for p in sec.values()]
    sec["alert"] = ("down" if "down" in worst
                    else "degraded" if "degraded" in worst else None)
    sec["surface"] = surface        # informational; never drives the badge
    try:                            # tunnel map for the RED topology screen
        with open(CF_CONFIG) as f:
            sec["tunnel"] = _parse_cf_ingress(f.read())
    except Exception:
        sec["tunnel"] = None
    sec["checked"] = int(now)
    return sec


# --- push notifications (ntfy) -----------------------------------------------
# The badge tells whoever is LOOKING at the board; the push tells the phone.
# Config lives in data/ntfy.json ({url, topic, token}) — deliberately a file,
# not source: it holds a credential, and re-pointing it (e.g. to the Air's own
# ntfy later) must not need a code change or even a netops restart, so it is
# re-read on every push. Missing/invalid file = pushes silently off.
NTFY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "data", "ntfy.json")


def _ntfy_conf():
    try:
        with open(NTFY_FILE) as f:
            c = json.load(f)
        return c if isinstance(c, dict) and c.get("url") and c.get("topic") else None
    except Exception:
        return None


def _ntfy_push(title, body, priority="default", tags=""):
    """Best-effort publish; never raises (a dead ntfy must not kill _sec_loop)."""
    c = _ntfy_conf()
    if not c:
        return False
    try:
        hdrs = {"Title": title, "Priority": priority, "Tags": tags}
        if c.get("token"):
            hdrs["Authorization"] = "Bearer " + c["token"]
        req = urllib.request.Request(c["url"].rstrip("/") + "/" + c["topic"],
                                     data=body.encode(), headers=hdrs)
        urllib.request.urlopen(req, timeout=10).read()
        return True
    except Exception:
        return False


# ---- n8n notification router --------------------------------------------
# Owner decision 2026-08-26: notification ROUTING lives in n8n. netops emits
# raw events to the router webhook (data/n8n-feed.json {url, token}, re-read
# per event like ntfy.json); the workflow shapes and pushes to ntfy. If n8n
# is unreachable the event falls back to a DIRECT ntfy push — a broken n8n
# must degrade to plainer notifications, never to silence.
N8N_FEED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "data", "n8n-feed.json")


def _n8n_conf():
    try:
        with open(N8N_FEED_FILE) as f:
            c = json.load(f)
        return c if isinstance(c, dict) and c.get("url") and c.get("token") else None
    except Exception:
        return None


EVENTS_LEN = 200
_events = []   # recent alert-worthy events (newest last), for GET /api/alerts


def _log_event(title, body, priority, tags):
    _events.append({"ts": time.time(), "title": title, "body": body,
                    "priority": priority, "tags": tags})
    del _events[:-EVENTS_LEN]


def _n8n_event(title, body, priority="default", tags=""):
    """Route one notification through n8n; direct ntfy is the fallback.

    Every call is logged to _events regardless of delivery outcome — a dead
    ntfy/n8n must not hide the underlying problem from /api/alerts, only from
    the push channel.
    """
    _log_event(title, body, priority, tags)
    c = _n8n_conf()
    if c:
        try:
            req = urllib.request.Request(
                c["url"],
                data=json.dumps({"title": title, "body": body,
                                 "priority": priority, "tags": tags}).encode(),
                headers={"Content-Type": "application/json",
                         "X-Feed-Token": c["token"]})
            urllib.request.urlopen(req, timeout=10).read()
            return True
        except Exception:
            pass
    return _ntfy_push(title, body, priority, tags)


def _sec_summary(v):
    """['firewall down unit inactive', ...] — the troubled components, tersely."""
    bits = []
    for key, label in (("firewall", "firewall"), ("sigs", "antivirus"),
                       ("scan", "scan"), ("updates", "patching")):
        d = (v or {}).get(key) or {}
        st = d.get("status")
        if st not in ("down", "degraded"):
            continue
        why = ""
        if key == "scan" and d.get("infected"):
            why = f' {d["infected"]} infected'
        elif key == "scan" and d.get("never"):
            why = " never ran"
        elif key == "firewall" and not d.get("verified"):
            why = f' unit {d.get("unit")}'
        bits.append(f"{label} {st}{why}")
    return bits


def _alert_transition(prev, cur):
    """None, or (title, body, priority, tags) for one push. Pure, selftest-fed.

    Pushes only on CHANGE — the level moves, or the set of troubled components
    shifts while red/amber — never on a repeat of the same bad state, so a
    broken component costs one notification, not one per minute. Recovery to
    all-green gets its own push (silence after an alert must not be ambiguous).
    """
    p, c = (prev or {}).get("alert"), (cur or {}).get("alert")
    pb, cb = _sec_summary(prev), _sec_summary(cur)
    if c == p and cb == pb:
        return None
    if c is None:
        return (("Security: all clear", "every component back to operational",
                 "default", "white_check_mark") if p is not None else None)
    return ("Security alert" if c == "down" else "Security warning",
            "; ".join(cb) or str(c),
            "high" if c == "down" else "default",
            "rotating_light" if c == "down" else "warning")


def _scan_transition(p, c):
    """None, or one push when a scan starts or finishes. Pure, selftest-fed.

    `running` is "daily"/"deep" while the systemd unit is active, falsy after.
    An infected result ALSO trips the red security alert via _alert_transition;
    the extra summary push here is deliberate for something that serious.
    """
    p, c = p or {}, c or {}
    pr, cr = p.get("running"), c.get("running")
    if not pr and cr:
        return ("Scan started", f"antivirus sweep ({cr}) is running",
                "min", "mag")
    if pr and not cr:
        # a fresh result file proves the scan actually wrote its verdict; a
        # stale one means the process died (OOM/SIGKILL) and the numbers below
        # would be the PREVIOUS run's (adversarial review F2)
        age = c.get("age_s")
        if age is None or age > 3 * SEC_EVERY:
            return ("Scan ended without a result",
                    "the scan unit stopped but no fresh verdict was written — "
                    "check journalctl -u netops-clamscan",
                    "high", "warning")
        n = c.get("scanned")
        n = "?" if n is None else f"{n:,}"
        inf = c.get("infected") or 0
        if inf:
            return ("Scan finished: INFECTED",
                    f"{inf} infected of {n} files scanned — check the board",
                    "high", "rotating_light")
        return ("Scan finished: clean", f"{n} files scanned, nothing found",
                "default", "white_check_mark")
    return None


def _sec_loop():
    prev = None
    while True:
        try:
            cur = _security_check()
            _sec["v"] = cur
            note = _alert_transition(prev, cur)
            if note:
                _n8n_event(*note)
            snote = _scan_transition((prev or {}).get("scan"), cur.get("scan"))
            if snote:
                _n8n_event(*snote)
            prev = cur
        except Exception:
            pass
        time.sleep(SEC_EVERY)


# ---- service / machine up-down pushes ---------------------------------------
# Same one-push-per-change philosophy as the security alerts, plus a debounce:
# a transition must survive two consecutive checks (~2 min) before it pushes,
# so a single failed HTTP probe or an ssh hiccup never reaches the phone.

SVC_EVERY = 60.0


# local health thresholds (homeserver only — the machine netops runs on)
SYS_LOAD_X = 1.5          # 5-min load >= 1.5 x cores       -> sys:load high
SYS_RAM_MIN_PCT = 8       # MemAvailable below this percent -> sys:ram low
SYS_TEMP_HOT = 85.0       # CPU temperature, deg C          -> sys:temp hot
DISK_FULL_PCT = 93        # filesystem used percent         -> disk:<p> full
WATCH_MOUNTS = ("/media", "/music")


def _health_states(load5, ncpu, ram_avail_pct, temp_c, mounts, disk_used, smart):
    """Pure classifier -> {key: state}, merged into the fleet snapshot so the
    same debounce/one-push machinery covers drives and load too.
    mounts {path: bool}, disk_used {path: used%}, smart {drive: verdict}."""
    st = {}
    if load5 is not None and ncpu:
        st["sys:load"] = "high" if load5 >= SYS_LOAD_X * ncpu else "ok"
    if ram_avail_pct is not None:
        st["sys:ram"] = "low" if ram_avail_pct < SYS_RAM_MIN_PCT else "ok"
    if temp_c is not None:
        st["sys:temp"] = "hot" if temp_c >= SYS_TEMP_HOT else "ok"
    for path, ok in (mounts or {}).items():
        st["mount:" + path] = "mounted" if ok else "missing"
    for path, pct in (disk_used or {}).items():
        st["disk:" + path] = "full" if pct >= DISK_FULL_PCT else "ok"
    for name, verdict in (smart or {}).items():
        st["smart:" + name] = verdict or "ok"
    return st


_mnt_probes = {}   # path -> last disk_usage probe thread (hung ones must not stack)


def _probe_mount(p, out):
    try:
        du = shutil.disk_usage(p)
        out[p] = round(du.used / du.total * 100)
    except Exception:
        out[p] = None


def _local_health(data):
    """Raw readings for _health_states. Best-effort, never raises."""
    load5 = ncpu = ram = temp = None
    mounts, used, smart = {}, {}, {}
    try:
        load5, ncpu = os.getloadavg()[1], os.cpu_count()
    except Exception:
        pass
    try:
        mi = {}
        with open("/proc/meminfo") as f:
            for ln in f:
                k, v = ln.split(":", 1)
                mi[k.strip()] = int(v.strip().split()[0])
        ram = round(mi["MemAvailable"] / mi["MemTotal"] * 100, 1)
    except Exception:
        pass
    try:
        for m in (data or {}).get("machines", []):
            if m.get("id") == LOCAL_ID:
                temp = (m.get("host") or {}).get("temp_c")
    except Exception:
        pass
    # mount checks must never block the push pipeline: a dying USB mount can
    # hang statvfs in D-state (adversarial review F6). /proc/mounts is a
    # non-blocking presence check; disk_usage runs in a throwaway thread with
    # a join timeout, and a hung probe marks the mount sick (which IS news).
    try:
        with open("/proc/mounts") as f:
            mounted = {ln.split()[1] for ln in f if len(ln.split()) > 1}
    except Exception:
        mounted = None
    for p in WATCH_MOUNTS:
        prev = _mnt_probes.get(p)
        if prev is not None and prev.is_alive():
            mounts[p] = False          # last probe still stuck -> sick mount
            continue
        try:
            mounts[p] = (p in mounted) if mounted is not None \
                else os.path.ismount(p)
        except Exception:
            mounts[p] = False
        if not mounts[p]:
            continue
        res = {}
        th = threading.Thread(target=_probe_mount, args=(p, res), daemon=True)
        _mnt_probes[p] = th
        th.start()
        th.join(4)
        if th.is_alive():
            mounts[p] = False          # statvfs hung mid-probe -> sick mount
        elif res.get(p) is not None:
            used[p] = res[p]
    try:
        for b in _smart_blocks():
            mid = b.get("mid")
            for d in (b.get("drives") or []):
                v, nm = d.get("verdict"), d.get("name") or d.get("dev")
                # "unknown" means we have no health data (a remote drive with no
                # root for smartctl) - never alert on the absence of a signal
                if v and nm and v != "unknown":
                    key = str(nm) if mid == LOCAL_ID else f"{mid}:{nm}"
                    smart[key] = str(v)
                # capacity IS measurable for remote drives even without SMART, and
                # a filling disk is the failure that actually arrives first. Local
                # mounts stay with the WATCH_MOUNTS probe above; this adds every
                # other reporting host.
                pct, use = d.get("used_pct"), d.get("use")
                if mid != LOCAL_ID and pct is not None and use:
                    used[f"{mid}:{use}"] = pct
    except Exception:
        pass
    return load5, ncpu, ram, temp, mounts, used, smart


def _fleet_snapshot(data):
    """{'m/<id>': online|offline, 's/<mid>/<name>': status} from one build.

    Services on an offline machine are omitted: the machine line carries the
    news once, instead of one push per service it hosts.
    """
    snap = {}
    for m in data.get("machines", []):
        on = bool((m.get("host") or {}).get("online"))
        snap["m/" + m["id"]] = "online" if on else "offline"
        if not on:
            continue
        for c in m.get("categories", []):
            for s in c.get("services", []):
                snap[f's/{m["id"]}/{s["name"]}'] = s.get("status")
    return snap


# states that push as bad news, by severity; every other new state = recovery
_BAD_HIGH = {"down", "offline", "missing", "replace_now", "full"}
_BAD_WARN = {"degraded", "high", "low", "hot", "watch", "replace_soon"}
_BAD_WORD = {"missing": "DISCONNECTED", "full": "almost full"}


def _nice_key(k):
    """'s/homeserver/qbittorrent' -> 'qbittorrent', 'mount:/media' -> '/media drive', ..."""
    if k.startswith("m/"):
        return k.split("/")[1] + " machine"
    if k.startswith("s/"):
        return k.split("/", 2)[2]
    if k.startswith("mount:"):
        return k.split(":", 1)[1] + " drive"
    if k.startswith("disk:"):
        return k.split(":", 1)[1] + " space"
    if k.startswith("smart:"):
        return k.split(":", 1)[1] + " SMART"
    if k.startswith("sys:"):
        return "system " + k.split(":", 1)[1]
    return k


def _fleet_problems(state):
    """[str] terse description of every CURRENTLY bad key in a fleet-state
    snapshot (not just what changed this cycle) — feeds /api/alerts. Pure."""
    return [f"{_nice_key(k)} {_BAD_WORD.get(v, v)}"
            for k, v in sorted((state or {}).items())
            if v in _BAD_HIGH or v in _BAD_WARN]


def _fleet_push(confirmed):
    """None, or ONE combined (title, body, priority, tags) per cycle. Pure.

    confirmed: {key: (old_state, new_state)} of debounced transitions. All of
    a cycle's news rides in a single push (a docker restart flips many services
    at once — that must not fire a burst of notifications). Keys: m/<id>
    machine, s/<mid>/<name> service, plus _health_states' mount:/disk:/smart:/
    sys: local-health keys.
    """
    if not confirmed:
        return None
    bad = [(k, b) for k, (a, b) in sorted(confirmed.items())
           if b in _BAD_HIGH or b in _BAD_WARN]
    good = [(k, b) for k, (a, b) in sorted(confirmed.items())
            if b not in _BAD_HIGH and b not in _BAD_WARN]
    lines = [f"{_nice_key(k)} {_BAD_WORD.get(b, b)}" for k, b in bad]
    lines += [f"{_nice_key(k)} " + ("back online" if k.startswith("m/") else
              "back up" if k.startswith("s/") else "recovered")
              for k, b in good]
    worst = ("down" if any(b in _BAD_HIGH for _, b in bad)
             else "degraded" if bad else "up")
    return ({"down": "Lab alert", "degraded": "Lab warning",
             "up": "Recovered"}[worst],
            "; ".join(lines),
            "high" if worst == "down" else "default",
            {"down": "rotating_light", "degraded": "warning",
             "up": "white_check_mark"}[worst])


_fleet_state = None   # current key -> state, mirrors _svc_loop's debounced
                      # view; read by /api/alerts as "what's bad right now"


def _svc_loop():
    global _fleet_state
    pend = {}
    while True:
        try:
            data = get_data()
            snap = _fleet_snapshot(data)
            snap.update(_health_states(*_local_health(data)))
            if _fleet_state is None:
                _fleet_state = snap        # boot baseline: never re-announce old news
            else:
                confirmed = {}
                for k, st in snap.items():
                    if k not in _fleet_state:
                        # new or RESURRECTED key (e.g. services hidden during a
                        # one-tick machine flap): baseline silently. Treating
                        # None->state as news replayed old, already-announced
                        # states as a "Recovered"/alert storm (adversarial
                        # review F1) — the machine line carries that news.
                        _fleet_state[k] = st
                        pend.pop(k, None)
                        continue
                    if st == _fleet_state.get(k):
                        pend.pop(k, None)
                        continue
                    s0, n = pend.get(k, (None, 0))
                    n = n + 1 if s0 == st else 1
                    pend[k] = (st, n)
                    if n >= 2:
                        confirmed[k] = (_fleet_state.get(k), st)
                        _fleet_state[k] = st
                        pend.pop(k, None)
                for k in [k for k in _fleet_state if k not in snap]:   # catalog edits
                    _fleet_state.pop(k, None)
                    pend.pop(k, None)
                note = _fleet_push(confirmed)
                if note:
                    _n8n_event(*note)
        except Exception:
            pass
        time.sleep(SVC_EVERY)


def _with_history(data):
    """Attach hist[] + uptime% to each service from _history, at serve time.

    hist is a COPY: the sampler thread keeps mutating the ring in place, and
    json.dumps must never iterate a list that is changing under it.
    """
    for m in data["machines"]:
        for c in m.get("categories", []):
            for s in c["services"]:
                hist = list(_history.get(f'{m["id"]}/{s["name"]}', []))
                s["hist"] = hist
                s["uptime"] = round(sum(hist) / len(hist) * 100) if hist else None
    data["disks"] = _smart_blocks()   # serve time: fresh file, no restart needed
    data["security"] = _sec["v"]      # latest sweep from _sec_loop; None at boot
    return data


LOGIN_PAGE = r"""<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>__TITLE__</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M2.5 12h4l3-8 4 16 3-8h4.5' fill='none' stroke='%233ddc84' stroke-width='2.4' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<style>
:root{--bg:#0a0d12;--card:#141a23;--border:#212a35;--border2:#2f3a47;--text:#eef2f7;
  --text2:#9fb0c2;--text3:#7e8ea0;--accent:#6366f1;--accent-t:#a5b4fc;--down:#f04444;--ring:#818cf8;
  --mono:ui-monospace,"SF Mono",Menlo,Consolas,monospace;
  --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;
  background:var(--bg);color:var(--text);font-family:var(--sans);font-size:15px;line-height:1.6;
  background-image:radial-gradient(ellipse 900px 520px at 50% -40px,rgba(99,102,241,.10),transparent 72%)}
:focus-visible{outline:2px solid var(--ring);outline-offset:2px}
.box{width:100%;max-width:360px;background:var(--card);border:1px solid var(--border);
  border-radius:14px;padding:26px 24px 24px}
.brand{font:700 15px/1 var(--mono);letter-spacing:1px}
.brand b{color:var(--accent-t)}
.sub{margin-top:9px;font:400 13px/1.5 var(--sans);color:var(--text3)}
label{display:block;margin:15px 0 5px;font:600 9px var(--mono);letter-spacing:1.1px;
  text-transform:uppercase;color:var(--text3)}
input{width:100%;background:var(--bg);border:1px solid var(--border2);border-radius:8px;
  padding:10px 12px;color:var(--text);font:500 14px var(--mono)}
input:focus{outline:2px solid var(--ring);outline-offset:1px;border-color:transparent}
button{width:100%;margin-top:20px;border:1px solid var(--accent);border-radius:8px;
  background:var(--accent);color:#fff;font:600 12px var(--mono);padding:11px;cursor:pointer}
button:hover:not(:disabled){background:#4f52e0}
button:disabled{opacity:.6;cursor:wait}
.err{margin-top:13px;min-height:19px;font:500 12px/1.5 var(--mono);color:var(--down)}
@media(prefers-reduced-motion:reduce){*{transition:none!important}}
</style></head><body>
<main class="box">
  <div class="brand" id="logo"></div>
  <p class="sub">Panel privado. Inicia sesión para ver el estado.</p>
  <form id="f">
    <label for="u">usuario</label>
    <input id="u" autocomplete="username" autocapitalize="off" autocorrect="off" required autofocus>
    <label for="p">contraseña</label>
    <input id="p" type="password" autocomplete="current-password" required>
    <button id="b">entrar</button>
  </form>
  <p class="err" id="e" role="alert"></p>
</main>
<script>
const $=id=>document.getElementById(id);
$("logo").innerHTML="__TITLE__".replace(/\/\//,"<b>//</b>");
// btoa() throws on non-ASCII; encode UTF-8 first so accented passwords work.
const b64=s=>btoa(String.fromCharCode.apply(null,new TextEncoder().encode(s)));
$("f").addEventListener("submit",async e=>{
  e.preventDefault();
  const b=$("b");b.disabled=true;b.textContent="…";$("e").textContent="";
  try{
    const r=await fetch("/api/login",{method:"POST",
      headers:{"Authorization":"Basic "+b64($("u").value+":"+$("p").value)}});
    if(r.ok){location.replace("/");return;}
    if(r.status===401){$("e").textContent="usuario o contraseña incorrectos";$("p").value="";}
    else $("e").textContent=r.status===429
      ?"demasiados intentos — espera unos minutos"
      :"error del servidor ("+r.status+") — inténtalo de nuevo";
  }catch(err){$("e").textContent="error de conexión";}
  b.disabled=false;b.textContent="entrar";$("p").focus();
});
</script></body></html>"""


PAGE_M = r"""<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>__TITLE__</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M2.5 12h4l3-8 4 16 3-8h4.5' fill='none' stroke='%233ddc84' stroke-width='2.4' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<style>
:root{
  --bg:#05090a;--panel:#0a1012;--card:#0c1315;--card2:#101a1c;
  --line:#17262a;--line2:#22383c;
  --mint:#5df2a0;--mint2:#2fd98a;--cyan:#67e8f9;--amber:#f5b544;--red:#ff6b6b;--violet:#a78bfa;
  --txt:#dbeae5;--txt2:#8fa9a2;--dim:#6f8c86;
  --mono:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,"DejaVu Sans Mono",monospace;
  --glow:0 0 12px rgba(93,242,160,.45);
}
*{box-sizing:border-box;margin:0;padding:0}
html{-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}
body{min-height:100dvh;background:var(--bg);color:var(--txt);
  font-family:var(--mono);font-size:14px;line-height:1.5;overflow-x:hidden;
  font-variant-numeric:tabular-nums;
  background-image:radial-gradient(ellipse 820px 420px at 50% -120px,rgba(45,217,138,.10),transparent 72%)}
:focus-visible{outline:2px solid var(--mint2);outline-offset:2px}
.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}
a{color:inherit;text-decoration:none}

.app{max-width:1180px;margin:0 auto;min-height:100dvh;display:flex;flex-direction:column}

/* ---------- top bar ---------- */
.top{display:flex;align-items:center;gap:14px;padding:14px 20px 11px;
  border-bottom:1px solid var(--line)}
.bd{width:7px;height:7px;border-radius:50%;background:var(--mint);box-shadow:var(--glow);flex:none}
.brand{font-weight:700;font-size:14px;letter-spacing:2.4px;color:var(--mint);
  text-shadow:0 0 14px rgba(93,242,160,.4);white-space:nowrap}
.brand i{color:var(--dim);font-style:normal}
.sp{flex:1}
.scr{display:flex;align-items:center;gap:9px}
.scr .no{display:grid;place-items:center;width:19px;height:19px;border-radius:5px;
  background:var(--mint);color:#04120b;font-weight:700;font-size:11px}
.scr .nm{font-weight:600;font-size:12.5px;letter-spacing:3px;color:var(--txt)}
.clk{font-weight:700;font-size:20px;letter-spacing:1.6px;color:var(--mint);
  text-shadow:0 0 14px rgba(93,242,160,.35)}
.tb{border:1px solid var(--line2);border-radius:6px;background:transparent;color:var(--dim);
  font:600 9.5px/1 var(--mono);letter-spacing:1.2px;padding:7px 9px;cursor:pointer}
.tb:hover{color:var(--mint);border-color:var(--mint2)}

/* ---------- 5 · RED (network surface) ---------- */
.nswrap{width:100%;overflow-x:auto;padding:6px 0 2px}
.nswrap svg{display:block;width:100%;min-width:760px;height:auto}
.nsleg{margin:2px 4px 0;font:600 10.5px var(--mono);letter-spacing:.9px;color:var(--dim)}
.nst{font:700 12px var(--mono);letter-spacing:2px}
.nsh{font:700 14px var(--mono);letter-spacing:2.4px}
.nss{font:400 11px var(--mono)}
.nsv{font:700 12px var(--mono)}
.nsl{font:400 10px var(--mono)}
.nsfw{font:700 12px var(--mono);letter-spacing:3px}
.nsn.off rect{stroke-dasharray:5 4}
/* One shared keyframe; speed is picked from a SMALL set of classes rather than a
   continuously-varying duration, because changing animation-duration rescales a
   running animation's progress and makes the dashes visibly jump. */
@keyframes nsflow{to{stroke-dashoffset:-32}}
.ns-flow{stroke-dasharray:5 11;animation:nsflow 2.4s linear infinite}
.ns-flow.s1{animation-duration:2.6s}
.ns-flow.s2{animation-duration:1.4s}
.ns-flow.s3{animation-duration:.7s}
.ns-dead{stroke-dasharray:4 9;opacity:.4}
@keyframes nspulse{0%,100%{opacity:1}50%{opacity:.45}}
.ns-pulse{animation:nspulse 2.2s ease-in-out infinite}
@media (prefers-reduced-motion:reduce){
  .ns-flow,.ns-pulse{animation:none}
  .ns-flow{stroke-dasharray:none}}

/* ---------- screens ---------- */
.screens{flex:1;display:flex;overflow-x:auto;overflow-y:hidden;scroll-snap-type:x mandatory;
  scroll-behavior:smooth;-webkit-overflow-scrolling:touch;scrollbar-width:none}
.screens::-webkit-scrollbar{display:none}
.screen{flex:0 0 100%;min-width:100%;scroll-snap-align:center;scroll-snap-stop:always;
  padding:18px 20px 10px;overflow-y:auto}

.lbl{font-weight:600;font-size:9.5px;letter-spacing:2.2px;text-transform:uppercase;color:var(--dim)}
.card{background:var(--card);border:1px solid var(--line);border-radius:11px;padding:14px 16px}
.card.sp3{border-left:3px solid var(--mint)}

/* ---------- gauges ---------- */
.gauges{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;align-items:center;
  margin-bottom:14px}
.gw{position:relative;display:grid;place-items:center}
.gw svg{width:100%;max-width:210px;height:auto;display:block}
.gw .gt{position:absolute;top:50%;left:50%;transform:translate(-50%,-46%);text-align:center;width:100%}
.gw .gv{font-weight:700;font-size:34px;letter-spacing:-1px;line-height:1}
.gw .gl{margin-top:7px;font-weight:600;font-size:9.5px;letter-spacing:2.2px;color:var(--dim)}
.gw.big .gv{font-size:42px}
.gw.big svg{max-width:250px}

/* ---------- info cards row ---------- */
.row3{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:10px}
.big{font-weight:700;font-size:29px;letter-spacing:-.6px;line-height:1.1;margin-top:5px}
.sub{margin-top:5px;font-size:11.5px;color:var(--txt2)}
.sub b{font-weight:700;color:var(--mint)}
.thermo{display:flex;gap:22px;margin-top:9px;flex-wrap:wrap}
.thermo .t k{display:block;font-weight:600;font-size:9.5px;letter-spacing:1.6px;color:var(--dim)}
.thermo .t v{display:block;font-weight:700;font-size:19px;color:var(--cyan);margin-top:3px}
.thermo .t v.warm{color:var(--amber)}.thermo .t v.hot{color:var(--red)}
.netcard{position:relative}
.netpop{position:absolute;left:0;right:0;top:calc(100% + 6px);z-index:40;
  background:var(--card2);border:1px solid var(--line2);border-radius:10px;padding:11px 13px;
  opacity:0;visibility:hidden;transform:translateY(-4px);
  transition:opacity .15s,transform .15s,visibility .15s;
  box-shadow:0 16px 38px rgba(0,0,0,.6)}
.netcard:hover .netpop,.netcard:focus-within .netpop{opacity:1;visibility:visible;transform:none}
.nrow{display:grid;grid-template-columns:minmax(0,1fr) auto auto;gap:12px;padding:5px 0;
  font-size:11.5px;border-top:1px solid var(--line)}
.nrow:first-of-type{border-top:0}
.nrow .nn{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--txt)}
.nrow .nd{color:var(--mint);font-weight:700}
.nrow .nu{color:var(--cyan);font-weight:700}
.nblind{margin-top:9px;padding-top:8px;border-top:1px solid var(--line);
  font-size:10.5px;line-height:1.5;color:var(--dim)}

/* ---------- system line ---------- */
.sysline{margin-top:9px;font-size:12px;color:var(--txt2);line-height:2;
  display:flex;flex-wrap:wrap;align-items:baseline;gap:0 4px}
.sysline .h{font-weight:700;color:var(--mint);letter-spacing:1.4px;margin-right:8px}
.sysline b{color:var(--dim);font-weight:600;letter-spacing:1.3px;margin-right:6px}
.sysline s{text-decoration:none;color:var(--txt);font-weight:700}
.sysline em{font-style:normal;color:var(--line2);margin:0 9px}
.sysrow{padding:7px 0;border-top:1px solid var(--line)}
.sysrow:first-of-type{border-top:0}
.dk{display:grid;grid-template-columns:12px minmax(0,1fr) auto auto auto auto;align-items:center;
  gap:11px;padding:9px 0;border-top:1px solid var(--line);font-size:12.5px}
.dk:first-of-type{border-top:0}
.dk .dn{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dk .dn u{text-decoration:none;color:var(--dim);margin-left:9px;font-size:11px}
.dk .dn u b{font-weight:700;color:var(--txt2)}
.dk .dn u b.high{color:var(--amber)}
.dk .dn u b.full{color:var(--red)}
.dk .dm{color:var(--txt2);font-size:11.5px;white-space:nowrap}
.dk .dm.bad{color:var(--red)}.dk .dm.warn{color:var(--amber)}
.dk .dt{font-weight:700;color:var(--cyan)}
.dk .dt.warm{color:var(--amber)}.dk .dt.hot{color:var(--red)}
.dk .dv{font-weight:700;font-size:8.5px;letter-spacing:1px;padding:4px 7px;border-radius:4px;white-space:nowrap}
.dk .dv.up{color:var(--mint);background:rgba(93,242,160,.1);border:1px solid rgba(93,242,160,.3)}
.dk .dv.deg{color:var(--amber);background:rgba(245,181,68,.1);border:1px solid rgba(245,181,68,.35)}
.dk .dv.down{color:var(--red);background:rgba(255,107,107,.1);border:1px solid rgba(255,107,107,.3)}
.dk .dv.na{color:var(--dim);background:rgba(143,169,162,.08);border:1px solid var(--line2)}
.sv .d.na,.dk .d.na{color:var(--dim)}
.hd{display:flex;align-items:baseline;gap:9px}
.hd .r{margin-left:auto;font-weight:600;font-size:9.5px;letter-spacing:1.4px;color:var(--dim)}
.hd .r.old{color:var(--amber)}
@media(max-width:820px){.dk{grid-template-columns:12px minmax(0,1fr) auto auto;gap:8px}
  .dk .dm.hidem{display:none}}

/* ---------- services ---------- */
.strip{display:flex;align-items:center;gap:20px;flex-wrap:wrap;padding:12px 16px;margin-bottom:11px;
  background:var(--card);border:1px solid var(--line);border-radius:11px;font-size:12.5px}
.strip .k{display:flex;align-items:center;gap:7px}
.strip .k i{width:8px;height:8px;border-radius:50%;display:inline-block}
.strip .k b{font-weight:700;font-size:15px}
.strip .meta{margin-left:auto;color:var(--txt2)}
.strip .meta b{color:var(--txt);font-weight:700}
.cats{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(310px,100%),1fr));gap:11px;align-items:start}
/* SERVICIOS: cards differ wildly in height (1 service vs 8), and a grid row is as
   tall as its tallest cell - so short cards left large holes. Columns pack them
   tightly instead. Scoped, because .cats is also the SEGURIDAD container where a
   card uses grid-column:1/-1, which is meaningless outside a grid. */
.cats.flow{display:flex;align-items:flex-start;gap:11px}
.cats.flow>.ncol{flex:1 1 0;min-width:0;display:flex;flex-direction:column;gap:11px}
.clgd{display:flex;flex-wrap:wrap;gap:12px;padding:5px 10px 8px}
.clg{display:flex;align-items:center;gap:5px;font:600 10px/1 var(--mono);
  letter-spacing:1px;color:var(--txt2)}
.clg i{width:8px;height:8px;border-radius:2px;flex:none}
.prows{columns:2 320px;column-gap:26px;padding-top:4px}
.prow{break-inside:avoid}
.sv .act{border:1px solid var(--line2);background:transparent;color:var(--dim);
  border-radius:5px;font:600 10.5px/1 var(--mono);padding:3px 6px;cursor:pointer;
  flex:none}
.sv .act:hover{color:var(--mint);border-color:var(--mint2)}
.logsv{max-height:58vh;max-width:min(78vw,860px);overflow:auto;text-align:left;
  background:var(--bg);border:1px solid var(--line);border-radius:8px;
  padding:10px 12px;font:400 10.5px/1.5 var(--mono);color:var(--txt2);
  white-space:pre-wrap;word-break:break-all;margin:10px 0 2px}
.cat{background:var(--card);border:1px solid var(--line);border-left:3px solid var(--cc,var(--mint));
  border-radius:11px;padding:13px 15px 11px}
.cath{display:flex;align-items:center;gap:9px;margin-bottom:10px}
.cath i{width:9px;height:9px;border-radius:2px;background:var(--cc,var(--mint));flex:none}
.cath .n{font-weight:700;font-size:11.5px;letter-spacing:2px;text-transform:uppercase}
.cath .c{margin-left:auto;font-weight:600;font-size:11px;color:var(--dim)}
.sv{display:flex;align-items:center;gap:9px;padding:4px 0}
.sv .d{flex:none}
.sv .n{flex:1;min-width:0}
.sv .m,.sv .upd{flex:none}
.upd{font:700 8.5px/1 var(--mono);letter-spacing:.9px;text-transform:uppercase;
  padding:5px 8px;border-radius:5px;cursor:pointer;white-space:nowrap;
  border:1px solid rgba(245,181,68,.45);background:rgba(245,181,68,.1);color:var(--amber)}
.upd:hover:not(:disabled){background:rgba(245,181,68,.22)}
.upd:disabled{opacity:.5;cursor:wait}
dialog{border:1px solid var(--line2);border-radius:14px;background:var(--card);color:var(--txt);
  padding:0;margin:auto;max-width:min(430px,92vw);font-family:var(--mono)}
dialog.wide{max-width:min(900px,94vw)}
dialog.wide .logsv{margin:10px 16px 2px}
.libwrap{max-height:56vh;overflow-y:auto;margin:6px 16px 10px}
table.libt{width:100%;border-collapse:collapse;font-size:.92rem}
table.libt td{padding:7px 6px;border-bottom:1px solid var(--line2)}
table.libt td.r{text-align:right;white-space:nowrap;color:var(--dim);padding-right:12px}
dialog::backdrop{background:rgba(3,6,7,.76);-webkit-backdrop-filter:blur(3px);backdrop-filter:blur(3px)}
.dh{font:700 11px var(--mono);letter-spacing:1.8px;text-transform:uppercase;color:var(--dim);padding:17px 19px 0}
.db{padding:10px 19px 4px;font:400 13.5px/1.55 var(--mono);color:var(--txt2)}
.df{display:flex;justify-content:flex-end;gap:8px;padding:16px 19px 18px}
.bt{border:1px solid var(--line2);border-radius:8px;background:transparent;color:var(--txt2);
  font:600 11px var(--mono);letter-spacing:.8px;padding:9px 15px;cursor:pointer}
.bt:hover{color:var(--txt);border-color:var(--dim)}
.bt.go{background:var(--amber);border-color:var(--amber);color:#140c00}
.toast{position:fixed;right:16px;bottom:74px;z-index:60;max-width:min(390px,92vw);
  background:var(--card2);border:1px solid var(--line2);border-left:3px solid var(--mint);
  border-radius:10px;padding:12px 14px;font:500 12.5px/1.5 var(--mono);color:var(--txt)}
.toast.bad{border-left-color:var(--red)}
.toast.work::before{content:"";display:inline-block;width:7px;height:7px;border-radius:50%;
  background:var(--mint);margin-right:9px;animation:blink 1s ease-in-out infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
.toast[hidden]{display:none}
.sv .d{font-size:9px;line-height:1;color:var(--mint)}
.sv .d.degraded{color:var(--amber)}.sv .d.down{color:var(--red)}
.sv .n{font-size:13.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sv .n a:hover{color:var(--mint)}
.sv .m{font-size:12px;color:var(--txt2)}

/* ---------- seguridad ---------- */
.tb.secb.bad{color:var(--red);border-color:var(--red)}
.tb.secb.warn{color:var(--amber);border-color:var(--amber)}
.seccard .c.up{color:var(--mint);font-weight:700}
.seccard .c.degraded{color:var(--amber);font-weight:700}
.seccard .c.down{color:var(--red);font-weight:700}
.secline{padding:6px 0;border-top:1px solid var(--line);font-size:12.5px;
  color:var(--txt2);line-height:1.55;overflow-wrap:anywhere}
.secline b{color:var(--txt);font-weight:700}
.secline.bad{color:var(--red)}
.secline .k{color:var(--dim);font-weight:600;letter-spacing:1.2px;font-size:9.5px;
  text-transform:uppercase;margin-right:7px}
.stat .v.warnv{color:var(--amber)}
.scanbars{width:100%;height:46px;display:block;margin-top:4px}
.prow{display:grid;grid-template-columns:52px minmax(0,1fr) auto;gap:10px;
  align-items:center;padding:5px 0;border-top:1px solid var(--line);font-size:12.5px}
.prow .pp{font-weight:700;color:var(--txt)}
.prow .pn{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--txt2)}
.prow .pb{font-weight:700;font-size:8.5px;letter-spacing:1px;padding:3px 7px;
  border-radius:4px;border:1px solid;white-space:nowrap}
.pb.open{color:var(--amber);border-color:rgba(245,181,68,.4);background:rgba(245,181,68,.08)}
.pb.lan{color:var(--cyan);border-color:rgba(103,232,249,.35);background:rgba(103,232,249,.07)}
.pb.blocked,.pb.policy{color:var(--dim);border-color:var(--line2)}
.scanrun{color:var(--amber)}
.scanrun::before{content:"";display:inline-block;width:7px;height:7px;border-radius:50%;
  background:var(--amber);margin-right:8px;animation:blink 1s ease-in-out infinite}

/* ---------- consumption ---------- */
.ctop{display:grid;grid-template-columns:minmax(210px,1fr) 2.3fr;gap:10px;margin-bottom:10px}
.hero{display:flex;flex-direction:column;justify-content:center;text-align:center;
  background:var(--card);border:1px solid var(--line);border-radius:11px;padding:20px 16px}
.hero .v{font-weight:700;font-size:66px;line-height:.95;letter-spacing:-3px;color:var(--mint);
  text-shadow:0 0 26px rgba(93,242,160,.3)}
.hero .v u{text-decoration:none;font-size:22px;letter-spacing:0;margin-left:3px}
.hero .k{margin-top:12px;font-weight:600;font-size:9.5px;letter-spacing:2px;color:var(--dim);line-height:1.7}
.hero .r{margin-top:7px;font-size:11px;color:var(--dim)}
.chart{background:var(--card);border:1px solid var(--line);border-radius:11px;padding:8px;
  display:grid;place-items:stretch;min-height:190px}
.chart svg{width:100%;height:100%;display:block}
.costs{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:10px}
.cost{background:var(--card);border:1px solid var(--line);border-radius:11px;padding:13px 16px}
.cost.on{border-left:3px solid var(--mint)}
.cost .v{font-weight:700;font-size:29px;letter-spacing:-1px;color:var(--mint);margin-top:6px}
.cost .sub{margin-top:6px;font-size:10.5px;color:var(--dim);line-height:1.5}
.stats{display:grid;grid-template-columns:repeat(4,1fr);background:var(--card);
  border:1px solid var(--line);border-radius:11px;overflow:hidden}
.stat{padding:14px 10px;text-align:center;border-left:1px solid var(--line)}
.stat:first-child{border-left:0}
.stat .v{font-weight:700;font-size:25px;color:var(--cyan);line-height:1.1}
.stat .v.mint{color:var(--mint)}
.stat .k{margin-top:6px;font-weight:600;font-size:9px;letter-spacing:1.6px;color:var(--dim)}
.note{margin-top:10px;padding:11px 14px;border-radius:9px;border:1px solid rgba(245,181,68,.28);
  background:rgba(245,181,68,.06);font-size:11.5px;line-height:1.6;color:var(--txt2)}
.note b{color:var(--amber);font-weight:700;letter-spacing:1.4px;margin-right:7px}

/* ---------- edge arrows ---------- */
.nav{position:fixed;top:50%;transform:translateY(-50%);z-index:30;
  width:42px;height:76px;display:grid;place-items:center;border-radius:10px;
  background:rgba(10,16,18,.72);border:1px solid var(--line2);color:var(--dim);
  font:400 20px/1 var(--mono);cursor:pointer;
  -webkit-backdrop-filter:blur(7px);backdrop-filter:blur(7px);
  transition:color .18s,border-color .18s,opacity .18s}
.nav:hover:not([disabled]){color:var(--mint);border-color:var(--mint2);
  box-shadow:0 0 18px rgba(93,242,160,.16)}
.nav.l{left:9px}.nav.r{right:9px}
.nav[disabled]{opacity:.2;cursor:default}
@media(max-width:820px){.nav{width:32px;height:58px;font-size:16px}
  .nav.l{left:3px}.nav.r{right:3px}}

/* ---------- footer ---------- */
.foot{position:sticky;bottom:0;display:flex;align-items:center;gap:12px;
  padding:12px 20px max(14px,env(safe-area-inset-bottom));border-top:1px solid var(--line);
  background:linear-gradient(0deg,var(--bg) 62%,rgba(5,9,10,.86))}
.dots{position:absolute;left:50%;transform:translateX(-50%);display:flex;gap:8px}
.dot{width:7px;height:7px;padding:0;border-radius:50%;border:0;background:var(--line2);
  cursor:pointer;transition:background .2s,box-shadow .2s,transform .2s}
.dot.on{background:var(--mint);box-shadow:var(--glow);transform:scale(1.25)}
.fmeta{margin-left:auto;font-size:10.5px;color:var(--dim);letter-spacing:.6px}
.fmeta .err{color:var(--red)}
@media(max-width:820px){
  .gauges{grid-template-columns:repeat(3,1fr);gap:6px}
  .gw svg,.gw.big svg{max-width:106px}
  .gw .gv,.gw.big .gv{font-size:18px;letter-spacing:0}
  .gw .gl{margin-top:3px;font-size:7.5px;letter-spacing:.8px;line-height:1.5}
  .row3,.costs,.ctop{grid-template-columns:1fr}
  .stats{grid-template-columns:repeat(2,1fr)}
  .stat:nth-child(3){border-left:0}
  .clk{font-size:15px}.brand{font-size:11.5px;letter-spacing:1.6px}
}
/* Phones: the top bar's one-row minimum is ~570px, which made mobile browsers
   zoom the whole board out to fit it. Let it wrap: brand+clock on the first
   row, screen label + action buttons on the second. */
@media(max-width:640px){
  .top{flex-wrap:wrap;row-gap:7px;gap:10px;padding:10px 14px 9px}
  .top .sp{display:none}
  .bd{order:0}
  .brand{order:1;margin-right:auto;font-size:10.5px;letter-spacing:1.2px}
  .clk{order:2;font-size:13px}
  .scr{order:3;margin-right:auto}
  .scr .nm{font-size:11.5px;letter-spacing:2px}
  #secbadge{order:4}#lang{order:5}#logout{order:6}
}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}
  .screens{scroll-behavior:auto}}
</style></head><body>
<div class="app">
  <header class="top">
    <span class="bd"></span>
    <span class="brand" id="logo"></span>
    <div class="sp"></div>
    <div class="scr"><span class="no" id="scn">1</span><span class="nm" id="scl">VITALES</span></div>
    <div class="sp"></div>
    <button class="tb secb" id="secbadge" hidden></button>
    <button class="tb" id="lang" title="ES / EN">ES</button>
    <button class="tb" id="logout">SALIR</button>
    <span class="clk" id="clock"></span>
  </header>

  <div class="screens" id="screens">
    <section class="screen" id="sc0"><div id="v-body"></div></section>
    <section class="screen" id="sc1"><div id="s-body"></div></section>
    <section class="screen" id="sc2"><div id="p-body"></div></section>
    <section class="screen" id="sc3"><div id="x-body"></div></section>
    <section class="screen" id="sc4"><div id="n-body"></div><div class="nsleg" id="n-legend"></div></section>
  </div>

  <button class="nav l" id="prev">&lsaquo;</button>
  <button class="nav r" id="next">&rsaquo;</button>

  <footer class="foot">
    <span class="lbl" id="fleft">■ AUTO</span>
    <nav class="dots" id="dots"></nav>
    <span class="fmeta" id="foot">conectando…</span>
  </footer>
</div>
<div class="sr-only" id="live" role="status" aria-live="polite"></div>
<div class="toast" id="toast" role="status" hidden></div>
<dialog id="dlg" aria-modal="true"></dialog>
<script>
const BRAND="__TITLE__";
const $=id=>document.getElementById(id);
$("logo").innerHTML=BRAND.replace(/\/\//,"<i>//</i>");
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const short=n=>String(n).split("·")[0].trim();
const SPINE=["#5df2a0","#67e8f9","#a78bfa","#f5b544","#f472b6","#2dd4bf"];
const GLY={up:"●",degraded:"▲",down:"✕",na:"○"};
const WORD=()=>({up:t("wUp"),degraded:t("wDeg"),down:t("wDown")});
const nword=(n,k)=>t(n===1?k+"1":k);   // singular/plural status words
// ---- i18n. Every visible string lives here; nothing is hardcoded in a view.
// Disk verdicts and timestamps are worded here too (the server sends the raw
// verdict key and an age in seconds) so switching language re-labels them.
const T={
 es:{sc:["VITALES","SERVICIOS","CONSUMO","SEGURIDAD","RED"],
  cpu:"CPU",mem:"MEMORIA",temp:"TEMPERATURA",net:"RED",thermal:"TÉRMICA",
  svcOnline:"SERVICIOS ONLINE",system:"SISTEMA",disks:"DISCOS",fleet:"FLOTA",
  uptime:"UPTIME",load:"CARGA",memf:"MEM",disk:"DISCO",containers:"CONTENEDORES",
  power:"CONSUMO",cats:"CATEGORÍAS",ram:"RAM",services:"SERVICIOS",role:"ROL",
  online:"online",degraded:"degradados",degraded1:"degradado",
  down:"caídos",down1:"caído",inCats:"en",
  ramNote:"RAM (solo servicios)",offline:"sin conexión",allOk:"todo operativo",
  now:"CONSUMO AHORA",est:"(EST.)",meas:"(MEDIDO)",
  today:"HOY · MEDIDO",month:"ESTE MES · MEDIDO",year:"AL AÑO · PROYECCIÓN",
  noRecord:"sin registro",proj:"proy.",atRate:"al ritmo actual",
  soFar:"kWh medidos hasta ahora",kwhMonth:"KWH / MES",wSvc:"W / SERVICIO",
  realloc:"realoc",err:"err",link:"enlace",logout:"SALIR",
  updated:"ACTUALIZADO",cache:"CACHÉ · ACTUALIZANDO…",noconn:"✕ SIN CONEXIÓN",
  conn:"conectando…",
  talkers:"POR CONTENEDOR",noneMoving:"ningún contenedor medible mueve datos ahora",
  catmap:{},
  hostNet:n=>`${n} en red del host — Docker no puede medirlos por separado; su tráfico ya está en el total de arriba`,
  updBtn:"actualizar",updWork:"actualizando…",updTitle:"actualizar servicio",updOk:"actualizar",cancel:"cancelar",
  updAsk:n=>`¿Actualizar ${n}? Se descargará la imagen nueva y el servicio se reiniciará.`,
  updDone:n=>`${n} actualizado`,updLost:"se perdió el seguimiento",
  srvBusy:"ya hay una actualización en curso",srvScanBusy:"ya hay un escaneo en curso",
  srvUnknown:"servicio desconocido",srvBad:"petición inválida",
  srvNoCont:"ningún contenedor coincide",srvNoCompose:"el contenedor no está gestionado por compose",
  srvFail:"no se pudo iniciar",srvNoRemote:"no disponible en máquinas remotas",
  rstBtn:"reiniciar",rstTitle:"reiniciar servicio",rstOk:"reiniciar",
  rstAsk:n=>`¿Reiniciar ${n}? El servicio se detendrá y arrancará de nuevo.`,
  rstDone:n=>`${n} reiniciado`,logsBtn:"logs",close:"cerrar",
  srvBusyOp:"ya hay una operación en curso sobre este servicio",
  libBtn:"biblioteca",libRadarr:"películas",libSonarr:"series",
  libLoading:"cargando…",libEmpty:"nada aquí",libDelete:"eliminar",
  libDelTitle:"eliminar definitivamente",
  libDelAsk:n=>`¿Eliminar "${n}" y sus archivos? Esto no se puede deshacer.`,
  libDelOk:"eliminar",libDeleted:n=>`liberado ${n}`,
  libTorrentCleaned:"torrent también eliminado",
  libLocalOnly:"solo disponible en la red local",
  st:{preparing:"preparando…",locating:"localizando contenedor…",pulling:"descargando imagen…",
      pulled:"imagen descargada",recreating:"recreando contenedor…",starting:"arrancando servicio…",
      stopping:"deteniendo el anterior…",uptodate:"ya estaba al día",done:"listo",failed:"error"},
  prev:"Pantalla anterior",next:"Pantalla siguiente",goto:"Ir a",
  waiting:"esperando muestras…",noMachines:"Ninguna máquina accesible.",
  notCounted:"Sin contabilizar",wUp:"operativo",wDeg:"degradado",wDown:"caído",
  estHead:"ESTIMADO",
  estBody:"Placa, discos y fuente están modelados a partir del hardware y del uso de CPU — no medidos. ",
  estRapl:"El término de CPU sí es una lectura real del contador RAPL. ",
  estSince:"Los totales MEDIDOS son vatios-hora acumulados desde ",
  estTariff:"Tarifa",
  estFixed:"Los cargos fijos de la factura son del hogar y no se incluyen.",
  tariffAutoHead:"TARIFA SIN CONFIRMAR",
  tariffAutoBody:"detectada automáticamente por la zona horaria del sistema, no por tu factura real — corrígela con la clave \"power\" en config.json.",
  routeDown:"proceso activo · ruta pública caída",
  approxCpu:"CPU aproximada — derivada de la carga y el nº de núcleos, no medida",
  vExcellent:"EXCELENTE",vGood:"BIEN",vWatch:"VIGILAR",vUnknown:"SIN DATOS",
  nsTitle:"Superficie de red",nsNet:"INTERNET",nsPublic:"red pública",
  nsTunnel:"TÚNEL CLOUDFLARE",nsMesh:"MALLA TAILSCALE",nsMeshSub:"WireGuard cifrado",
  nsHostsN:n=>`${n} host públicos`,nsRulesN:n=>`${n} reglas`,nsDropN:n=>`${n} descartes`,
  nsBypass:"sale hacia fuera · no pasa por nftables",
  nsLOpen:"abiertos",nsLanOnly:"solo LAN",nsBlocked:"bloqueados",nsLoop:"loopback",
  nsDocker:"docker",nsSvc:"servicios",nsCtn:"contenedores",
  nsOwnPath:"salida propia a internet",nsViaMesh:"solo por la malla",
  nsNoHub:"ninguna máquina local",nsDown:"bajada",nsUp:"subida",nsPorts:"puertos",
  vReplaceSoon:"CAMBIAR PRONTO",vReplaceNow:"CAMBIAR YA",
  io:"E/S",avg:"media",cover:"cobertura",ofMonth:"del mes",
  noData:"sin dato",screens:"Pantallas",
  chartAria:(a,b)=>`Consumo reciente: de ${a} a ${b} vatios`,
  secBadge:"⚠ SEGURIDAD",secFw:"CORTAFUEGOS",secAv:"ANTIVIRUS · FIRMAS",
  secScan:"ÚLTIMO ESCANEO",secPatch:"PARCHES AUTOMÁTICOS",secChecked:"comprobado",
  fwRules:n=>`tabla noxafw en el kernel · ${n} reglas`,
  fwIndirect:"servicio activo · verificación indirecta (falta la regla sudo)",
  fwNone:"tabla noxafw AUSENTE del kernel",fwUnit:"servicio nftables",
  avSig:a=>`firmas ${a}`,avNoSig:"sin base de firmas todavía",
  avOff:"ClamAV no instalado — ejecuta security_setup.sh",
  avFresh:"actualizador freshclam",
  scanClean:n=>`limpio · ${n==null?"—":n} archivos`,
  scanHits:n=>`${n} ARCHIVO${n===1?"":"S"} INFECTADO${n===1?"":"S"}`,
  scanNever:"aún sin escaneo",scanDeep:"profundo",scanDaily:"diario",
  scanDur:s=>`${s==null?"—":Math.round(s/60)} min`,
  patchOn:"parches de seguridad de Debian automáticos",
  patchOff:"unattended-upgrades no instalado — ejecuta security_setup.sh",
  patchBroken:"instalado pero no programado",
  patchRun:a=>`última pasada ${a}`,patchNoRun:"aún sin pasada registrada",
  patchPend:(t,s)=>t?`${t} paquete${t===1?"":"s"} pendiente${t===1?"":"s"} · ${s} de seguridad`:"nada pendiente",
  fwHero:"BLOQUEADO POR EL CORTAFUEGOS",fwSince:"desde la última carga",
  fwProbes:n=>`${n} sondeo${n===1?"":"s"} a puertos de administración`,
  fwPolicy:p=>`política por defecto: ${p==="drop"?"descartar todo lo no permitido":p||"—"}`,
  fwChart:"BLOQUEOS RECIENTES · POR MINUTO",
  fwChains:(c,r)=>`${c} cadena${c===1?"":"s"} · ${r} reglas activas`,
  fwSet:(n,c)=>`${esc(n)} · ${c} puertos`,fwSets:"listas de puertos",
  stRules:"REGLAS FW",stSigs:"FIRMAS AV",stScanned:"ARCH. ESCANEADOS",
  stPend:"PARCHES PEND.",stPorts:"PUERTOS PÚBLICOS",
  avDb:(v,n)=>`base daily v${v??"—"} · ${n==null?"—":(n/1e6).toFixed(1)+" M"} firmas`,
  avEngine:"motor",avSchedLbl:"escaneos programados",
  avSched:"lun–sáb 04:15 · dom 05:00 profundo",
  scanHist:"historial",scanKnown:n=>`contra ${(n/1e6).toFixed(1)} M firmas`,
  scanNow:"escanear ahora",scanDeepBtn:"escaneo profundo",
  scanStarted:"escaneo iniciado — la tarjeta lo mostrará en curso",
  scanRunning:m=>`escaneo en curso · ${m==="deep"?"profundo":"diario"}…`,
  netSurf:"SUPERFICIE DE RED",nsOpen:"ABIERTO",nsLan:"SOLO LAN",nsBlock:"BLOQUEADO",
  nsMore:n=>`+ ${n} puertos más a la escucha, bloqueados por la política por defecto`,
  nsCounts:(l,ta,d)=>`${l} sólo en localhost · ${ta} sólo tailnet · ${d} en red docker`,
  secNote:"Comprobado cada 60 s en esta máquina: la tabla nftables se lee del propio kernel (sudo de sólo lectura), firmas y escaneos de ClamAV de sus temporizadores systemd y los parches del temporizador de APT. Todo son unidades systemd habilitadas — sobreviven a reinicios; si algo deja de funcionar, esta pantalla y el aviso ⚠ de la cabecera lo marcan.",
  day:"día",days:"días",ago:x=>"hace "+x,loc:"es-ES"},
 en:{sc:["VITALS","SERVICES","POWER","SECURITY","NETWORK"],
  cpu:"CPU",mem:"MEMORY",temp:"TEMPERATURE",net:"NETWORK",thermal:"THERMAL",
  svcOnline:"SERVICES ONLINE",system:"SYSTEM",disks:"DISKS",fleet:"FLEET",
  uptime:"UPTIME",load:"LOAD",memf:"MEM",disk:"DISK",containers:"CONTAINERS",
  power:"POWER",cats:"CATEGORIES",ram:"RAM",services:"SERVICES",role:"ROLE",
  online:"online",degraded:"degraded",degraded1:"degraded",
  down:"down",down1:"down",inCats:"across",
  ramNote:"RAM (services only)",offline:"offline",allOk:"all operational",
  now:"DRAW NOW",est:"(EST.)",meas:"(MEASURED)",
  today:"TODAY · MEASURED",month:"THIS MONTH · MEASURED",year:"PER YEAR · PROJECTED",
  noRecord:"no record yet",proj:"proj.",atRate:"at the current rate",
  soFar:"kWh measured so far",kwhMonth:"KWH / MONTH",wSvc:"W / SERVICE",
  realloc:"realloc",err:"err",link:"link",logout:"SIGN OUT",
  updated:"UPDATED",cache:"CACHED · REFRESHING…",noconn:"✕ NO CONNECTION",
  conn:"connecting…",
  talkers:"BY CONTAINER",noneMoving:"no measurable container is moving data right now",
  catmap:{"AUTOMATIZACIÓN":"AUTOMATION","IA & AGENTES":"AI & AGENTS","APPS":"APPS",
    "CONTENIDO":"MEDIA","SITIOS PÚBLICOS":"PUBLIC SITES","INFRA & DATOS":"INFRA & DATA",
    "OTROS":"OTHER"},
  hostNet:n=>`${n} on host networking — Docker cannot measure them separately; their traffic is already in the total above`,
  updBtn:"update",updWork:"updating…",updTitle:"update service",updOk:"update",cancel:"cancel",
  updAsk:n=>`Update ${n}? The new image will be pulled and the service restarted.`,
  updDone:n=>`${n} updated`,updLost:"lost track of the job",
  srvBusy:"an update is already running",srvScanBusy:"a scan is already running",
  srvUnknown:"unknown service",srvBad:"bad request",
  srvNoCont:"no containers matched",srvNoCompose:"container is not compose-managed",
  srvFail:"could not start",srvNoRemote:"not available on remote machines",
  rstBtn:"restart",rstTitle:"restart service",rstOk:"restart",
  rstAsk:n=>`Restart ${n}? The service will stop and start again.`,
  rstDone:n=>`${n} restarted`,logsBtn:"logs",close:"close",
  srvBusyOp:"an operation is already running on this service",
  libBtn:"library",libRadarr:"movies",libSonarr:"shows",
  libLoading:"loading…",libEmpty:"nothing here",libDelete:"delete",
  libDelTitle:"delete permanently",
  libDelAsk:n=>`Delete "${n}" and its files? This cannot be undone.`,
  libDelOk:"delete",libDeleted:n=>`freed ${n}`,
  libTorrentCleaned:"torrent cleaned up too",
  libLocalOnly:"only available on the local network",
  st:{preparing:"preparing…",locating:"locating container…",pulling:"pulling image…",
      pulled:"image pulled",recreating:"recreating container…",starting:"starting service…",
      stopping:"stopping the old one…",uptodate:"already up to date",done:"done",failed:"error"},
  prev:"Previous screen",next:"Next screen",goto:"Go to",
  waiting:"waiting for samples…",noMachines:"No machine reachable.",
  notCounted:"Not counted",wUp:"operational",wDeg:"degraded",wDown:"down",
  estHead:"ESTIMATED",
  estBody:"Board, disks and PSU are modelled from the hardware and CPU load — not measured. ",
  estRapl:"The CPU term is a real reading from the RAPL counter. ",
  estSince:"MEASURED totals are watt-hours accumulated since ",
  estTariff:"Tariff",
  estFixed:"The bill's fixed charges belong to the household and are excluded.",
  tariffAutoHead:"UNCONFIRMED TARIFF",
  tariffAutoBody:"auto-detected from the system's timezone, not your real bill — correct it with a \"power\" key in config.json.",
  routeDown:"process healthy · public route down",
  approxCpu:"CPU approximate — derived from load average and core count, not measured",
  vExcellent:"EXCELLENT",vGood:"GOOD",vWatch:"WATCH",vUnknown:"NO DATA",
  nsTitle:"Network surface",nsNet:"INTERNET",nsPublic:"public network",
  nsTunnel:"CLOUDFLARE TUNNEL",nsMesh:"TAILSCALE MESH",nsMeshSub:"encrypted WireGuard",
  nsHostsN:n=>`${n} public hosts`,nsRulesN:n=>`${n} rules`,nsDropN:n=>`${n} drops`,
  nsBypass:"outbound-initiated · does not traverse nftables",
  nsLOpen:"open",nsLanOnly:"LAN only",nsBlocked:"blocked",nsLoop:"loopback",
  nsDocker:"docker",nsSvc:"services",nsCtn:"containers",
  nsOwnPath:"own internet egress",nsViaMesh:"via mesh only",
  nsNoHub:"no local machine",nsDown:"down",nsUp:"up",nsPorts:"ports",
  vReplaceSoon:"REPLACE SOON",vReplaceNow:"REPLACE NOW",
  io:"I/O",avg:"avg",cover:"coverage",ofMonth:"of the month",
  noData:"no data",screens:"Screens",
  chartAria:(a,b)=>`Recent draw: ${a} to ${b} watts`,
  secBadge:"⚠ SECURITY",secFw:"FIREWALL",secAv:"ANTIVIRUS · SIGNATURES",
  secScan:"LAST SCAN",secPatch:"AUTO-PATCHING",secChecked:"checked",
  fwRules:n=>`noxafw table in the kernel · ${n} rules`,
  fwIndirect:"unit active · indirect verification (sudo rule missing)",
  fwNone:"noxafw table MISSING from the kernel",fwUnit:"nftables unit",
  avSig:a=>`signatures ${a}`,avNoSig:"no signature database yet",
  avOff:"ClamAV not installed — run security_setup.sh",
  avFresh:"freshclam updater",
  scanClean:n=>`clean · ${n==null?"—":n} files`,
  scanHits:n=>`${n} INFECTED FILE${n===1?"":"S"}`,
  scanNever:"no scan yet",scanDeep:"deep",scanDaily:"daily",
  scanDur:s=>`${s==null?"—":Math.round(s/60)} min`,
  patchOn:"Debian security patches applied automatically",
  patchOff:"unattended-upgrades not installed — run security_setup.sh",
  patchBroken:"installed but not scheduled",
  patchRun:a=>`last run ${a}`,patchNoRun:"no run recorded yet",
  patchPend:(t,s)=>t?`${t} package${t===1?"":"s"} pending · ${s} security`:"nothing pending",
  fwHero:"BLOCKED BY THE FIREWALL",fwSince:"since last load",
  fwProbes:n=>`${n} probe${n===1?"":"s"} at admin ports`,
  fwPolicy:p=>`default policy: ${p==="drop"?"drop everything not allowed":p||"—"}`,
  fwChart:"RECENT BLOCKS · PER MINUTE",
  fwChains:(c,r)=>`${c} chain${c===1?"":"s"} · ${r} active rules`,
  fwSet:(n,c)=>`${esc(n)} · ${c} ports`,fwSets:"port sets",
  stRules:"FW RULES",stSigs:"AV SIGNATURES",stScanned:"FILES SCANNED",
  stPend:"PENDING PATCHES",stPorts:"PUBLIC PORTS",
  avDb:(v,n)=>`daily db v${v??"—"} · ${n==null?"—":(n/1e6).toFixed(1)+" M"} signatures`,
  avEngine:"engine",avSchedLbl:"scheduled scans",
  avSched:"Mon–Sat 04:15 · Sun 05:00 deep",
  scanHist:"history",scanKnown:n=>`against ${(n/1e6).toFixed(1)} M signatures`,
  scanNow:"scan now",scanDeepBtn:"deep scan",
  scanStarted:"scan started — the card will show it running",
  scanRunning:m=>`scan running · ${m==="deep"?"deep":"daily"}…`,
  netSurf:"NETWORK SURFACE",nsOpen:"OPEN",nsLan:"LAN ONLY",nsBlock:"BLOCKED",
  nsMore:n=>`+ ${n} more listening ports, blocked by the default policy`,
  nsCounts:(l,ta,d)=>`${l} on localhost only · ${ta} tailnet only · ${d} on docker networks`,
  secNote:"Checked every 60 s on this box: the nftables table is read from the kernel itself (read-only sudo), ClamAV signatures and scans come from their systemd timers, and patching from APT's timer. Everything is an enabled systemd unit — it all survives reboots; if any piece stops working, this screen and the ⚠ badge in the header flag it.",
  day:"day",days:"days",ago:x=>x+" ago",loc:"en-GB"}};
function initLang(){
  try{const v=localStorage.netopsLang;if(v==="en"||v==="es")return v;}catch(e){}
  return (navigator.language||"es").toLowerCase().startsWith("en")?"en":"es";}
let L=initLang();
const t=k=>{const v=T[L][k];return v===undefined?T.es[k]:v;};
const catName=n=>{const m=T[L].catmap;return (m&&m[n])||n;};
const VLABEL={excellent:"vExcellent",good:"vGood",watch:"vWatch",
  replace_soon:"vReplaceSoon",replace_now:"vReplaceNow",unknown:"vUnknown"};
const agoTxt=sec=>sec==null?"—":t("ago")(
  sec<90?Math.round(sec)+"s":sec<5400?Math.round(sec/60)+" min":
  sec<172800?Math.round(sec/3600)+" h":Math.round(sec/86400)+" d");
let LABELS=T[L].sc;
const VCOL={excellent:"up",good:"up",watch:"deg",replace_soon:"down",replace_now:"down",
  unknown:"na"};
const DOT={up:"up",deg:"degraded",down:"down",na:"na"};
let D=null;

// year and week cases must exist or `uptime -p` past 7 days renders "1week1h13m"
const upshort=u=>String(u||"—").replace(/(\d+)\s*y(ears?)?/,"$1y")
  .replace(/(\d+)\s*w(eeks?)?/,"$1w").replace(/(\d+)\s*d(ays?)?/,"$1d")
  .replace(/(\d+)\s*h(ours?)?/,"$1h").replace(/(\d+)\s*m(in(ute)?s?)?/,"$1m")
  .replace(/,/g,"").replace(/\s+/g,"");
let CUR="$";   // set from D.power.currency each time power() paints
const money=v=>v==null?"—":CUR+v.toLocaleString(t("loc"),{minimumFractionDigits:2,maximumFractionDigits:2});
// bytes/s -> 11.4M/s · 154K/s · 820B/s
const rate=b=>b==null?"—":b>=1e6?(b/1e6).toFixed(1)+"M/s":b>=1e3?(b/1e3).toFixed(0)+"K/s":b+"B/s";
const col=p=>p==null?"var(--dim)":p>=85?"var(--red)":p>=65?"var(--amber)":"var(--mint)";

// ---- arc gauge: full track, rounded partial arc from 12 o'clock ----
function gauge(pct,label,big,disp){
  const R=54,C=2*Math.PI*R,p=pct==null?0:Math.max(0,Math.min(100,pct));
  const c=col(pct);
  return`<div class="gw${big?" big":""}">`+
    `<svg viewBox="0 0 140 140" role="img" aria-label="${esc(label)} ${pct==null?t("noData"):p+"%"}">`+
    `<circle cx="70" cy="70" r="${R}" fill="none" stroke="#12201f" stroke-width="13"/>`+
    `<circle cx="70" cy="70" r="${R}" fill="none" stroke="${c}" stroke-width="13"
       stroke-linecap="round" stroke-dasharray="${C.toFixed(1)}"
       stroke-dashoffset="${(C*(1-p/100)).toFixed(1)}" transform="rotate(-90 70 70)"
       style="transition:stroke-dashoffset .6s ease"/></svg>`+
    `<div class="gt"><div class="gv" style="color:${c}">${pct==null?"—":(disp||p+"%")}</div>`+
    `<div class="gl">${esc(label).replace(" · ","<br>")}</div></div></div>`;}

// ---- line chart with gradient fill + dotted baseline ----
function chart(vals,series){
  const W=760,H=200,PAD=6;
  if(!vals||vals.length<2)
    return`<svg viewBox="0 0 ${W} ${H}"><text x="${W/2}" y="${H/2}" fill="#6f8c86"
      font-family="monospace" font-size="13" text-anchor="middle">${t("waiting")}</text></svg>`;
  const all=vals.concat(...(series||[]).map(s=>s.vals.filter(v=>v!=null)));
  const mn=Math.min(...all),mx=Math.max(...all),span=Math.max(1,mx-mn);
  // keep a flat line off the very edge: pad the range so noise is visible but not absurd
  const lo=mn-span*0.6,hi=mx+span*0.6,rng=hi-lo;
  const x=i=>PAD+i*(W-PAD*2)/(vals.length-1);
  const y=v=>PAD+(1-(v-lo)/rng)*(H-PAD*2);
  const pts=vals.map((v,i)=>`${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const area=`${PAD},${H-PAD} ${pts} ${(W-PAD).toFixed(1)},${H-PAD}`;
  const base=y(vals.reduce((a,b)=>a+b,0)/vals.length).toFixed(1);
  // with per-machine overlays the combined line steps back to a faint gray
  // guide so the machine lines carry the chart (own gradient id: both charts
  // coexist in the DOM and url(#id) resolves document-wide)
  const muted=!!(series&&series.length);
  // per-invocation gradient id: two charts coexist in the DOM and url(#id)
  // resolves document-wide — a shared id would cross-contaminate styles
  const mc=muted?"#8fa9a2":"#5df2a0",gid="gf"+(chart._i=(chart._i||0)+1);
  return`<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img"
      aria-label="${t("chartAria")(mn.toFixed(0),mx.toFixed(0))}">
    <defs><linearGradient id="${gid}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="${mc}" stop-opacity="${muted?".08":".30"}"/>
      <stop offset="100%" stop-color="${mc}" stop-opacity="0"/></linearGradient></defs>
    <line x1="${PAD}" y1="${base}" x2="${W-PAD}" y2="${base}" stroke="#2a4a42"
      stroke-width="1" stroke-dasharray="3 5"/>
    <polygon points="${area}" fill="url(#${gid})"/>
    <polyline points="${pts}" fill="none" stroke="${mc}" stroke-width="${muted?"1.5":"1.8"}"
      opacity="${muted?".38":"1"}" stroke-linejoin="round" stroke-linecap="round"/>${
    // per-machine overlays share the axis; a null (machine offline) breaks the
    // line into segments instead of drawing a fake dip to zero
    (series||[]).map(s=>{
      const segs=[];let cur=[];
      s.vals.slice(0,vals.length).forEach((v,i)=>{
        if(v==null){if(cur.length>1)segs.push(cur);cur=[];return;}
        cur.push(`${x(i).toFixed(1)},${y(v).toFixed(1)}`);});
      if(cur.length>1)segs.push(cur);
      return segs.map(sg=>`<polyline points="${sg.join(" ")}" fill="none" stroke="${s.color}"
        stroke-width="1.6" opacity=".95" stroke-linejoin="round" stroke-linecap="round"/>`).join("");
    }).join("")}</svg>`;}

// Hover/focus panel breaking the link total down per container. Docker can only
// measure containers with their own network namespace: one on network_mode=host
// shares the host's stack and reports a flat 0B/0B, which is indistinguishable
// from idle — so those are counted and named as UNMEASURABLE rather than shown
// as zero, which would read as "this container is doing nothing".
function netPop(m){
  const top=(m&&m.net_top)||[],blind=(m&&m.net_blind)||0;
  const rows=top.length
    ? top.map(x=>`<div class="nrow"><span class="nn">${esc(x.name)}</span>`+
        `<span class="nd">↓ ${rate(x.rx)}</span><span class="nu">↑ ${rate(x.tx)}</span></div>`).join("")
    : `<div class="nrow"><span class="nn" style="color:var(--dim)">${t("noneMoving")}</span></div>`;
  return`<div class="netpop"><div class="lbl">${t("talkers")}</div>${rows}`+
    (blind?`<div class="nblind">${esc(t("hostNet")(blind))}</div>`:"")+`</div>`;}

// ================= 1 · VITALES =================
function vitals(){
  const ms=D.machines||[],sp=D.speed||{},s=D.summary||{};
  const local=ms.find(m=>(m.host||{}).online&&(m.host||{}).temps&&m.host.temps.length)
           || ms.find(m=>(m.host||{}).online) || ms[0] || {};
  const h=local.host||{};
  let out="";
  out+=`<div class="gauges">`+
    gauge(h.cpu_pct,t("cpu")+(h.io_pct!=null?`  ·  ${t("io")} ${h.io_pct}%`:""))+
    gauge(h.ram_pct,t("mem")+" · "+esc(short(local.name||"")),true)+
    gauge(h.temp_c!=null?Math.min(100,Math.round(h.temp_c)):null,t("temp"),false,
          h.temp_c!=null?Math.round(h.temp_c)+"°":null)+`</div>`;

  // RED shows what the link is CARRYING right now, not how fast it could go.
  // The hourly capacity test is demoted to a sub-line.
  const rx=h.net_rx,tx=h.net_tx;
  out+=`<div class="row3">`+
    `<div class="card netcard" tabindex="0">${netPop(local)}<div class="lbl">${t("net")}</div>`+
    `<div class="big">${rate(rx==null||tx==null?null:rx+tx)}</div>`+
    `<div class="sub">↓ <b>${rate(rx)}</b>  ↑ <b>${rate(tx)}</b></div>`+
    `<div class="sub" style="color:var(--dim);font-size:10.5px">${t("link")} ${
      sp.down==null?"—":sp.down.toFixed(0)}/${sp.up==null?"—":sp.up.toFixed(0)} Mb/s${
      sp.age!=null?` · ${agoTxt(sp.age)}`:""}</div></div>`;

  const T=(h.temps||[]).slice(0,3);
  out+=`<div class="card"><div class="lbl">${t("thermal")}</div><div class="thermo">`+
    (T.length?T.map(t=>`<div class="t"><k>${esc(String(t.name).slice(0,9))}</k>`+
      `<v class="${t.c>=80?"hot":t.c>=65?"warm":""}">${t.c.toFixed(0)}°</v></div>`).join("")
     :`<div class="t"><k>CPU</k><v>${h.temp_c!=null?h.temp_c+"°":"—"}</v></div>`)+
    `</div></div>`;

  out+=`<div class="card sp3"><div class="lbl">${t("svcOnline")}</div>`+
    `<div class="big" style="color:var(--mint)">${s.online}<span style="color:var(--txt2)">/${s.count}</span></div>`+
    `<div class="sub">${(()=>{const b=[];
      if(s.degraded)b.push(`<b style="color:var(--amber)">${s.degraded}</b> ${nword(s.degraded,"degraded")}`);
      if(s.down)b.push(`<b style="color:var(--red)">${s.down}</b> ${nword(s.down,"down")}`);
      return b.length?b.join(" · "):t("allOk");})()}</div></div></div>`;

  // DISCOS — SMART health per drive: temperature and the two counters that
  // actually predict failure (reallocated sectors, total errors).
  (D.disks||[]).forEach(b=>{
    out+=`<div class="card" style="margin-top:10px"><div class="hd"><span class="lbl">${t("disks")} · ${esc(short(b.name))}</span>`+
      `<span class="r${b.stale?" old":""}">SMART · ${esc(b.age_s!=null?agoTxt(b.age_s):b.stamp)}</span></div>`+
      b.drives.map(d=>{
        const k=VCOL[d.verdict]||"deg";
        const tc=d.temp==null?"":d.temp>=55?"hot":d.temp>=45?"warm":"";
        const re=d.realloc,er=d.errors;
        const rcl=re?(re>=24?"bad":"warn"):"",ecl=er?(er>=24?"bad":"warn"):"";
        return`<div class="dk" title="${esc([d.model||"",d.note||""].filter(Boolean).join(" — "))}">`+
          `<span class="d ${DOT[k]}" role="img" aria-label="${esc(d.label||"")}">${GLY[DOT[k]]||"●"}</span>`+
          `<span class="dn">${esc(d.name)}<u>${esc(d.use||"")} · ${
             d.used?`${esc(d.used)} / ${esc(d.cap||d.size||"")}`:esc(d.size||"")}${
             d.used_pct!=null?` · <b class="${d.used_pct>=90?"full":d.used_pct>=75?"high":""}">${d.used_pct}%</b>`:""}</u></span>`+
          `<span class="dm ${rcl}">${re==null?"—":re} ${t("realloc")}</span>`+
          `<span class="dm ${ecl} hidem">${er==null?"—":er} ${t("err")}</span>`+
          `<span class="dt ${tc}">${d.temp==null?"—":d.temp+"°"}</span>`+
          `<span class="dv ${k}">${esc(t(VLABEL[d.verdict]||"vWatch"))}</span>`+
          `</div>`;}).join("")+`</div>`;});

  // SISTEMA — one line per reachable machine, fields separated like the readout
  const f=(k,v)=>`<b>${k}</b><s>${esc(v)}</s>`;
  const SEP=`<em>·</em>`;
  out+=`<div class="card" style="margin-top:10px"><div class="lbl">${t("system")}</div>`+
    ms.map(m=>{
      const o=m.host||{};
      if(!o.online)
        return`<div class="sysrow"><div class="sysline"><span class="h">${esc(short(m.name))}</span>`+
          `<s style="color:var(--red)">${t("offline")}</s>${SEP}<b>${t("role")}</b><s>${esc(m.role||"—")}</s></div></div>`;
      const ap=o.cpu_approx&&o.cpu_pct!=null;
      return`<div class="sysrow"${ap?` title="${esc(t("approxCpu"))}"`:""}><div class="sysline"><span class="h">${esc(short(m.name))}</span>`+
        [f(t("uptime"),upshort(o.uptime)),f(t("load"),o.load||"—"),
         f(t("cpu"),o.cpu_pct==null?"—":o.cpu_pct+"%"+(ap?"~":"")),
         f(t("io"),o.io_pct==null?"—":o.io_pct+"%"),f(t("memf"),o.ram||"—"),
         f(t("disk"),o.disk||"—"),
         f(t("power"),o.watts!=null?o.watts.toFixed(0)+"W":"—")].join(SEP)+`</div></div>`;
    }).join("")+
    `<div class="sysrow"><div class="sysline"><span class="h">${t("fleet")}</span>`+
    [f(t("services"),s.count),f(t("containers"),s.containers!=null?s.containers:"—"),
     f(t("cats"),s.cats||"—"),f(t("ram"),s.ram||"—")].join(SEP)+`</div></div></div>`;
  $("v-body").innerHTML=out;}

// ================= 2 · SERVICIOS =================
function services(){
  const s=D.summary||{},map={},order=[];
  (D.machines||[]).forEach(m=>{const sn=short(m.name);
    (m.categories||[]).forEach(c=>{
      if(!map[c.name]){map[c.name]={name:c.name,svcs:[]};order.push(c.name);}
      c.services.forEach(v=>map[c.name].svcs.push({...v,host:sn,mid:m.id}));});});
  let out=`<div class="strip">`+
    `<span class="k"><i style="background:var(--mint)"></i><b>${s.online}</b> ${t("online")}</span>`+
    `<span class="k"><i style="background:var(--amber)"></i><b>${s.degraded}</b> ${nword(s.degraded,"degraded")}</span>`+
    `<span class="k"><i style="background:var(--red)"></i><b>${s.down}</b> ${nword(s.down,"down")}</span>`+
    `<span class="meta"><b>${s.count}</b> ${t("inCats")} <b>${s.cats||order.length}</b> ${t("cats").toLowerCase()} · <b>${esc(s.ram||"—")}</b> ${t("ramNote")}</span>`+
    ((D.library_apps||[]).length?`<button class="bt libopen">${esc(t("libBtn"))}</button>`:"")+
    `</div>`;
  if(!order.length)out+=`<div class="card">${t("noMachines")}</div>`;
  // Balance the category cards across columns by ESTIMATED height. A plain grid
  // makes every row as tall as its tallest card, so a 1-service card left a large
  // hole beside an 8-service one; CSS multi-column packs tightly but balances
  // badly once break-inside:avoid forbids splitting a card. Filling the currently
  // shortest column is deterministic and needs no DOM measurement, so it costs
  // nothing on the 3s repaint.
  const CW=($("s-body")||{}).clientWidth||document.body.clientWidth||1200;
  const NCOL=Math.max(1,Math.min(4,Math.floor(CW/330)));
  const est=nm=>46+(map[nm].svcs.length*26);
  const ncols=Array.from({length:NCOL},()=>({h:0,names:[]}));
  order.forEach(n=>{let t=ncols[0];
    for(const c of ncols)if(c.h<t.h)t=c;
    t.names.push(n);t.h+=est(n);});
  const CARD=(n,i)=>{
    const cat=map[n],up=cat.svcs.filter(v=>v.status==="up").length,c=SPINE[i%SPINE.length];
    return`<div class="cat" style="--cc:${c}"><div class="cath"><i></i>`+
      `<span class="n">${esc(catName(n))}</span><span class="c">${up}/${cat.svcs.length}</span></div>`+
      cat.svcs.map(v=>{
        const nm=v.url?`<a href="https://${esc(v.url)}" target="_blank" rel="noopener">${esc(v.name)}</a>`:esc(v.name);
        const why=v.note==="route_down"?t("routeDown"):"";
        const busyU=updating.has((v.mid||"")+"/"+v.name);
        const upd=(v.update&&v.update.length)?
          `<button class="upd" data-mid="${esc(v.mid||"")}" data-svc="${esc(v.name)}"${
            busyU?" disabled":""}>${busyU?t("updWork"):t("updBtn")}</button>`:"";
        const act=v.act?
          `<button class="act lg" data-mid="${esc(v.mid||"")}" data-svc="${esc(v.name)}"
             title="${esc(t("logsBtn"))}" aria-label="${esc(t("logsBtn"))} ${esc(v.name)}">≡</button>`+
          `<button class="act rst" data-mid="${esc(v.mid||"")}" data-svc="${esc(v.name)}"
             title="${esc(t("rstBtn"))}" aria-label="${esc(t("rstBtn"))} ${esc(v.name)}">↻</button>`:"";
        return`<div class="sv${upd?" hasupd":""}"${why?` title="${esc(why)}"`:""}>`+
          `<span class="d ${v.status}" role="img" aria-label="${esc((WORD()[v.status]||v.status)+(why?" · "+why:""))}">${GLY[v.status]||"●"}</span>`+
          `<span class="n">${nm}</span>${act}${upd}<span class="m">${esc(v.ram||"—")}</span></div>`;}).join("")+
      `</div>`;};
  out+=`<div class="cats flow">`+ncols.map(c=>
    `<div class="ncol">`+c.names.map(n=>CARD(n,order.indexOf(n))).join("")+`</div>`
  ).join("")+`</div>`;
  $("s-body").innerHTML=out;}

// ---- media library cleanup (Sonarr/Radarr) ----
// Browsing works over the tunnel like everything else (plain authed GET);
// only POST /api/library/delete is local-network-only, enforced server-side
// (403 outside the LAN) — libLocalOnly below just turns that into a clear
// message instead of a generic error.
const fmtB=b=>b==null?"—":b>=1024**3?(b/1024**3).toFixed(1)+"G":
  b>=1024**2?(b/1024**2).toFixed(1)+"M":b>=1024?(b/1024).toFixed(1)+"K":b+"B";
let libApp=null,libItems=[];
function renderLibrary(loading,err){
  const apps=D.library_apps||[];
  // type="button" is REQUIRED on every control here: inside <form method=
  // "dialog"> a bare <button> defaults to submit, which closes the dialog
  const tabs=apps.length>1?`<div class="df" style="justify-content:flex-start">`+
    apps.map(a=>`<button type="button" class="bt${a===libApp?" go":""}" data-libapp="${a}">${
      esc(t(a==="radarr"?"libRadarr":"libSonarr"))}</button>`).join(" ")+`</div>`:"";
  const body=loading?`<div class="db">${esc(t("libLoading"))}</div>`:
    err?`<div class="db">${esc(err)}</div>`:
    !libItems.length?`<div class="db">${esc(t("libEmpty"))}</div>`:
    `<div class="libwrap"><table class="libt"><tbody>`+libItems.map(it=>
      `<tr><td>${esc(it.title)}${it.year?` (${it.year})`:""}</td>`+
      `<td class="r">${fmtB(it.size_bytes)}</td>`+
      `<td><button type="button" class="bt libdel" data-id="${it.id}" `+
      `aria-label="${esc(t("libDelete"))} ${esc(it.title)}">${esc(t("libDelete"))}</button></td></tr>`
    ).join("")+`</tbody></table></div>`;
  dlg.innerHTML=`<form method="dialog"><div class="dh">${esc(t("libBtn"))}</div>`+
    tabs+body+
    `<div class="df"><button class="bt go" value="ok">${esc(t("close"))}</button></div></form>`;}
async function loadLibrary(){
  renderLibrary(true);
  try{
    const r=await fetch(`/api/library?app=${encodeURIComponent(libApp)}`);
    if(r.status===401)return expired();
    if(!r.ok)return renderLibrary(false,await r.text());
    libItems=(await r.json()).items||[];
    renderLibrary(false);
  }catch(err){renderLibrary(false,String(err));}}
document.addEventListener("click",async e=>{
  const open=e.target.closest&&e.target.closest("button.libopen");
  if(open){
    const apps=D.library_apps||[];
    if(!apps.length)return;
    if(!apps.includes(libApp))libApp=apps[0];
    dlg.className="wide";renderLibrary(true);dlg.showModal();await loadLibrary();return;
  }
  const tab=e.target.closest&&e.target.closest("button[data-libapp]");
  if(tab){libApp=tab.dataset.libapp;await loadLibrary();return;}
  const del=e.target.closest&&e.target.closest("button.libdel");
  if(!del||del.disabled)return;
  const id=parseInt(del.dataset.id,10),item=libItems.find(x=>x.id===id);
  if(!item)return;
  // ask() reuses this same <dialog>, and showModal() on an open one throws.
  // close() fires its close event ASYNCHRONOUSLY, so the wait below is not
  // optional: without it that stale event lands on ask()'s own once-listener
  // and resolves the confirm before the user ever sees it.
  await new Promise(r=>{
    if(!dlg.open)return r();
    dlg.addEventListener("close",r,{once:true});dlg.close();});
  const go=await ask(t("libDelTitle"),t("libDelAsk")(item.title),t("libDelOk"));
  dlg.className="wide";renderLibrary(false);dlg.showModal();
  if(!go)return;
  // the re-render above detached `del`; disable the fresh node, not the old one
  const btn=dlg.querySelector(`button.libdel[data-id="${id}"]`)||del;
  btn.disabled=true;
  try{
    const r=await fetch("/api/library/delete",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({app:libApp,id})});
    if(r.status===401)return expired();
    if(r.status===403)return toast(t("libLocalOnly"),1);
    if(!r.ok)return toast(await r.text(),1);
    const res=await r.json();
    toast(t("libDeleted")(fmtB(res.freed_bytes))+
      (res.torrents_cleaned?` · ${t("libTorrentCleaned")}`:""));
    await loadLibrary();
  }catch(err){toast(String(err),1);}
  finally{btn.disabled=false;}});

// ================= 3 · CONSUMO =================
function power(){
  const p=D.power||{},c=p.cost,s=D.summary||{};
  CUR=p.currency||"$";
  const per=p.per||[],mine=per.find(x=>x.measured)||per[0];
  // per-machine chart series + legend: color by machine order, current W beside
  // each name ("—" while offline). The combined mint line stays as-is.
  const MPAL=["#67e8f9","#a78bfa","#f472b6","#f5b544"];
  const wById={};per.forEach(q=>{wById[q.id]=q.watts});
  const mach=D.machines||[];
  const mseries=mach.map((m,i)=>({name:short(m.name),color:MPAL[i%MPAL.length],
    vals:(p.mhist&&p.mhist[m.id])||[]})).filter(x=>x.vals.filter(v=>v!=null).length>1);
  const leg=`<div class="clgd"><span class="clg"><i style="background:#8fa9a2;opacity:.55"></i>${t("fleet")} ${
      p.watts==null?"—":p.watts.toFixed(0)+"W"}</span>`+
    mach.map((m,i)=>`<span class="clg"><i style="background:${MPAL[i%MPAL.length]}"></i>${
      esc(short(m.name))} ${wById[m.id]!=null?wById[m.id].toFixed(0)+"W":"—"}</span>`).join("")+`</div>`;
  let out=`<div class="ctop">`+
    `<div class="hero"><div class="v">${p.watts==null?"—":p.watts.toFixed(0)}<u>W</u></div>`+
    `<div class="k">${t("now")} · ${per.length>1?t("fleet"):esc(short(mine?mine.name:"—"))}<br>${p.modelled?t("est"):t("meas")}</div>`+
    `<div class="r">${per.map(x=>`${esc(short(x.name))} ${x.watts.toFixed(0)}W`).join(" · ")||"—"}</div></div>`+
    `<div class="chart">${chart(p.hist||[],mseries)}${leg}</div></div>`;

  // Measured (accumulated watt-hours) leads; the extrapolation rides beneath it
  // so a projection is never mistaken for a bill.
  const a=p.actual;
  // A projection built from the RECORDED average beats extrapolating the current
  // reading: it ignores momentary spikes and tightens as the month accumulates.
  // Falls back to the instantaneous extrapolation until there is enough data.
  const projM=a&&a.month_proj_cost!=null?a.month_proj_cost:null;
  const projY=a&&a.year_proj_cost!=null?a.year_proj_cost:null;
  out+=`<div class="costs">`+
    `<div class="cost"><div class="lbl">${t("today")}</div>`+
    `<div class="v">${a?money(a.today_cost):"—"}</div>`+
    `<div class="sub">${a?`${a.today_kwh.toFixed(2)} kWh · ${a.today_hours.toFixed(1)} h`:t("noRecord")}</div></div>`+
    `<div class="cost on"><div class="lbl">${t("month")}</div>`+
    `<div class="v">${a?money(a.month_cost):"—"}</div>`+
    `<div class="sub">${a?`${a.month_kwh.toFixed(1)} kWh · ${a.month_days} ${a.month_days===1?t("day"):t("days")}`:t("noRecord")}`+
    `${projM!=null?` · ${t("proj")} ${money(projM)}`:(c?` · ${t("proj")} ${money(c.month)}`:"")}</div>`+
    `${a&&a.month_avg_w!=null?`<div class="sub">${t("avg")} ${a.month_avg_w.toFixed(0)}W · ${
      t("cover")} ${Math.round(a.coverage*100)}% ${t("ofMonth")}</div>`:""}</div>`+
    `<div class="cost"><div class="lbl">${t("year")}</div>`+
    `<div class="v">${projY!=null?money(projY):(c?money(c.year):"—")}</div>`+
    `<div class="sub">${a&&a.year_kwh?`${a.year_kwh.toFixed(1)} ${t("soFar")}`:t("atRate")}</div></div></div>`;

  const wps=(p.watts!=null&&s.count)?(p.watts/s.count):null;
  out+=`<div class="stats">`+
    `<div class="stat"><div class="v">${c?c.kwh_month.toFixed(0):"—"}</div><div class="k">${t("kwhMonth")}</div></div>`+
    `<div class="stat"><div class="v">${s.count!=null?s.count:"—"}</div><div class="k">${t("services")}</div></div>`+
    `<div class="stat"><div class="v">${s.containers!=null?s.containers:"—"}</div><div class="k">${t("containers")}</div></div>`+
    `<div class="stat"><div class="v mint">${wps==null?"—":wps.toFixed(1)}</div><div class="k">${t("wSvc")}</div></div></div>`;

  if(p.modelled)
    out+=`<div class="note"><b>${t("estHead")}</b>${t("estBody")}${
      per.some(x=>x.measured)?t("estRapl"):""}${
      p.actual&&p.actual.since?t("estSince")+esc(p.actual.since)+". ":""}`+
      `${t("estTariff")} ${money(p.rate)}/kWh · ${esc(p.note||"")}. ${t("estFixed")}`+
      (p.offline&&p.offline.length?` ${t("notCounted")}: ${esc(p.offline.join(", "))}.`:"")+`</div>`;
  else
    out+=`<div class="note">${t("estTariff")} ${money(p.rate)}/kWh · ${esc(p.note||"")}. ${t("estFixed")}</div>`;
  if(p.auto)
    out+=`<div class="note"><b>${t("tariffAutoHead")}</b>${t("tariffAutoBody")}</div>`;
  $("p-body").innerHTML=out;}

// ================= 4 · SEGURIDAD =================
// Facts come raw from the server (_security_check); every visible word is
// built here so the ES/EN toggle re-labels this screen like the others.

// scan history: one bar per sweep — height = files scanned, red = infections,
// cyan = the weekly deep pass. Hover a bar for date + numbers.
function scanBars(h){
  if(!h||!h.length)return"";
  const W=280,H=46,n=h.length,step=W/Math.max(12,n),bw=Math.max(3,step-2);
  const mx=Math.max(1,...h.map(e=>e.scanned||0));
  return`<svg class="scanbars" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img" aria-label="${esc(t("scanHist"))}">`+
    h.map((e,i)=>{
      const v=e.scanned||0,bh=Math.max(2.5,Math.round(v/mx*(H-6)));
      const c=e.infected?"var(--red)":e.mode==="deep"?"var(--cyan)":"var(--mint)";
      const tip=`${new Date((e.ts||0)*1000).toLocaleDateString(t("loc"))} · ${v} · ${
        e.infected||0}✕`;
      return`<rect x="${(i*step+1).toFixed(1)}" y="${H-bh}" width="${bw.toFixed(1)}" height="${bh}" rx="1.5" fill="${c}" opacity=".9"><title>${esc(tip)}</title></rect>`;}).join("")+`</svg>`;}

function security(){
  const x=D&&D.security;
  if(!x){$("x-body").innerHTML=`<div class="card">${t("waiting")}</div>`;return;}
  const W=WORD(),CC={up:"var(--mint)",degraded:"var(--amber)",down:"var(--red)"};
  const num=n=>n==null?"—":n>=1e6?(n/1e6).toFixed(1)+"M":n.toLocaleString(t("loc"));
  const line=(k,v,bad)=>`<div class="secline${bad?" bad":""}">${k?`<span class="k">${esc(k)}</span>`:""}${v}</div>`;
  const card=(title,st,lines)=>{
    const s=CC[st]?st:"down";
    return`<div class="cat seccard" style="--cc:${CC[s]}"><div class="cath"><i></i>`+
      `<span class="n">${title}</span><span class="c ${s}">${GLY[s]||"●"} ${esc(W[s]||s)}</span></div>`+
      lines.filter(Boolean).join("")+`</div>`;};
  const f=x.firewall||{},a=x.sigs||{},s=x.scan||{},u=x.updates||{},sf=x.surface;
  const db=a.db||{},pend=u.pending;

  // hero: what the wall has eaten, and the live block-rate chart beside it
  let out=`<div class="ctop">`+
    `<div class="hero"><div class="v" style="color:${CC[f.status]||CC.down}${
       f.status!=="up"?";text-shadow:none":""}">${f.verified?num(f.blocked):"—"}<u>pkt</u></div>`+
    `<div class="k">${t("fwHero")} · ${t("fwSince")}<br>${
       f.verified?esc(t("fwChains")(f.chains??"—",f.rules??"—")):esc(t("fwIndirect"))}</div>`+
    `<div class="r">${f.probes!=null?esc(t("fwProbes")(f.probes)):"&nbsp;"}</div></div>`+
    `<div class="chart"><div class="lbl" style="padding:4px 8px 0">${t("fwChart")}</div>${
       chart(f.bhist&&f.bhist.length>1?f.bhist:[])}</div></div>`;

  // stat tiles
  const st=(v,k,warn)=>`<div class="stat"><div class="v${warn?" warnv":""}">${v}</div><div class="k">${k}</div></div>`;
  out+=`<div class="stats">`+
    st(f.rules!=null?f.rules:"—",t("stRules"))+
    st(num(db.total),t("stSigs"))+
    st(s.scanned!=null?num(s.scanned):"—",t("stScanned"))+
    st(pend?pend.total:"—",t("stPend"),pend&&pend.security>0)+
    st(sf?sf.public:"—",t("stPorts"))+`</div>`;

  out+=`<div class="cats">`;
  // firewall internals
  out+=card(t("secFw"),f.status,[
    line("",f.verified?(f.rules?`<b>${esc(t("fwRules")(f.rules))}</b>`:esc(t("fwNone"))):esc(t("fwIndirect"))),
    f.verified?line("",esc(t("fwPolicy")(f.policy))):"",
    ...(f.sets||[]).map(o=>line(t("fwSets"),t("fwSet")(o.name,o.ports))),
    line(t("fwUnit"),`<b>${esc(f.unit||"—")}</b>`)]);
  // antivirus
  out+=card(t("secAv"),a.status,[
    line("",a.installed?(a.age_s!=null?`<b>${esc(t("avSig")(agoTxt(a.age_s)))}</b> · ${
      esc(t("avDb")(db.daily_ver,db.total))}`:esc(t("avNoSig"))):esc(t("avOff"))),
    a.installed&&a.engine?line(t("avEngine"),`<b>ClamAV ${esc(a.engine)}</b> · ${
      esc(t("avFresh"))} <b>${esc(a.svc||"—")}</b>`):"",
    a.installed?line(t("avSchedLbl"),esc(t("avSched"))):""]);
  // last scan + history bars
  out+=card(t("secScan"),s.status,[
    line("",s.never?esc(t("scanNever")):`<b>${esc(agoTxt(s.age_s))}</b> · ${
      esc(s.mode==="deep"?t("scanDeep"):t("scanDaily"))} · ${esc(t("scanDur")(s.duration_s))}`),
    s.never?"":line("",(s.infected?`<b>${esc(t("scanHits")(s.infected))}</b>`:esc(t("scanClean")(s.scanned)))+
      (s.known?` · ${esc(t("scanKnown")(s.known))}`:""),!!s.infected),
    ...(s.hits||[]).map(h=>line("",`✕ ${esc(h)}`,1)),
    s.err?line("err",esc(s.err),1):"",
    (s.hist&&s.hist.length)?line(t("scanHist"),scanBars(s.hist)):"",
    s.running?line("",`<span class="scanrun">${esc(t("scanRunning")(s.running))}</span>`):
      line("",`<button class="upd sbtn" data-deep="0">${t("scanNow")}</button> `+
        `<button class="upd sbtn" data-deep="1">${t("scanDeepBtn")}</button>`)]);
  // patching
  out+=card(t("secPatch"),u.status,[
    line("",u.installed?(u.conf&&u.timer==="active"?esc(t("patchOn")):esc(t("patchBroken"))):esc(t("patchOff"))),
    pend?line("",esc(t("patchPend")(pend.total,pend.security))+
      (pend.total&&pend.names&&pend.names.length?`<br><span style="color:var(--dim)">${
        pend.names.map(esc).join(" · ")}</span>`:"")):"",
    u.installed?line("",u.age_s!=null?esc(t("patchRun")(agoTxt(u.age_s))):esc(t("patchNoRun"))):""]);
  // The topology diagram moved to its own screen (5 · RED). What stays here is
  // the per-port verdict table — a list, which this card does better than a picture.
  if(sf){
    const BW={open:t("nsOpen"),lan:t("nsLan"),blocked:t("nsBlock"),policy:t("nsBlock")};
    out+=`<div class="cat seccard" style="--cc:var(--cyan);grid-column:1/-1"><div class="cath"><i></i>`+
      `<span class="n">${t("netSurf")}</span><span class="c">${sf.public}</span></div>`+
      `<div class="prows">`+
      (sf.rows||[]).map(r=>`<div class="prow"><span class="pp">${esc(r.port)}</span>`+
        `<span class="pn">${esc(r.name||r.proto)}</span>`+
        `<span class="pb ${esc(r.fw)}">${esc(BW[r.fw]||r.fw)}</span></div>`).join("")+`</div>`+
      (sf.more?line("",esc(t("nsMore")(sf.more))):"")+
      line("",esc(t("nsCounts")(sf.local,sf.tail,sf.docker)))+`</div>`;}
  out+=`</div><div class="note">${t("secNote")} ${t("secChecked")}: ${
    esc(x.checked?new Date(x.checked*1000).toLocaleTimeString(t("loc"),{hour12:false}):"—")}.</div>`;
  $("x-body").innerHTML=out;}

// Header badge: visible from EVERY screen the moment any security component
// stops working (red = down, amber = degraded); tapping it jumps to screen 4.
function secBadge(){
  const lvl=D&&D.security&&D.security.alert,b=$("secbadge");
  b.hidden=!lvl;
  b.className="tb secb"+(lvl==="down"?" bad":lvl==="degraded"?" warn":"");
  if(lvl)b.textContent=t("secBadge");}

// ================= 5 · RED (network surface) =================
// REBUILD-GATED. The SVG is animated, and a full innerHTML replace every 3s would
// restart every CSS animation mid-flight (visible stutter). So the skeleton is
// rebuilt ONLY when the topology itself changes — machines, their online state,
// the firewall verdict set, the language — and every other tick just patches the
// text nodes marked data-k. Steady state touches no SVG element at all.
let nsUid=0;
function netModel(){
  const x=(D&&D.security)||{},sf=x.surface||{},fw=x.firewall||{},tun=x.tunnel||{};
  const ms=(D&&D.machines)||[];
  // authoritative: the server stamps local=true on the box netops runs on.
  // Guessing by name picks the wrong hub the moment a machine is renamed.
  const hub=ms.find(m=>m.local)||null;
  const tunHosts=new Set((tun.hosts||[]).map(h=>h.host));
  const others=ms.filter(m=>m!==hub).map(m=>{
    const urls=[];(m.categories||[]).forEach(c=>(c.services||[]).forEach(s=>{
      if(s.url)urls.push(s.url);}));
    // a public hostname this machine serves that the hub's tunnel does NOT carry
    // can only be reaching the internet by its own route
    return {m,own:urls.filter(u=>!tunHosts.has(u))};});
  const rows=sf.rows||[];
  const sp=(D&&D.speed)||{};
  return {hub,others,sf,fw,tun,rows,sp,
    nOpen:rows.filter(r=>r.fw==="open").length,
    nLan:rows.filter(r=>r.fw==="lan").length,
    nBlk:rows.filter(r=>r.fw==="blocked"||r.fw==="policy").length+(sf.more||0)};}
// structural only — nothing that merely ticks (load, rates, temps, counts)
function netSig(o){
  return [L,o.hub?o.hub.id:"-",o.fw.status||"-",
    o.others.map(z=>z.m.id+":"+(((z.m.host||{}).online)?1:0)+":"+z.own.length).join(","),
    (o.rows||[]).map(r=>r.port+r.fw).join(","),(o.tun.n||0)].join("|");}
// D.speed is Mbit/s (see the VITALS readout), NOT bytes/s. Three coarse tiers
// on purpose: animation-duration is only changed when the tier changes, because
// retargeting a running animation rescales its progress and the dashes jump.
const nsSpeed=m=>m==null?"s1":m>=200?"s3":m>=25?"s2":"s1";
function nsNode(x,y,w,h,title,sub,col,dim){
  return`<g class="nsn${dim?" off":""}"><rect x="${x}" y="${y}" width="${w}" height="${h}" rx="11"
    fill="#0b1214" stroke="${col}" stroke-width="1.5"/>
    <text x="${x+w/2}" y="${y+25}" text-anchor="middle" fill="${col}" class="nst">${esc(title)}</text>`+
    sub.map((s,i)=>`<text x="${x+w/2}" y="${y+45+i*17}" text-anchor="middle" fill="${s.c||"#8fa9a2"}"
      class="nss"${s.k?` data-k="${s.k}"`:""}>${esc(s.t)}</text>`).join("")+`</g>`;}
function nsEdge(x1,y1,x2,y2,col,cls,bend){
  const mx=(x1+x2)/2,by=bend==null?0:bend;
  const d=`M${x1} ${y1} C${mx} ${y1+by} ${mx} ${y2+by} ${x2} ${y2}`;
  return`<path d="${d}" fill="none" stroke="${col}" stroke-width="1.7" class="${cls}"/>`;}
function netBuild(o){
  const u=++nsUid,gid=`nsg${u}`;
  const fwOk=o.fw.status==="up",fwCol=fwOk?"#5df2a0":"#ff6b6b";
  const hubOn=o.hub&&(o.hub.host||{}).online,hubCol=hubOn?"#5df2a0":"#ff6b6b";
  const dnCls="ns-flow "+nsSpeed(o.sp.down),upCls="ns-flow "+nsSpeed(o.sp.up);
  let s=`<defs><linearGradient id="${gid}" x1="0" y1="0" x2="1" y2="0">
    <stop offset="0" stop-color="#a78bfa" stop-opacity=".9"/>
    <stop offset="1" stop-color="#5df2a0" stop-opacity=".9"/></linearGradient></defs>`;
  // ---- edges (drawn first, boxes paint over them) ----
  // tunnel: cloudflared dials OUT and delivers to loopback, so it never crosses
  // nftables. Routed ABOVE the barrier and labelled, because the old diagram drew
  // it straight through the wall and implied the opposite.
  s+=`<path d="M190 300 C300 300 300 96 470 96 L700 96 L700 150" fill="none"
      stroke="url(#${gid})" stroke-width="2" class="${dnCls}"/>`;
  s+=`<text x="492" y="84" fill="#a78bfa" class="nsl">${esc(t("nsBypass"))}</text>`;
  // exposed path: the ports that really do face the internet, through the barrier
  s+=nsEdge(190,320,470,300,fwOk?"#5df2a0":"#ff6b6b",upCls,0);
  s+=nsEdge(548,300,700,300,fwOk?"#5df2a0":"#ff6b6b","ns-flow s1",0);
  // mesh spine
  s+=nsEdge(190,360,300,470,"#67e8f9","ns-flow s1",0);
  o.others.forEach((z,i)=>{const on=(z.m.host||{}).online,y=140+i*160,cy=y+52;
    // under the hub, never through it
    s+=`<path d="M420 505 C640 562 820 566 958 566 L958 ${cy+16} Q958 ${cy} 980 ${cy}"
      fill="none" stroke="${on?"#67e8f9":"#22383c"}" stroke-width="1.7"
      class="${on?"ns-flow s1":"ns-dead"}"/>`;
    // its OWN way out: independent of the hub's tunnel AND of its firewall.
    // Own lane at x>1170 so it clears every machine box.
    if(z.own.length){const lane=1178+i*8,top=16+i*11;
      s+=`<path d="M115 262 C115 44 700 ${top} ${lane} ${top} L${lane} ${cy} L1170 ${cy}"
        fill="none" stroke="#a78bfa" stroke-width="1.6"
        class="${on?"ns-flow s2":"ns-dead"}"/>`;}});
  // ---- nodes ----
  s+=nsNode(40,262,150,76,t("nsNet"),[{t:t("nsPublic")}],"#8fa9a2");
  s+=nsNode(250,58,220,76,t("nsTunnel"),
    [{t:t("nsHostsN")(o.tun.n!=null?o.tun.n:"—"),c:"#a78bfa",k:"tunN"}],"#a78bfa");
  s+=nsNode(250,440,220,76,t("nsMesh"),[{t:t("nsMeshSub"),c:"#67e8f9"}],"#67e8f9");
  // firewall barrier
  s+=`<rect x="490" y="150" width="58" height="330" rx="9" fill="#101a1c"
    stroke="${fwCol}" stroke-width="1.5"/>
    <text x="519" y="315" text-anchor="middle" fill="${fwCol}" class="nsfw"
      transform="rotate(-90 519 315)">nftables</text>
    <text x="519" y="140" text-anchor="middle" fill="#8fa9a2" class="nsl"
      data-k="fwRules">${esc(t("nsRulesN")(o.fw.rules!=null?o.fw.rules:"—"))}</text>
    <text x="519" y="500" text-anchor="middle" fill="#6f8c86" class="nsl"
      data-k="fwBlocked">${esc(t("nsDropN")(o.fw.blocked!=null?num2(o.fw.blocked):"—"))}</text>`;
  // hub
  if(o.hub){
    s+=`<rect x="700" y="150" width="250" height="300" rx="12" fill="#0c1315"
      stroke="${hubCol}" stroke-width="1.6" class="${hubOn?"":"ns-pulse"}"/>
      <text x="825" y="180" text-anchor="middle" fill="${hubCol}" class="nsh">${
        esc(short(o.hub.name||"?").toUpperCase())}</text>
      <text x="825" y="199" text-anchor="middle" fill="#6f8c86" class="nsl">${esc(o.hub.role||"")}</text>`;
    const rowsL=[["nsLOpen","open","#5df2a0"],["nsLanOnly","lan","#67e8f9"],
      ["nsBlocked","blk","#f5b544"],["nsLoop","loop","#6f8c86"],
      ["nsDocker","dock","#6f8c86"],["nsSvc","svc","#8fa9a2"],["nsCtn","ctn","#8fa9a2"]];
    rowsL.forEach((r,i)=>{const y=232+i*28;
      s+=`<circle cx="722" cy="${y-4}" r="3.5" fill="${r[2]}"/>
        <text x="736" y="${y}" fill="#8fa9a2" class="nss">${esc(t(r[0]))}</text>
        <text x="930" y="${y}" text-anchor="end" fill="${r[2]}" class="nsv" data-k="${r[1]}">—</text>`;});
  }else{
    s+=`<text x="825" y="300" text-anchor="middle" fill="#ff6b6b" class="nsh">${esc(t("nsNoHub"))}</text>`;}
  // remote machines
  o.others.forEach((z,i)=>{const on=(z.m.host||{}).online,y=140+i*160;
    const col=on?"#5df2a0":"#ff6b6b";
    s+=nsNode(980,y,190,104,short(z.m.name||"?").toUpperCase(),
      [{t:z.m.role||"",c:"#6f8c86"},
       {t:"—",c:on?"#8fa9a2":"#ff6b6b",k:"m"+z.m.id},
       {t:z.own.length?t("nsOwnPath"):t("nsViaMesh"),c:z.own.length?"#a78bfa":"#6f8c86"}],
      col,!on);});
  return`<div class="nswrap"><svg viewBox="0 0 1210 600" preserveAspectRatio="xMidYMid meet"
    role="img" aria-label="${esc(t("nsTitle"))}">${s}</svg></div>`;}
function netPatch(o){
  const set=(k,v)=>{const e=$("n-body").querySelector(`[data-k="${k}"]`);
    if(e)e.textContent=v;};
  const hs=(o.hub&&o.hub.summary)||{};
  set("tunN",t("nsHostsN")(o.tun.n!=null?o.tun.n:"—"));
  set("fwRules",t("nsRulesN")(o.fw.rules!=null?o.fw.rules:"—"));
  set("fwBlocked",t("nsDropN")(o.fw.blocked!=null?num2(o.fw.blocked):"—"));
  set("open",o.nOpen);set("lan",o.nLan);set("blk",o.nBlk);
  set("loop",o.sf.local!=null?o.sf.local:"—");
  set("dock",o.sf.docker!=null?o.sf.docker:"—");
  set("svc",`${hs.online!=null?hs.online:"—"}/${hs.count!=null?hs.count:"—"}`);
  set("ctn",hs.containers!=null?hs.containers:"—");
  o.others.forEach(z=>{const on=(z.m.host||{}).online,sm=z.m.summary||{};
    set("m"+z.m.id,on?`${sm.online!=null?sm.online:"—"}/${sm.count!=null?sm.count:"—"} svc`
                     :t("offline"));});}
function netSurface(){
  const host=$("n-body");if(!host)return;
  const o=netModel(),sig=netSig(o);
  if(host.dataset.nsSig!==sig){host.innerHTML=netBuild(o);host.dataset.nsSig=sig;}
  netPatch(o);
  const sp=o.sp,mbps=v=>v==null?"—":v.toFixed(0)+" Mb/s",legend=[
    `${t("nsDown")} ${mbps(sp.down)}`,`${t("nsUp")} ${mbps(sp.up)}`,
    `${t("nsPorts")} ${o.nOpen+o.nLan+o.nBlk}`];
  const el=$("n-legend");if(el)el.textContent=legend.join("  ·  ");}

// ---------- swipe + dots ----------
const track=$("screens");
function paintDots(){
  $("dots").innerHTML=LABELS.map((l,i)=>
    `<button class="dot${i===cur?" on":""}" data-i="${i}" aria-label="${t("goto")} ${l}"></button>`).join("");}
const goto=i=>track.scrollTo({left:i*track.clientWidth,
  behavior:matchMedia("(prefers-reduced-motion:reduce)").matches?"auto":"smooth"});
$("dots").addEventListener("click",e=>{const b=e.target.closest(".dot");if(b)goto(+b.dataset.i);});
let cur=0,st=null;
function mark(i){
  cur=i;$("scn").textContent=i+1;$("scl").textContent=LABELS[i]||"";
  [...$("dots").children].forEach((d,j)=>d.classList.toggle("on",j===i));
  $("prev").disabled=i<=0;$("next").disabled=i>=LABELS.length-1;
  $("live").textContent=LABELS[i]||"";}
$("prev").onclick=()=>goto(Math.max(0,cur-1));
$("next").onclick=()=>goto(Math.min(LABELS.length-1,cur+1));
$("secbadge").onclick=()=>goto(3);

// Switching language re-labels the chrome and re-renders every screen from data
// already in hand — no refetch, so the change is instant. The choice is kept in
// localStorage and, on a first visit, seeded from the browser's own language.
function applyLang(){
  LABELS=T[L].sc;
  document.documentElement.lang=L;
  $("lang").textContent=(L==="es"?"EN":"ES");
  $("logout").textContent=t("logout");
  $("prev").setAttribute("aria-label",t("prev"));
  $("next").setAttribute("aria-label",t("next"));
  $("dots").setAttribute("aria-label",t("screens"));
  LABELS.forEach((l,i)=>{const el=$("sc"+i);if(el)el.setAttribute("aria-label",l);});
  paintDots();mark(cur);
  if(!D)$("foot").textContent=t("conn");
  if(D)paint(D,1);}
$("lang").onclick=()=>{
  L=(L==="es"?"en":"es");
  try{localStorage.netopsLang=L}catch(e){}
  applyLang();clock();};
applyLang();
track.addEventListener("scroll",()=>{
  clearTimeout(st);
  st=setTimeout(()=>{
    const i=Math.round(track.scrollLeft/Math.max(1,track.clientWidth));
    if(i!==cur)mark(i);},60);},{passive:true});
track.tabIndex=0;
track.addEventListener("keydown",e=>{
  if(e.key==="ArrowRight"){e.preventDefault();goto(Math.min(LABELS.length-1,cur+1));}
  if(e.key==="ArrowLeft"){e.preventDefault();goto(Math.max(0,cur-1));}});
addEventListener("resize",()=>goto(cur));

const num2=n=>n>=1e6?(n/1e6).toFixed(1)+"M":Number(n).toLocaleString(t("loc"));

// ---------- data ----------
function expired(){try{delete localStorage.netopsCache}catch(e){}location.replace("/");}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const stepTxt=k=>(T[L].st&&T[L].st[k])||(T.es.st&&T.es.st[k])||k||"";
// Known server plain-text errors -> localized; anything unrecognized shows raw.
const SRVMAP={"update already running":"srvBusy","scan already running":"srvScanBusy",
  "unknown service":"srvUnknown","unknown machine":"srvUnknown","bad json":"srvBad",
  "bad content-length":"srvBad","no containers matched":"srvNoCont",
  "container is not compose-managed":"srvNoCompose","start failed":"srvFail",
  "remote not supported":"srvNoRemote","restart failed":"srvFail",
  "operation already running":"srvBusyOp"};
const srvMsg=s=>{const k=SRVMAP[String(s).trim()];return k?t(k):s;};
let toastT=null;
function toast(msg,bad,sticky){
  const el=$("toast");el.className="toast"+(bad?" bad":"")+(sticky?" work":"");
  el.textContent=msg;el.hidden=false;   // toast is role=status; #live would double-announce
  clearTimeout(toastT);
  if(!sticky)toastT=setTimeout(()=>{el.hidden=true},7000);}
const dlg=$("dlg");
function ask(title,body,okLabel){
  return new Promise(res=>{
    let out=false;
    dlg.className="";
    dlg.innerHTML=`<form method="dialog"><div class="dh">${esc(title)}</div>`+
      `<div class="db">${esc(body)}</div><div class="df">`+
      `<button class="bt" type="button" id="dx">${esc(t("cancel"))}</button>`+
      `<button class="bt go" id="dok" value="ok">${esc(okLabel)}</button></div></form>`;
    dlg.querySelector("form").addEventListener("submit",()=>{out=true});
    dlg.querySelector("#dx").onclick=()=>dlg.close();
    dlg.addEventListener("close",()=>res(out),{once:true});
    dlg.showModal();dlg.querySelector("#dok").focus();});}

// The POST only STARTS the update and returns a job id; the pull runs server-side
// and is polled. Keeps every request short (Cloudflare cuts an origin response at
// ~100s) and lets the board narrate what compose is doing.
const updating=new Set();
async function doUpd(mid,name){
  if(!await ask(t("updTitle"),t("updAsk")(name),t("updOk")))return;
  const key=mid+"/"+name;updating.add(key);
  const stop=(msg,bad)=>{updating.delete(key);if(D)paint(D,1);if(msg)toast(msg,bad);};
  let jid;
  try{
    const r=await fetch("/api/update",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({machine:mid,service:name})});
    if(r.status===401)return expired();
    const txt=await r.text();
    if(!r.ok)return stop(`${name}: ${srvMsg(txt)}`,1);
    jid=JSON.parse(txt).job;
  }catch(e){return stop(`${name}: ${e}`,1);}
  toast(`${name} · ${stepTxt("preparing")}`,0,1);
  for(let miss=0;;){
    await sleep(1200);
    let j;
    try{
      const r=await fetch("/api/job?id="+encodeURIComponent(jid));
      if(r.status===401)return expired();
      if(!r.ok){if(++miss>4)return stop(`${name}: ${t("updLost")}`,1);continue;}
      j=await r.json();miss=0;
    }catch(e){if(++miss>4)return stop(`${name}: ${e}`,1);continue;}
    if(j.state==="running"){toast(`${name} · ${stepTxt(j.step)}`,0,1);continue;}
    stop(j.state==="done"?t("updDone")(name):`${name}: ${j.msg||stepTxt("failed")}`,j.state!=="done");
    tick();
    return;
  }}
document.addEventListener("click",e=>{
  const b=e.target.closest&&e.target.closest("button.upd:not(.sbtn)");
  if(!b||b.disabled)return;
  b.disabled=true;doUpd(b.dataset.mid,b.dataset.svc);});
// Dockge-without-Dockge: restart + logs per LOCAL docker service, straight
// from the row. Restart confirms first and shares the server-side update
// guard; logs render escaped in the shared dialog.
document.addEventListener("click",async e=>{
  const b=e.target.closest&&e.target.closest("button.act");
  if(!b||b.disabled)return;
  const mid=b.dataset.mid,svc=b.dataset.svc;
  b.disabled=true;
  try{
    if(b.classList.contains("rst")){
      if(await ask(t("rstTitle"),t("rstAsk")(svc),t("rstOk"))){
        const r=await fetch("/api/restart",{method:"POST",
          headers:{"Content-Type":"application/json"},
          body:JSON.stringify({machine:mid,service:svc})});
        if(r.status===401)return expired();
        const txt=await r.text();
        toast(r.ok?t("rstDone")(svc):`${svc}: ${srvMsg(txt)}`,!r.ok);
        tick();
      }
    }else{
      const r=await fetch(`/api/logs?machine=${encodeURIComponent(mid)}&service=${encodeURIComponent(svc)}&lines=150`);
      if(r.status===401)return expired();
      if(!r.ok)return toast(`${svc}: ${srvMsg(await r.text())}`,1);
      dlg.className="wide";
      dlg.innerHTML=`<form method="dialog"><div class="dh">${esc(t("logsBtn"))} · ${esc(svc)}</div>`+
        `<pre class="logsv">${esc(await r.text())}</pre>`+
        `<div class="df"><button class="bt go" value="ok">${esc(t("close"))}</button></div></form>`;
      dlg.showModal();
    }
  }catch(err){toast(String(err),1);}
  finally{b.disabled=false;}});
// on-demand ClamAV sweep; the server refuses (409) while one is running
document.addEventListener("click",async e=>{
  const b=e.target.closest&&e.target.closest("button.sbtn");
  if(!b||b.disabled)return;
  b.disabled=true;
  try{
    const r=await fetch("/api/scan",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({deep:b.dataset.deep==="1"})});
    if(r.status===401)return expired();
    if(!r.ok)return toast(srvMsg(await r.text()),1);
    toast(t("scanStarted"));tick();
  }catch(err){toast(String(err),1);}});
// A repaint replaces the whole subtree, so an in-progress text selection or a
// focused link inside the carousel is destroyed. Data still updates; the DOM
// write is deferred until the user is no longer interacting with it.
const busy=()=>{
  const a=document.activeElement,
        sel=typeof getSelection==="function"?getSelection():null;
  return !!(a&&a!==track&&track.contains&&track.contains(a))||
         !!(sel&&!sel.isCollapsed&&sel.anchorNode&&track.contains&&track.contains(sel.anchorNode));};
function paint(d,force){D=d;if(!force&&busy())return false;
  vitals();services();power();security();netSurface();secBadge();return true;}
// seq guard: a stalled response resolving after a newer one must not repaint
// older data. The footer stamp moves only when the DOM actually repainted, so
// a deferred (busy) or crashed render can't claim freshness; render errors go
// to the console instead of masquerading as "no connection".
let tickSeq=0;
function tick(){
  const seq=++tickSeq;
  fetch("/api/services").then(r=>{
    if(r.status===401){expired();return null;}
    return r.json();
  }).then(d=>{
    if(!d||seq!==tickSeq)return;
    let ok=false;
    try{ok=paint(d)!==false}catch(e){console.error("netops render error:",e)}
    try{localStorage.netopsCache=JSON.stringify(d)}catch(e){}
    if(ok)$("foot").textContent=t("updated")+" "+new Date().toLocaleTimeString(t("loc"));
  }).catch(()=>{if(seq===tickSeq)$("foot").innerHTML=`<span class="err">${t("noconn")}</span>`});}
try{const c=localStorage.netopsCache;if(c){const d=JSON.parse(c);if(d&&d.machines){paint(d);$("foot").textContent=t("cache");}}}catch(e){}
$("logout").onclick=async()=>{try{await fetch("/api/logout",{method:"POST"})}catch(e){}expired();};
function clock(){$("clock").textContent=new Date().toLocaleTimeString(t("loc"),{hour12:false})}
setInterval(clock,1000);clock();setInterval(tick,__REFRESH__);tick();
</script></body></html>"""




# ---- media library cleanup (Sonarr/Radarr) -----------------------------------
# Opt-in via config.json's "arr" key; absent = feature invisible, no crash.
# Deletes an already-imported movie/show AND actually frees the disk space —
# Radarr/Sonarr's own delete only removes ONE side of a hardlinked pair (this
# box's qBittorrent downloads are hardlinked into the library — confirmed via
# `stat`, link count 2), so the qBittorrent-side twin needs a separate,
# best-effort cleanup pass below or the space never comes back.
_ARR_APPS = ("sonarr", "radarr")
QBIT_URL = "http://127.0.0.1:8081"   # same host as netops; pre-existing local auth bypass


def _arr_conf(app):
    c = (_CFG.get("arr") or {}).get(app)
    return c if isinstance(c, dict) and c.get("url") and c.get("api_key") else None


def _arr_call(app, path, method="GET"):
    """One Sonarr/Radarr API call. Raises on any failure — callers decide
    what that means for the response."""
    c = _arr_conf(app)
    if not c:
        raise RuntimeError(f"{app} not configured")
    req = urllib.request.Request(c["url"].rstrip("/") + path, method=method,
                                 headers={"X-Api-Key": c["api_key"]})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read()
    return json.loads(body) if body else None


def _arr_items(app):
    """[{id,title,year,size_bytes,monitored,has_file}], sorted by size
    descending, for the library dialog's list view. Sonarr rows are per
    SERIES (a show is deleted as a whole, matching Sonarr's own delete)."""
    if app == "radarr":
        rows = _arr_call(app, "/api/v3/movie") or []
        items = [{"id": m["id"], "title": m.get("title", "?"),
                  "year": m.get("year"), "size_bytes": m.get("sizeOnDisk") or 0,
                  "monitored": bool(m.get("monitored")),
                  "has_file": bool(m.get("hasFile"))} for m in rows]
    else:
        rows = _arr_call(app, "/api/v3/series") or []
        items = [{"id": s["id"], "title": s.get("title", "?"),
                  "year": s.get("year"),
                  "size_bytes": (s.get("statistics") or {}).get("sizeOnDisk") or 0,
                  "monitored": bool(s.get("monitored")),
                  "has_file": bool((s.get("statistics") or {}).get("episodeFileCount"))}
                 for s in rows]
    items.sort(key=lambda x: x["size_bytes"], reverse=True)
    return items


def _arr_item(app, item_id):
    """{id,title,size_bytes,has_file,paths} for ONE item, fetched fresh right
    before a delete (not the cached list). `paths`: every on-disk file for
    this item — one for a movie, one per episode for a series, since
    deleting a series removes all of them at once."""
    kind = "movie" if app == "radarr" else "series"
    d = _arr_call(app, f"/api/v3/{kind}/{item_id}")
    if app == "radarr":
        mf = d.get("movieFile") or {}
        return {"id": d["id"], "title": d.get("title", "?"),
                "size_bytes": d.get("sizeOnDisk") or 0,
                "has_file": bool(d.get("hasFile")),
                "paths": [mf["path"]] if mf.get("path") else []}
    stats = d.get("statistics") or {}
    has_file = bool(stats.get("episodeFileCount"))
    paths = []
    if has_file:
        files = _arr_call(app, f"/api/v3/episodefile?seriesId={item_id}") or []
        paths = [f["path"] for f in files if f.get("path")]
    return {"id": d["id"], "title": d.get("title", "?"),
            "size_bytes": stats.get("sizeOnDisk") or 0,
            "has_file": has_file, "paths": paths}


def _arr_delete(app, item_id):
    """DELETE the item + its files in Radarr/Sonarr. Raises on failure."""
    kind = "movie" if app == "radarr" else "series"
    _arr_call(app, f"/api/v3/{kind}/{item_id}?deleteFiles=true", method="DELETE")


def _find_hardlink_twins(target_ids, candidates):
    """candidates: [(hash, path, file_id)] of qBittorrent's torrent files, where
    file_id is (st_dev, st_ino) — the DEVICE matters: inode numbers repeat
    across filesystems (qBittorrent sees both /media and /music), so matching
    on inode alone would delete an unrelated torrent. -> {hash, ...}.

    A torrent is only claimed when EVERY one of its files is being deleted.
    One matching file is not enough: a multi-item pack shares a torrent with
    media we are not deleting, and qBittorrent's delete takes the whole
    torrent's files with it. Pure — tuple comparisons, no filesystem access.
    """
    targets = set(target_ids)
    by_torrent = {}
    for h, _, fid in candidates:
        by_torrent.setdefault(h, []).append(fid)
    return {h for h, fids in by_torrent.items()
            if fids and all(f in targets for f in fids)}


def _qbit_torrent_files():
    """[(hash, path, (st_dev, st_ino))] for every file of every qBittorrent
    torrent, best effort — an unreachable/misconfigured qBittorrent yields an
    empty list rather than blocking the primary Radarr/Sonarr deletion."""
    out = []
    try:
        req = urllib.request.Request(QBIT_URL + "/api/v2/torrents/info")
        with urllib.request.urlopen(req, timeout=10) as r:
            torrents = json.loads(r.read())
        for t in torrents:
            h, save = t.get("hash"), t.get("save_path") or ""
            if not h:
                continue
            try:
                freq = urllib.request.Request(
                    QBIT_URL + f"/api/v2/torrents/files?hash={h}")
                with urllib.request.urlopen(freq, timeout=10) as fr:
                    files = json.loads(fr.read())
                for f in files:
                    p = os.path.join(save, f.get("name", ""))
                    try:
                        st = os.stat(p)
                        out.append((h, p, (st.st_dev, st.st_ino)))
                    except OSError:
                        # a file qBittorrent lists but we cannot stat stays in
                        # the torrent's set as an unmatchable sentinel, so the
                        # all()-match below refuses to claim that torrent
                        out.append((h, p, None))
            except Exception:
                continue
    except Exception:
        pass
    return out


def _qbit_delete(h):
    req = urllib.request.Request(
        QBIT_URL + f"/api/v2/torrents/delete?hashes={h}&deleteFiles=true",
        method="POST")
    urllib.request.urlopen(req, timeout=10).read()


class Handler(BaseHTTPRequestHandler):
    # Sent on every response. The board is one self-contained file — no external
    # scripts, styles, fonts or images — so a CSP this tight costs nothing and
    # stops an injected <script src> or a beacon from reaching anywhere.
    _SEC = (("X-Content-Type-Options", "nosniff"),
            ("Referrer-Policy", "no-referrer"),
            ("X-Frame-Options", "DENY"))
    _CSP = ("default-src 'none'; img-src 'self' data:; style-src 'unsafe-inline'; "
            "script-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; "
            "base-uri 'none'; frame-ancestors 'none'")

    def _send(self, code, body, ctype, headers=()):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in self._SEC:
            self.send_header(k, v)
        if ctype.startswith("text/html"):
            self.send_header("Content-Security-Policy", self._CSP)
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        """A live session, from the cookie or the X-Session header.

        The cookie carries the board and /api/services; the header is kept so a
        script holding a token from /api/login can still drive /api/update.
        """
        return (_session_valid(self.headers.get("X-Session", ""))
                or _session_valid(_cookie_token(self.headers.get("Cookie", ""))))

    def _cookie(self, tok, ttl):
        """Set-Cookie value. Secure only when the request actually arrived over
        HTTPS (it always does through the tunnel) — otherwise a browser pointed
        at plain http://127.0.0.1:8787 for testing would silently drop it."""
        https = self.headers.get("X-Forwarded-Proto", "").lower() == "https"
        return (f"{_COOKIE}={tok}; HttpOnly; SameSite=Lax; Path=/; Max-Age={ttl}"
                + ("; Secure" if https else ""))

    def do_HEAD(self):
        # health probes (our own _http_check, external uptime monitors) lead
        # with HEAD; the stdlib server would 501 it and read as "down"
        if self.path in ("/", "/m") or self.path.startswith("/api/services"):
            self.send_response(200)
        else:
            self.send_response(404)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def do_GET(self):
        nostore = (("Cache-Control", "no-store"),)
        if self.path.startswith("/api/services"):
            if not self._authed():
                return self._send(401, b"auth required", "text/plain", nostore)
            self._send(200, json.dumps(_with_history(get_data())).encode(),
                       "application/json", nostore)
        elif self.path.startswith("/api/logs"):
            return self._logs()
        elif self.path.startswith("/api/alerts"):
            if not self._authed():
                return self._send(401, b"auth required", "text/plain", nostore)
            problems = _sec_summary(_sec["v"]) + _fleet_problems(_fleet_state)
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:
                since = float(qs.get("since", ["0"])[0])
            except ValueError:
                since = 0.0
            events = sorted((e for e in _events if e["ts"] > since),
                            key=lambda e: e["ts"], reverse=True)[:100]
            self._send(200, json.dumps({"ok": not problems, "problems": problems,
                                        "events": events}).encode(),
                       "application/json", nostore)
        elif self.path.startswith("/api/library"):
            if not self._authed():
                return self._send(401, b"auth required", "text/plain", nostore)
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            app = qs.get("app", [""])[0]
            if app not in _ARR_APPS:
                return self._send(400, b"bad app", "text/plain", nostore)
            if not _arr_conf(app):
                return self._send(404, b"not configured", "text/plain", nostore)
            try:
                items = _arr_items(app)
            except Exception as e:
                return self._send(502, str(e).encode()[:300], "text/plain", nostore)
            self._send(200, json.dumps({"items": items}).encode(),
                       "application/json", nostore)
        elif self.path.startswith("/api/job"):
            if not self._authed():
                return self._send(401, b"auth required", "text/plain", nostore)
            jid = urllib.parse.parse_qs(
                urllib.parse.urlparse(self.path).query).get("id", [""])[0]
            with _jobs_lock:
                job = dict(_jobs[jid]) if jid in _jobs else None
            if job is None:
                return self._send(404, b"no such job", "text/plain", nostore)
            self._send(200, json.dumps(job).encode(), "application/json", nostore)
        elif self.path in ("/", "/m"):     # /m kept as an alias for old bookmarks
            if not self._authed():
                return self._send(200, LOGIN_PAGE.replace("__TITLE__", TITLE).encode(),
                                  "text/html; charset=utf-8", nostore)
            self._send(200, (PAGE_M.replace("__TITLE__", TITLE)
                                   .replace("__REFRESH__", str(REFRESH_MS))).encode(),
                       "text/html; charset=utf-8", nostore)
        else:
            self._send(404, b"not found", "text/plain")

    def _client_id(self):
        # Behind the tunnel every TCP peer is 127.0.0.1; Cloudflare sets the real
        # client IP header (it can't be spoofed past the edge). Fall back to peer.
        return self.headers.get("CF-Connecting-IP") or self.client_address[0]

    def _local_only(self):
        # Same non-spoofable signal as _client_id: the header's presence means
        # this request came through cloudflared, i.e. the public tunnel. Gates
        # the media-library delete — reachable locally, never over the internet.
        return not self.headers.get("CF-Connecting-IP")

    def do_POST(self):
        if self.path == "/api/login":
            return self._login()
        if self.path == "/api/update":
            return self._update()
        if self.path == "/api/restart":
            return self._restart()
        if self.path == "/api/scan":
            return self._scan()
        if self.path == "/api/library/delete":
            return self._library_delete()
        if self.path == "/api/logout":
            return self._logout()
        return self._send(404, b"not found", "text/plain")

    def _login(self):
        """Basic user:pass -> a random session token. Throttled per client."""
        cid = self._client_id()
        if _login_blocked(cid):
            return self._send(429, b"too many attempts", "text/plain")
        user, pw = _parse_basic(self.headers.get("Authorization", ""))
        if not _check_login(user, pw):
            _login_fail(cid)
            return self._send(401, b"bad credentials", "text/plain")  # generic: user vs pass not revealed
        _login_reset(cid)
        tok = _new_session()
        self._send(200, json.dumps({"token": tok}).encode(), "application/json",
                   (("Cache-Control", "no-store"),
                    ("Set-Cookie", self._cookie(tok, _SESSION_TTL))))

    def _logout(self):
        tok = (self.headers.get("X-Session", "")
               or _cookie_token(self.headers.get("Cookie", "")))
        with _sessions_lock:
            _sessions.pop(tok, None)          # server-side too, not just the cookie
        self._send(200, b"ok", "text/plain",
                   (("Cache-Control", "no-store"), ("Set-Cookie", self._cookie("", 0))))

    def _update(self):
        if not self._authed():
            return self._send(401, b"no session", "text/plain")
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            return self._send(400, b"bad content-length", "text/plain")
        if n < 0:  # negative would skip the cap below and read() to EOF (unbounded)
            return self._send(400, b"bad content-length", "text/plain")
        if n > 64 * 1024:
            return self._send(413, b"body too large", "text/plain")
        try:
            body = json.loads(self.rfile.read(n))
        except Exception:
            return self._send(400, b"bad json", "text/plain")
        if not isinstance(body, dict):  # valid JSON but a scalar/list -> .get() would crash the thread
            return self._send(400, b"bad json", "text/plain")
        m = next((x for x in MACHINES if x["id"] == body.get("machine")), None)
        svc = next((s for s in (m["catalog"] if m else [])
                    if s["name"] == body.get("service") and "match" in s), None)
        if not svc:
            return self._send(404, b"unknown service", "text/plain")
        key = f'{m["id"]}/{svc["name"]}'
        with _inflight_lock:  # atomic check-and-add: two simultaneous POSTs can't both start
            busy = key in _inflight
            if not busy:
                _inflight.add(key)
        if busy:
            return self._send(409, b"update already running", "text/plain")
        jid = _start_update(m, svc, dry=bool(body.get("dry")))
        self._send(202, json.dumps({"job": jid}).encode(), "application/json",
                   (("Cache-Control", "no-store"),))

    def _svc_target(self, machine, service):
        """(svc, containers) for a LOCAL docker service, or (None, error tuple).

        Shared validation for /api/restart and /api/logs: the service must be a
        catalog entry with `match` on the machine netops runs on — remote boxes
        are read-only on purpose (the M1 ssh key allowlists read commands only).
        """
        m = next((x for x in MACHINES if x["id"] == machine), None)
        svc = next((s for s in (m["catalog"] if m else [])
                    if s["name"] == service and "match" in s), None)
        if not svc:
            return None, (404, b"unknown service")
        if m.get("ssh") is not None:
            return None, (400, b"remote not supported")
        names = _local_containers(svc)
        if not names:
            return None, (404, b"no containers matched")
        return svc, names

    def _restart(self):
        if not self._authed():
            return self._send(401, b"no session", "text/plain")
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            return self._send(400, b"bad content-length", "text/plain")
        if n < 0 or n > 64 * 1024:
            return self._send(400, b"bad content-length", "text/plain")
        try:
            body = json.loads(self.rfile.read(n))
        except Exception:
            return self._send(400, b"bad json", "text/plain")
        if not isinstance(body, dict):
            return self._send(400, b"bad json", "text/plain")
        svc, res = self._svc_target(body.get("machine"), body.get("service"))
        if svc is None:
            return self._send(res[0], res[1], "text/plain")
        key = f'{body.get("machine")}/{svc["name"]}'
        with _inflight_lock:   # shares the update guard: no restart mid-update
            busy = key in _inflight
            if not busy:
                _inflight.add(key)
        if busy:
            return self._send(409, b"operation already running", "text/plain")
        try:
            # 90s keeps the response inside Cloudflare's ~100s origin cutoff.
            # A CLI timeout only kills the docker CLIENT — dockerd finishes the
            # restart anyway — so report it as in-progress, not as a failure.
            rc, _, err = _run_rc(["docker", "restart", "-t", "15"] + res,
                                 timeout=90)
            if rc == 124:
                return self._send(202, json.dumps(
                    {"restarted": res, "note": "still finishing"}).encode(),
                    "application/json", (("Cache-Control", "no-store"),))
            if rc != 0:
                return self._send(500, (err.strip() or "restart failed")
                                  .encode()[:300], "text/plain")
            self._send(200, json.dumps({"restarted": res}).encode(),
                       "application/json", (("Cache-Control", "no-store"),))
        finally:
            with _inflight_lock:
                _inflight.discard(key)

    def _logs(self):
        if not self._authed():
            return self._send(401, b"no session", "text/plain")
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        svc, res = self._svc_target(q.get("machine", [""])[0],
                                    q.get("service", [""])[0])
        if svc is None:
            return self._send(res[0], res[1], "text/plain")
        try:
            lines = max(10, min(500, int(q.get("lines", ["120"])[0])))
        except ValueError:
            lines = 120
        out = []
        for name in res:
            rc, o, e = _run_rc(["docker", "logs", "--tail", str(lines),
                                "--timestamps", name], timeout=20)
            body = (o + e).strip() or "(no output)"
            out.append(f"────── {name} ──────\n{body}")
        blob = "\n\n".join(out).encode(errors="replace")[:200_000]
        self._send(200, blob, "text/plain; charset=utf-8",
                   (("Cache-Control", "no-store"),))

    def _scan(self):
        """Kick a ClamAV sweep from the board. The scan itself stays a systemd
        unit (idle priority, its own timeout, survives a netops restart) — this
        only presses its start button, which the sudoers drop-in from
        security_setup.sh allows for exactly these two units."""
        if not self._authed():
            return self._send(401, b"no session", "text/plain")
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            return self._send(400, b"bad content-length", "text/plain")
        if n < 0 or n > 64 * 1024:
            return self._send(400 if n < 0 else 413, b"bad body", "text/plain")
        try:
            body = json.loads(self.rfile.read(n)) if n else {}
        except Exception:
            return self._send(400, b"bad json", "text/plain")
        if not isinstance(body, dict):
            return self._send(400, b"bad json", "text/plain")
        units = ("netops-clamscan.service", "netops-clamscan-deep.service")
        if any(_unit_active(u) in ("active", "activating") for u in units):
            return self._send(409, b"scan already running", "text/plain")
        unit = units[1] if body.get("deep") else units[0]
        rc, _, err = _run_rc(["sudo", "-n", "/usr/bin/systemctl", "start",
                              "--no-block", unit], timeout=10)
        if rc != 0:
            return self._send(500, (err.strip() or "start failed").encode()[:300],
                              "text/plain")
        try:                      # so the board shows "running" on the next poll
            _sec["v"] = _security_check()
        except Exception:
            pass
        self._send(202, b'{"ok":true}', "application/json",
                   (("Cache-Control", "no-store"),))

    def _library_delete(self):
        """Delete one movie/series (Radarr/Sonarr) AND its qBittorrent
        hardlink twin if one is found — see the module-level comment on
        _find_hardlink_twins for why the second half matters. Local-network
        only: this never runs for a request that arrived through the public
        tunnel, session or not."""
        if not self._local_only():
            return self._send(403, b"local network only", "text/plain")
        if not self._authed():
            return self._send(401, b"no session", "text/plain")
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            return self._send(400, b"bad content-length", "text/plain")
        if n < 0 or n > 64 * 1024:
            return self._send(400, b"bad content-length", "text/plain")
        try:
            body = json.loads(self.rfile.read(n))
        except Exception:
            return self._send(400, b"bad json", "text/plain")
        if not isinstance(body, dict):
            return self._send(400, b"bad json", "text/plain")
        app, item_id = body.get("app"), body.get("id")
        if app not in _ARR_APPS or not isinstance(item_id, int) or isinstance(item_id, bool):
            return self._send(400, b"bad app/id", "text/plain")
        if not _arr_conf(app):
            return self._send(404, b"not configured", "text/plain")
        key = f"lib/{app}/{item_id}"
        with _inflight_lock:
            busy = key in _inflight
            if not busy:
                _inflight.add(key)
        if busy:
            return self._send(409, b"already running", "text/plain")
        try:
            try:
                item = _arr_item(app, item_id)
            except Exception as e:
                return self._send(502, str(e).encode()[:300], "text/plain")
            inos = set()
            for p in item["paths"]:
                try:
                    st = os.stat(p)
                    inos.add((st.st_dev, st.st_ino))
                except OSError:
                    pass
            # Decide the matches BEFORE the destructive step. Deleting the
            # library file frees its inode, and the kernel reuses inode numbers,
            # so enumerating afterwards can match an unrelated file that has
            # since inherited the number - which we would then delete WITH its
            # data. Capture first, act second.
            twins = []
            if inos:
                try:
                    twins = sorted(_find_hardlink_twins(inos, _qbit_torrent_files()))
                except Exception:
                    twins = []
            try:
                _arr_delete(app, item_id)
            except Exception as e:
                return self._send(502, str(e).encode()[:300], "text/plain")
            cleaned = 0
            for h in twins:
                try:
                    _qbit_delete(h)
                    cleaned += 1
                except Exception:
                    pass  # the arr delete already succeeded; report what we did
            self._send(200, json.dumps({"deleted": True,
                                        "freed_bytes": item["size_bytes"],
                                        "torrents_cleaned": cleaned}).encode(),
                       "application/json", (("Cache-Control", "no-store"),))
        finally:
            with _inflight_lock:
                _inflight.discard(key)

    def log_message(self, *_):
        pass  # quiet


def _selftest():
    # --- money/mem formatting ---
    assert _parse_mem("215.2MiB") == int(215.2 * 1024**2)
    assert _parse_mem("0B") == 0
    assert _parse_mem("1.5GiB") == int(1.5 * 1024**3)
    assert _parse_mem("512KiB") == 512 * 1024
    assert _parse_mem("") == 0
    assert _fmt(0) == "0B"
    assert _fmt(int(454.8 * 1024**2)).endswith("M")
    assert _fmt(int(1.2 * 1024**3)).endswith("G")

    # --- service rollup ---
    assert _rollup([]) == "down"
    assert _rollup([{"run": True, "unhealthy": False}]) == "up"
    assert _rollup([{"run": True, "unhealthy": False},
                    {"run": False, "unhealthy": False}]) == "degraded"
    assert _rollup([{"run": True, "unhealthy": True}]) == "degraded"
    assert _rollup([{"run": False, "unhealthy": False}]) == "down"
    # clean one-shot exit (migration job) must not drag a healthy service down
    assert _rollup([{"run": True, "unhealthy": False},
                    {"run": False, "unhealthy": False, "done": True}]) == "up"

    # --- host status parsers (fed sample command output; no network) ---
    assert _parse_loadavg("{ 1.52 1.74 1.82 }") == 1.52
    assert _parse_loadavg("0.93 0.69 0.75 5/698 1191079") == 0.93  # linux /proc/loadavg
    assert _parse_loadavg("") == 0.0

    # --- cpu temp parsers: millidegree zones -> max °C; sensors fallback; None -> '—'
    assert _parse_thermal("27800\n52000\n") == 52
    assert _parse_thermal("45000") == 45
    assert _parse_thermal("") is None and _parse_thermal("garbage") is None
    assert _parse_sensors_temp("Package id 0:  +61.0°C  (high = +80.0°C, crit = +100.0°C)\ncore: +58.5°C") == 61
    assert _parse_sensors_temp("") is None and _parse_sensors_temp("no temps") is None

    # --- public URL up/down classification ---
    assert _http_ok(200) and _http_ok(302) and _http_ok(401) and _http_ok(403)
    # per-machine power series reload: rows align to the total-series timestamps,
    # unknown timestamps are dropped, absent machines read as None gaps
    assert _align_msamples([1.0, 2.0, 3.0],
                           [(1.0, "a", 5.0), (3.0, "a", 6.0), (2.0, "b", 7.0),
                            (9.9, "c", 1.0)]) == \
        {"a": [5.0, None, 6.0], "b": [None, 7.0, None]}
    assert _align_msamples([], []) == {}
    cf = _parse_cf_ingress(
        "tunnel: x\ningress:\n  - hostname: a.com\n    service: http://127.0.0.1:81\n"
        "  - hostname: b.com\n    service: http://localhost:82\n"
        "  - service: http_status:404\n")
    assert cf == {"hosts": [{"host": "a.com", "port": 81},
                            {"host": "b.com", "port": 82}], "n": 2}
    assert _parse_cf_ingress("") == {"hosts": [], "n": 0}
    # restart/logs container claiming uses the same prefix rule as the board
    assert _claimed(["affine_db", "kabala-web-1", "noxa", "n8n"],
                    ["affine_", "kabala"]) == ["affine_db", "kabala-web-1"]
    assert _claimed(["noxa"], []) == [] and _claimed([], ["x"]) == []
    # scan start/finish pushes: falsy->mode = started, mode->falsy = finished
    assert _scan_transition({}, {"running": "daily"})[0] == "Scan started"
    fin = _scan_transition({"running": "deep"},
                           {"scanned": 13895, "infected": 0, "age_s": 60})
    assert fin[0] == "Scan finished: clean" and "13,895" in fin[1]
    bad = _scan_transition({"running": "daily"},
                           {"scanned": 5, "infected": 2, "age_s": 30})
    assert bad[0] == "Scan finished: INFECTED" and bad[2] == "high"
    # scan died without writing: stale (or missing) result must not read clean
    died = _scan_transition({"running": "daily"},
                            {"scanned": 13895, "infected": 0, "age_s": 90000})
    assert died[0] == "Scan ended without a result" and died[2] == "high"
    assert _scan_transition({"running": "daily"},
                            {"scanned": 1})[0] == "Scan ended without a result"
    assert _scan_transition({"running": "daily"}, {"running": "daily"}) is None
    assert _scan_transition({}, {}) is None
    # fleet snapshot: offline machine masks its services; one combined push
    snap = _fleet_snapshot({"machines": [
        {"id": "a", "host": {"online": True}, "categories":
         [{"services": [{"name": "S1", "status": "up"}]}]},
        {"id": "b", "host": {"online": False}, "categories":
         [{"services": [{"name": "S2", "status": "down"}]}]}]})
    assert snap == {"m/a": "online", "s/a/S1": "up", "m/b": "offline"}
    note = _fleet_push({"s/a/Jellyfin": ("up", "down"),
                        "m/b": ("online", "offline"),
                        "s/a/Sonarr": ("down", "up")})
    assert note[0] == "Lab alert" and note[2] == "high"
    assert "Jellyfin down" in note[1] and "b machine offline" in note[1] \
        and "Sonarr back up" in note[1]
    assert _fleet_push({"s/a/X": ("down", "up")})[0] == "Recovered"
    assert _fleet_push({}) is None
    # local health: classifier states and their push wording
    hs = _health_states(20.0, 12, 5.0, 90.0, {"/media": False},
                        {"/music": 95}, {"Toshi": "watch"})
    assert hs == {"sys:load": "high", "sys:ram": "low", "sys:temp": "hot",
                  "mount:/media": "missing", "disk:/music": "full",
                  "smart:Toshi": "watch"}
    assert _health_states(1.0, 12, 50.0, 40.0, {"/media": True},
                          {"/media": 60}, {}) == \
        {"sys:load": "ok", "sys:ram": "ok", "sys:temp": "ok",
         "mount:/media": "mounted", "disk:/media": "ok"}
    note = _fleet_push({"mount:/media": ("mounted", "missing"),
                        "sys:load": ("ok", "high")})
    assert note[0] == "Lab alert" and "/media drive DISCONNECTED" in note[1] \
        and "system load high" in note[1]
    note = _fleet_push({"sys:temp": ("hot", "ok")})
    assert note[0] == "Recovered" and "system temp recovered" in note[1]
    note = _fleet_push({"smart:Toshi": ("watch", "replace_soon")})
    assert note[0] == "Lab warning" and "Toshi SMART replace_soon" in note[1]
    assert not _http_ok(0) and not _http_ok(404) and not _http_ok(502) and not _http_ok(500)

    sample_vm = ("Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
                 "Pages free:                          100000.\n"
                 "Pages active:                        200000.\n"
                 "Pages inactive:                      150000.\n"
                 "Pages wired down:                    120000.\n"
                 "Pages occupied by compressor:         90000.\n")
    assert _mem_used(sample_vm, 64 * 1024**3) == (200000 + 120000 + 90000) * 16384
    assert _mem_used(sample_vm, 1024) == 1024      # capped at total
    assert _mem_used("garbage", 100) == 0          # no header -> 0

    assert _parse_uptime("10:24  up 3 days, 14:22, 3 users, load averages: 1 2 3") == "3 days, 14:22"
    assert _parse_uptime("10:24  up 5 mins, 1 user, load averages: 0 0 0") == "5 mins"
    assert _parse_uptime("nonsense") == "?"

    sample_df = ("Filesystem     1024-blocks      Used Available Capacity iused ifree %iused  Mounted on\n"
                 "/dev/disk3s1s1   971350180  22000000 900000000     3%  500000   100    1%  /")
    assert _parse_df(sample_df) == 900000000 * 1024
    assert _parse_df("") == 0

    # --- update flag: an outdated image on a matched container flags the svc ---
    dstate = {"c1": {"run": True, "unhealthy": False, "mem": 0, "image": "x:latest"}}
    cat = [{"name": "S", "cat": "APPS", "match": ["c"]}]
    assert _collect_categories(cat, dstate, None, outdated={"x:latest"})[0][0]["services"][0]["update"] == ["x:latest"]
    assert not _collect_categories(cat, dstate, None)[0][0]["services"][0]["update"]

    # --- a port+match entry claims its containers: docker status/RAM win and
    # the container must NOT reappear in OTROS (the double-listing bug) ---
    dstate = {"c1": {"run": True, "unhealthy": False, "mem": 1024, "image": "x:latest"},
              "c2": {"run": True, "unhealthy": False, "mem": 2048, "image": "y:1"}}
    cat = [{"name": "S", "cat": "APPS", "port": 1, "match": ["c1"]}]
    cats, on, deg, dn, ram = _collect_categories(cat, dstate, None)
    byname = {c["name"]: c for c in cats}
    assert byname["APPS"]["services"][0]["status"] == "up"
    assert byname["APPS"]["services"][0]["mem"] == 1024
    otros = [s["name"] for s in byname.get("OTROS", {"services": []})["services"]]
    assert otros == ["c2"]  # claimed c1 must not double-list; unclaimed c2 still shows

    # --- state persistence round-trips + tolerates corruption (temp file) ---
    import tempfile
    global STATE_FILE
    _sf = STATE_FILE
    fd, STATE_FILE = tempfile.mkstemp(suffix=".json"); os.close(fd)
    _h, _u = dict(_history), dict(_updates)
    try:
        _history.clear(); _updates.clear()
        _history["tokyo/asta"] = [1, 0, 1, 1]
        _updates["mbp1234"] = {"docuseal/docuseal:latest"}
        _save_state()
        _history.clear(); _updates.clear()
        _load_state()
        assert _history["tokyo/asta"] == [1, 0, 1, 1]
        assert _updates["mbp1234"] == {"docuseal/docuseal:latest"}
        # every corrupt/wrong-shape file must start fresh, never raise (boot path)
        for bad in ("{not json", "[1,2,3]", "null", "5", '"hi"', "true"):
            with open(STATE_FILE, "w") as f:
                f.write(bad)
            _history.clear()
            _load_state()
            assert _history == {}
        # top-level dict but a truthy NON-dict / unhashable-nested history|updates
        # must also start fresh, never raise (the pre-bind crash-loop path)
        for bad in ('{"updates": "x"}', '{"updates": [1,2,3]}', '{"updates": 5}',
                    '{"updates": true}', '{"updates": {"m": [["x"]]}}',
                    '{"t": %d, "history": "s"}' % int(time.time()),
                    '{"t": %d, "history": [1,2,3]}' % int(time.time())):
            with open(STATE_FILE, "w") as f:
                f.write(bad)
            _history.clear(); _updates.clear()
            _load_state()  # must not raise
            assert _history == {}
        # a save older than the display window -> drop stale bars, keep update flags
        with open(STATE_FILE, "w") as f:
            json.dump({"t": 0, "history": {"tokyo/asta": [1, 1, 1]},
                       "updates": {"mbp1234": ["x:latest"]}}, f)
        _history.clear(); _updates.clear()
        _load_state()
        assert _history == {}                       # gap > window -> honest reset
        assert _updates["mbp1234"] == {"x:latest"}  # flags still restored
    finally:
        os.remove(STATE_FILE)
        STATE_FILE = _sf
        _history.clear(); _history.update(_h)
        _updates.clear(); _updates.update(_u)

    # --- auth: basic-parse, login check, sessions, throttle -------------------
    # Uses a THROWAWAY credential (overriding the module constants) so the real
    # password never appears in this committed source.
    global _AUTH_USER, _AUTH_SALT, _AUTH_HASH
    _au, _as, _ah = _AUTH_USER, _AUTH_SALT, _AUTH_HASH
    _fails_bak = dict(_login_fails); _sess_bak = dict(_sessions)
    try:
        _AUTH_USER = "u"
        _AUTH_SALT = b"testsalt"
        _AUTH_HASH = hashlib.pbkdf2_hmac("sha256", b"pw", _AUTH_SALT, _AUTH_ITERS)
        # parse: scheme case-insensitive, password may contain ':', junk -> (None,None)
        assert _parse_basic("Basic " + base64.b64encode(b"u:pw").decode()) == ("u", "pw")
        assert _parse_basic("basic " + base64.b64encode(b"u:p:w").decode()) == ("u", "p:w")
        assert _parse_basic("Bearer x") == (None, None)
        assert _parse_basic("garbage") == (None, None)
        assert _parse_basic("Basic !!not-base64!!") == (None, None)
        assert _parse_basic("") == (None, None)
        # login check: right creds pass; wrong user OR wrong pass OR empty fail
        assert _check_login("u", "pw")
        assert not _check_login("u", "nope")
        assert not _check_login("x", "pw")
        assert not _check_login("", "") and not _check_login("u", "")
        # sessions: fresh token validates; unknown/empty/expired do not
        tok = _new_session()
        assert _session_valid(tok)
        assert not _session_valid("nope") and not _session_valid("")
        with _sessions_lock:
            _sessions[tok] = time.monotonic() - 1   # force-expire
        assert not _session_valid(tok)              # expired -> rejected + purged
        assert tok not in _sessions
        # throttle: locks out after _LOGIN_MAX fails, forgives after the window
        _login_fails.clear()
        for _ in range(_LOGIN_MAX):
            assert not _login_blocked("1.2.3.4")
            _login_fail("1.2.3.4")
        assert _login_blocked("1.2.3.4")
        _login_reset("1.2.3.4")
        assert not _login_blocked("1.2.3.4")
    finally:
        _AUTH_USER, _AUTH_SALT, _AUTH_HASH = _au, _as, _ah
        _login_fails.clear(); _login_fails.update(_fails_bak)
        _sessions.clear(); _sessions.update(_sess_bak)

    # --- unreachable machine degrades gracefully (stub _run, no real ssh) ---
    global _run
    real_run, _run = _run, lambda *a, **k: ""   # every command "fails" -> host down
    try:
        off = _build_machine({"id": "x", "name": "N", "role": "R",
                              "ssh": "nobody@example", "catalog": []})
    finally:
        _run = real_run
    assert off["host"] == {"online": False}
    assert off["categories"] == [] and off["summary"]["count"] == 0

    # --- recorder: an unreachable machine's catalog services get honest 0s
    # (bars must not freeze on stale green); served hist[] is a snapshot copy
    global get_data, MACHINES
    real_get, real_mach = get_data, MACHINES
    _h2 = dict(_history)
    try:
        MACHINES = [
            {"id": "off", "name": "O", "role": "", "ssh": None,
             "catalog": [{"name": "s1", "cat": "APPS", "port": 1}]},
            {"id": "on", "name": "N", "role": "", "ssh": None,
             "catalog": [{"name": "s2", "cat": "APPS", "port": 2}]},
        ]
        get_data = lambda: {"machines": [
            {"id": "off", "host": {"online": False}, "categories": []},
            {"id": "on", "host": {"online": True}, "categories": [
                {"name": "APPS", "services": [{"name": "s2", "status": "up"}]}]},
        ]}
        _history.clear()
        _record()
        assert _history["off/s1"] == [0]   # offline -> 0, not frozen
        assert _history["on/s2"] == [1]
        hist = _with_history(get_data())["machines"][1]["categories"][0]["services"][0]["hist"]
        hist.append(0)
        assert _history["on/s2"] == [1]    # snapshot copy, not the live ring
    finally:
        get_data, MACHINES = real_get, real_mach
        _history.clear(); _history.update(_h2)

    # --- SMART panel: read from the file at serve time, keyed by hostname ---
    assert _ago(3) == "hace unos segundos"
    assert _ago(600) == "hace 10 min"
    assert _ago(3 * 3600) == "hace 3 h"
    assert _ago(4 * 86400) == "hace 4 d"
    assert _ago(-5) == "hace unos segundos"      # clock skew must not print '-1 d'
    global SMART_FILE
    real_sf, _tmp = SMART_FILE, SMART_FILE + f".selftest{os.getpid()}"
    try:
        # Uses MACHINES[0]'s real id/name (not a hardcoded one) so this passes
        # whether config.json (real deployment) or the demo fallback is loaded.
        _mid0, _mname0 = MACHINES[0]["id"], MACHINES[0]["name"]
        doc = {"hosts": {
            _mid0: {"t": time.time(), "drives": [
                {"dev": "sdc", "name": "Toshiba USB HDD", "verdict": "watch"}]},
            "macbook": {"t": time.time() - 9 * 3600, "drives": [{"dev": "disk0"}]},
            "empty": {"t": time.time(), "drives": []},   # reported, but no disks
            "junk": "not a dict"}}
        os.makedirs(os.path.dirname(_tmp), exist_ok=True)
        with open(_tmp, "w") as f:
            json.dump(doc, f)
        SMART_FILE, _smart_cache["mtime"] = _tmp, None
        blocks = _smart_blocks()
        assert sorted(b["mid"] for b in blocks) == sorted([_mid0, "macbook"])  # junk/empty dropped
        by_mid = {b["mid"]: b for b in blocks}
        # a hostname that matches a machine borrows its label; an unknown one
        # still renders under its own name (future MacBook / build Mac)
        assert by_mid[_mid0]["name"] == _mname0
        assert by_mid["macbook"]["name"] == "MACBOOK"
        assert by_mid[_mid0]["drives"][0]["verdict"] == "watch"
        assert not by_mid[_mid0]["stale"] and by_mid["macbook"]["stale"]  # >6h -> amber stamp
        with open(_tmp, "w") as f:
            f.write("{ truncated")          # mid-write file must not raise
        _smart_cache["mtime"] = None
        assert _smart_blocks() == []
        os.unlink(_tmp)
        _smart_cache["mtime"] = None
        assert _smart_blocks() == []        # missing file -> no panel, no crash
    finally:
        SMART_FILE, _smart_cache["mtime"] = real_sf, None
        if os.path.exists(_tmp):
            os.unlink(_tmp)

    # --- security scan classification (screen 4) ---
    _n = 1_700_000_000
    assert _scan_status(None, _n) == {"status": "down", "age_s": None, "never": True}
    assert _scan_status({"ts": "soon"}, _n)["never"] is True     # corrupt file
    _ok = _scan_status({"ts": _n - 3600, "scanned": 42, "infected": 0}, _n)
    assert _ok["status"] == "up" and _ok["age_s"] == 3600
    _bad = _scan_status({"ts": _n - 60, "infected": 2, "hits": ["a", "b"]}, _n)
    assert _bad["status"] == "down" and _bad["hits"] == ["a", "b"]  # malware = red
    assert _scan_status({"ts": _n - 40 * 3600}, _n)["status"] == "degraded"  # missed daily
    assert _scan_status({"ts": _n - 9 * 86400}, _n)["status"] == "down"      # week+ gap
    assert _scan_status({"ts": _n - 3600, "err": "no db"}, _n)["status"] == "degraded"

    # --- firewall ruleset digest (screen 4 hero + surface verdicts) ---
    _nft = (
        "table inet noxafw {\n"
        "\tset lan_tcp {\n\t\ttype inet_service\n"
        "\t\telements = { 3001, 8096 }\n\t}\n"
        "\tset watched_tcp {\n\t\ttype inet_service\n"
        "\t\telements = { 8080, 9696 }\n\t}\n"
        "\tchain input {\n"
        "\t\ttype filter hook input priority filter; policy drop;\n"
        "\t\tct state established,related accept\n"
        "\t\tudp dport 41641 accept\n"
        "\t\tip saddr 192.168.50.0/24 tcp dport 22 accept\n"
        "\t\ttcp dport 6881 accept\n"
        "\t\tiifname \"eno1\" ip saddr 192.168.50.0/24 tcp dport @lan_tcp accept\n"
        "\t\ttcp dport @watched_tcp counter packets 5 bytes 300 comment \"denied: admin/unauth surface\"\n"
        "\t\tcounter packets 42 bytes 999 comment \"packets denied by default policy\"\n"
        "\t}\n}\n")
    _p = _parse_nft_table(_nft)
    assert _p["policy"] == "drop" and _p["chains"] == 1
    assert _p["rules"] == 7            # set internals + hook header not counted
    assert _p["blocked"] == 42 and _p["probes"] == 5
    assert _p["accepts"][22] == "lan" and _p["accepts"][6881] == "open"
    assert _p["accepts"][3001] == "lan" and _p["accepts"][41641] == "open"
    assert _p["watched"] == {8080, 9696}  # the set no accept rule ever uses
    assert {s["name"]: s["ports"] for s in _p["sets"]} == \
        {"lan_tcp": 2, "watched_tcp": 2}

    # --- listening-socket classification (surface card) ---
    _ss = (
        'tcp   LISTEN 0 4096   0.0.0.0:22    0.0.0.0:*\n'
        'tcp   LISTEN 0 4096   *:8096        *:*    users:(("jellyfin",pid=1,fd=5))\n'
        'tcp   LISTEN 0 4096   0.0.0.0:9696  0.0.0.0:*\n'
        'tcp   LISTEN 0 4096   0.0.0.0:8055  0.0.0.0:*\n'
        'udp   UNCONN 0 0      0.0.0.0:6881  0.0.0.0:*\n'
        'tcp   LISTEN 0 4096   127.0.0.1:5432 0.0.0.0:*\n'
        'tcp   LISTEN 0 4096   100.64.1.2%tailscale0:6881 0.0.0.0:*\n'
        'tcp   LISTEN 0 4096   172.18.0.1:6881 0.0.0.0:*\n'
        'udp   UNCONN 0 0      192.168.50.169:68 0.0.0.0:*\n')
    _c = _classify_ports(_ss, {22: "lan", 6881: "open", 8096: "lan"},
                         {9696}, {8096: "Jellyfin"})
    assert _c["public"] == 5 and _c["more"] == 0
    assert _c["local"] == 1 and _c["tail"] == 1 and _c["docker"] == 1 and _c["lan"] == 1
    assert [r["port"] for r in _c["rows"]] == [6881, 22, 8096, 8055, 9696]
    assert _c["rows"][0]["fw"] == "open" and _c["rows"][2]["name"] == "Jellyfin"
    assert _c["rows"][3]["fw"] == "policy" and _c["rows"][4]["fw"] == "blocked"

    # --- push-notification transitions (one push per CHANGE, never per tick) ---
    _ok = {"alert": None}
    _bad = {"alert": "down", "scan": {"status": "down", "infected": 2}}
    assert _alert_transition(None, _ok) is None        # boot into a green state
    assert _alert_transition(_ok, _ok) is None
    _n = _alert_transition(_ok, _bad)
    assert _n and _n[2] == "high" and "2 infected" in _n[1]
    assert _alert_transition(_bad, dict(_bad)) is None  # same bad state: silent
    _n = _alert_transition(_bad, _ok)
    assert _n and "all clear" in _n[0]                  # recovery announces itself
    _w = {"alert": "degraded", "updates": {"status": "degraded"}}
    _n = _alert_transition(_bad, _w)
    assert _n and _n[2] == "default" and "patching" in _n[1]
    assert _alert_transition(None, _bad) is not None    # restart mid-incident

    # --- /api/alerts: current-problems snapshot + the event log it reads ---
    assert _nice_key("m/homeserver") == "homeserver machine"
    assert _nice_key("s/homeserver/qbittorrent") == "qbittorrent"
    assert _nice_key("mount:/media") == "/media drive"
    assert _nice_key("disk:/") == "/ space"
    assert _fleet_problems(None) == []
    _fp = _fleet_problems({"m/homeserver": "offline", "s/homeserver/x": "up",
                           "disk:/": "full", "sys:load": "ok"})
    assert _fp == ["/ space almost full", "homeserver machine offline"]
    global _events
    _e_save = _events
    try:
        _events = []
        _log_event("t1", "b1", "high", "warning")
        assert len(_events) == 1 and _events[0]["title"] == "t1"
        for i in range(EVENTS_LEN + 5):        # ring buffer caps at EVENTS_LEN
            _log_event(f"t{i}", "b", "default", "")
        assert len(_events) == EVENTS_LEN
        assert _events[-1]["title"] == f"t{EVENTS_LEN + 4}"   # newest kept
    finally:
        _events = _e_save

    # --- electricity tariff: auto-detect (timezone) with a config.json override ---
    assert set(_TZ_COUNTRY.values()) <= set(_COUNTRY_TARIFFS)  # no dangling country code
    global _CFG
    _cfg_save = _CFG
    try:
        _CFG = {"power": {"kwh_price": 0.5, "currency": "€", "tariff_note": "manual"}}
        assert _tariff() == (0.5, "€", "manual", False)
        _CFG = {"power": {"kwh_price": 0.9}}          # currency/note default when omitted
        price, cur, note, auto = _tariff()
        assert price == 0.9 and cur == "$" and auto is False
        _CFG = {}                                     # no override -> auto-detected
        price, cur, note, auto = _tariff()
        assert auto is True and price > 0 and cur and "auto-detected" in note
    finally:
        _CFG = _cfg_save
    # _cost()/_energy_stats() must scale with whatever KWH_PRICE actually loaded,
    # not a value pinned in the test (that's env-dependent by design)
    assert _cost(1000.0)["day"] == round(24 * KWH_PRICE, 2)
    assert _cost(None) is None

    # --- media library cleanup: config gating + the hardlink-matching logic ---
    # _CFG is forced here: these must not depend on whether the REAL config.json
    # happens to have an "arr" key (it does once the user configures the feature)
    _cfg_save = _CFG
    try:
        _CFG = {}
        assert _arr_conf("radarr") is None                 # no "arr" key -> disabled
        _CFG = {"arr": {"radarr": {"url": "http://127.0.0.1:7878", "api_key": "x"},
                        "sonarr": {"url": "http://127.0.0.1:8989"}}}
        assert _arr_conf("radarr") is not None
        assert _arr_conf("sonarr") is None                 # url without api_key -> unusable
    finally:
        _CFG = _cfg_save
    # file ids are (st_dev, st_ino): the same inode number on a DIFFERENT device
    # is a different file and must never match (h3 below)
    _cands = [("h1", "/d/a.mkv", (1, 100)),
              ("h2", "/d/b1.mkv", (1, 200)), ("h2", "/d/b2.mkv", (1, 201)),
              ("h3", "/music/c.mkv", (9, 100))]
    assert _find_hardlink_twins({(1, 100)}, _cands) == {"h1"}      # not h3
    assert _find_hardlink_twins({(1, 200)}, _cands) == set()       # pack only half-matched
    assert _find_hardlink_twins({(1, 200), (1, 201)}, _cands) == {"h2"}   # whole pack
    assert _find_hardlink_twins({(9, 100)}, _cands) == {"h3"}
    assert _find_hardlink_twins({(1, 999)}, _cands) == set()
    assert _find_hardlink_twins(set(), _cands) == set()
    assert _find_hardlink_twins({(1, 100)}, []) == set()
    # an unstattable file (None) makes its torrent unclaimable, never claimable
    assert _find_hardlink_twins({(1, 100)},
                                [("h4", "/d/x", (1, 100)), ("h4", "/d/gone", None)]) == set()

    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
        sys.exit(0)
    _load_state()   # restore heartbeat bars + update chips from the last run
    _pwr_restore()  # refill the consumption chart so a restart does not blank it
    threading.Thread(target=_hist_loop, daemon=True).start()  # uptime history sampler
    threading.Thread(target=_upd_loop, daemon=True).start()   # image update sweeper
    threading.Thread(target=_speed_loop, daemon=True).start() # hourly link speed
    threading.Thread(target=_energy_loop, daemon=True).start()# watt-hour ledger
    threading.Thread(target=_sec_loop, daemon=True).start()   # security posture
    threading.Thread(target=_svc_loop, daemon=True).start()   # up/down pushes
    print(f"NETOPS board -> http://localhost:{PORT}  (Ctrl-C to stop)")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
