# netops

A live, multi-machine homelab dashboard. Single Python file, **standard
library only** — no dependencies to install, no build step. It watches
several machines (local or over SSH), shows per-service status (Docker
containers or native processes by port), host vitals (CPU/RAM/disk/temp),
and an optional security posture screen (firewall, antivirus, patch status,
network topology).

```
python3 server.py            -> http://localhost:8787
python3 server.py --selftest -> run the built-in test suite
```

## Contents

- [server.py](server.py) — the dashboard itself: HTTP server, data collection, HTML/CSS/JS frontend, all in one file
- [security_scan.py](security_scan.py) + [security_setup.sh](security_setup.sh) — optional ClamAV sweep + nftables/unattended-upgrades install feeding the SEGURIDAD screen
- [smart_collect.py](smart_collect.py) + [smart-image/](smart-image/) — optional SMART drive health collector (runs `smartctl` in a small Docker container, for hosts without root)
- [backup.py](backup.py) — example/reference implementation for snapshotting self-hosted app data (SQLite-safe); **not wired up by default**, adapt it to your own services
- [mac-netops-allow.sh](mac-netops-allow.sh) — example SSH forced-command script: lets netops poll a remote macOS box read-only, without giving the SSH key shell access
- [systemd/](systemd/) — unit files for running the scan/SMART jobs on a schedule

## Quick start

1. **Config.** Copy the example and edit it for your machines:
   ```
   cp config.example.json config.json
   ```
   `config.json` is gitignored — it holds your real hostnames, SSH targets,
   and login credentials, and is never committed. See [Configuration](#configuration) below.

2. **Auth.** Generate a login (there's no default password baked in — an
   unconfigured board falls back to `admin`/`admin`, which is fine on
   `localhost` but must be changed before exposing the board to anything
   else):
   ```
   python3 -c "import hashlib,os,json; s=os.urandom(16); print(json.dumps({'user':'you','salt_hex':s.hex(),'hash_hex':hashlib.pbkdf2_hmac('sha256',b'<newpass>',s,200000).hex(),'iters':200000}))"
   ```
   Paste the output under `"auth"` in `config.json`.

3. **Run it:**
   ```
   python3 server.py
   ```
   Open `http://localhost:8787`, log in with the credentials from step 2.

4. **Test it** (no server needed, no network calls):
   ```
   python3 server.py --selftest
   ```

## Configuration

Everything machine-specific lives in `config.json` (schema mirrored in
`config.example.json`):

```jsonc
{
  "title": "NETOPS // HOMELAB DASHBOARD",   // optional, shown in the header/tab title
  "machines": [
    {
      "id": "local",                        // short, unique, lowercase
      "name": "HOMESERVER",                 // display name
      "role": "APPS NODE",                  // display subtitle
      "ssh": null,                          // null = run on this machine; "user@host" = over ssh
      "os": "linux",                        // optional hint
      "catalog": [
        {
          "name": "My Web App",
          "cat": "APPS",                    // grouping heading on the board
          "match": ["my-web-app"],          // docker container name prefix(es) to sum together
          "url": "app.example.com"          // optional: public URL for an HTTP health check
        },
        {
          "name": "Some Native Service",
          "cat": "INFRA & DATOS",
          "port": 8080                      // native process: identified by its listening port
        }
      ]
    }
  ],
  "auth": {
    "user": "you",
    "salt_hex": "...", "hash_hex": "...", "iters": 200000
  }
}
```

- A service entry uses **either** `port` (native process, status/RAM read
  from whatever listens on that TCP port) **or** `match` (a list of Docker
  container name prefixes, summed into one row) — not both.
- `url` is optional and adds a public HTTP(S) reachability check on top of
  the local status check; omit it for anything not meant to be internet-facing
  (an internal `url` judged by a public check that 502s will misreport a
  perfectly healthy local service as down).
- Which machine is "local" (`ssh: null`) is auto-detected by matching the
  system hostname against a machine `id`, or forced with the `NETOPS_LOCAL`
  environment variable. There's deliberately no silent fallback — a wrong
  guess would probe the wrong machine's ports and read as permanently
  offline instead of failing loudly.
- If `config.json` is missing, absent, or has no `"machines"` key, the board
  falls back to a small built-in demo list so a fresh clone still runs.

Point at a different config file with `NETOPS_CONFIG=/path/to/config.json`.

## Authentication

The board is meant to be safe to put behind a public tunnel (Cloudflare
Tunnel, Tailscale Funnel, etc.): viewing anything beyond the login screen,
and every mutating endpoint (`/api/update`, `/api/restart`, `/api/scan`),
requires a session. The password is **never** stored in source or sent to
the browser after login — only a salted PBKDF2-SHA256 hash (200k iterations)
lives in `config.json`, and a login exchanges it once for a random,
short-lived (12h) session token. Failed logins are throttled per client
(8 / 5 min → 429) and don't reveal whether the username or password was
wrong.

Rotate credentials any time by regenerating the hash (see [Quick start](#quick-start),
step 2) and restarting.

## Optional components

**Security screen** — `security_setup.sh` (run once with `sudo`) installs an
nftables firewall table, ClamAV + freshclam, unattended-upgrades, and two
systemd timers that run `security_scan.py` (daily quick sweep, weekly deep
sweep). The board's SEGURIDAD screen reads their JSON output and the live
kernel firewall table — nothing here is required for the core dashboard to
work.

**SMART drive health** — `smart_collect.py` shells out to `smartctl` (via a
tiny Docker image in `smart-image/`, for hosts where installing
`smartmontools` directly isn't an option) and writes `data/smart.json`,
which the board polls per-request. Schedule it with `smart_cron.sh` (cron)
or `systemd/netops-smart.*` (systemd timer) — pick one, not both.

**Push notifications** — `server.py` can push state-change alerts (service
up/down, scan results, disk health) to an [ntfy](https://ntfy.sh) server,
optionally routed through an [n8n](https://n8n.io) webhook for
shaping/fan-out. Configure `data/ntfy.json` / `data/n8n-feed.json`
(gitignored — these hold live tokens) — see the `_ntfy_push` /
`_n8n_event` functions in `server.py` for the expected shape. This is
entirely optional; without those files, alerting is just silently skipped.

**Cloudflare Tunnel** — `cloudflared-config.example.yml` is a template for
exposing the board (and anything else) publicly without opening a port.
Copy it, fill in your tunnel ID and hostnames, and never commit the filled-in
version.

**backup.py** — a reference SQLite-safe backup implementation, deliberately
left disconnected from any scheduler. Adapt `SOURCES` to your own
self-hosted apps rather than relying on the paths in the docstring.

## Running as a service

The `systemd/` unit files assume the repo lives at a fixed path and runs as
a specific user — edit `ExecStart=`, `User=`, and the timer schedule for
your setup before installing them. `server.py` itself has no unit file
included here since deployment (systemd, screen, a container, launchd)
varies too much to template usefully; a minimal systemd service is:

```ini
[Unit]
Description=netops dashboard
After=network-online.target

[Service]
ExecStart=/usr/bin/python3 /path/to/netops/server.py
Restart=on-failure
User=youruser

[Install]
WantedBy=multi-user.target
```

## Development

`server.py`, `smart_collect.py`, and `backup.py` are self-testing —
`python3 <script>.py --selftest` runs pure in-process assertions, no network
calls, no live filesystem/Docker/SSH access. Run them after any change:

```
python3 server.py --selftest
python3 smart_collect.py --selftest
python3 backup.py --selftest
```

`security_scan.py` has **no** `--selftest` flag — running it invokes a real
ClamAV scan (`--deep` for the full weekly pass, no flag for the daily
recent-files pass). Don't run it ad hoc to "test" it; let the timers in
`systemd/` schedule it, or read it rather than execute it.

`server.py` edits only take effect after a restart of whatever process is
running it (systemd, foreground, etc.) — running `--selftest` does not
affect a live instance.

## Before you publish a fork of this

If you're starting from a clone of this repo to build your own homelab
board: see [PUBLISHING_GUIDE.md](PUBLISHING_GUIDE.md) for a checklist of
what to review before making *your* fork public, once it has your own real
machines/domains in it.

## License

MIT — see [LICENSE](LICENSE).
