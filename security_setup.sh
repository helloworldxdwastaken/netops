#!/usr/bin/env bash
# =============================================================================
# security_setup.sh — one-shot (idempotent, safe to re-run) install of the
# security stack the netops SEGURIDAD screen monitors.
#
#   RUN AS ROOT:   sudo bash /home/tokyo/netops/security_setup.sh
#
# Everything lands as an ENABLED systemd unit, so it all survives reboots:
#   1. nftables      start the already-enabled unit -> loads table inet noxafw.
#                    ExecStart is the atomic per-table load from
#                    /etc/nftables.conf; the packaged `flush ruleset` ExecStop
#                    is already overridden by 10-noxa-no-flush.conf, so this
#                    NEVER touches Docker's or Tailscale's chains.
#   2. sudoers       /etc/sudoers.d/netops-security — user tokyo may run
#                    exactly `nft list table inet noxafw` (read-only), so the
#                    board verifies the KERNEL table, not just the unit state.
#   3. clamav        scanner + clamav-freshclam signature updater (daemon).
#                    clamav-daemon (clamd) is deliberately NOT installed: it
#                    pins ~1.3 GB RAM around the clock for a box that scans
#                    once a day; clamscan loads the db per run instead.
#   4. unattended-upgrades   automatic Debian security patches via APT timers.
#   5. netops-clamscan(.timer)      daily 04:15 Mon-Sat, new files only
#      netops-clamscan-deep(.timer) Sun 05:00, full downloads+home+/etc
#                    both write data/clamscan.json for the board.
#
# Afterwards restart the board itself:  sudo systemctl restart netops
# =============================================================================
set -euo pipefail
NETOPS=/home/tokyo/netops
[ "$(id -u)" = 0 ] || { echo "run me with sudo: sudo bash $0" >&2; exit 1; }

echo "== 1/5 firewall: load inet noxafw =="
systemctl start nftables
nft list table inet noxafw >/dev/null
echo "   table inet noxafw is in the kernel"

echo "== 2/5 read-only sudo rule so the board can verify the kernel table =="
cat > /etc/sudoers.d/netops-security <<'EOF'
# netops board (server.py): exact commands only.
#   nft list        _security_check confirms the firewall table really is in
#                   the kernel (read-only).
#   systemctl start POST /api/scan's "scan now" button presses the start
#                   button on the two scan units — nothing else.
tokyo ALL=(root) NOPASSWD: /usr/sbin/nft list table inet noxafw
tokyo ALL=(root) NOPASSWD: /usr/bin/systemctl start --no-block netops-clamscan.service
tokyo ALL=(root) NOPASSWD: /usr/bin/systemctl start --no-block netops-clamscan-deep.service
EOF
chmod 0440 /etc/sudoers.d/netops-security
visudo -cf /etc/sudoers.d/netops-security || { rm -f /etc/sudoers.d/netops-security; exit 1; }

echo "== 3/5 clamav + freshclam =="
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq clamav clamav-freshclam unattended-upgrades
systemctl enable --now clamav-freshclam
echo "   freshclam will fetch its first signature db in the background (~5 min)"

echo "== 4/5 unattended-upgrades =="
cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF
systemctl enable --now apt-daily.timer apt-daily-upgrade.timer

echo "== 5/5 scan timers =="
cp "$NETOPS"/systemd/netops-clamscan.service "$NETOPS"/systemd/netops-clamscan.timer \
   "$NETOPS"/systemd/netops-clamscan-deep.service "$NETOPS"/systemd/netops-clamscan-deep.timer \
   /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now netops-clamscan.timer netops-clamscan-deep.timer
# First sweep right now, in the background; security_scan.py itself waits (up
# to 1 h) for freshclam's first signature db before scanning.
systemctl start --no-block netops-clamscan.service

echo
echo "== resulting state =="
for u in nftables clamav-freshclam apt-daily.timer apt-daily-upgrade.timer \
         netops-clamscan.timer netops-clamscan-deep.timer; do
  printf '   %-28s %s\n' "$u" "$(systemctl is-active "$u" || true)"
done
if sudo -u tokyo sudo -n /usr/sbin/nft list table inet noxafw >/dev/null 2>&1; then
  echo "   board kernel-table check      OK"
else
  echo "   board kernel-table check      FAILED (sudoers rule?)" >&2
fi
if sudo -u tokyo sudo -n -l /usr/bin/systemctl start --no-block netops-clamscan.service >/dev/null 2>&1; then
  echo "   board scan-button rule        OK"
else
  echo "   board scan-button rule        FAILED (sudoers rule?)" >&2
fi
echo
echo "done — now:  sudo systemctl restart netops   (picks up the SEGURIDAD screen)"
