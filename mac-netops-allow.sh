#!/bin/sh
# netops-allow.sh - forced command for the netops status key (tokyo@homeserver).
#
# netops polls this Mac for read-only host and container status. It never needs a
# shell, a pty, forwarding, or any command that changes state. The key in
# ~/.ssh/authorized_keys is pinned to this script, so a stolen netops key buys an
# attacker exactly the commands below and nothing else.
#
# Matching: strip the PATH prefix netops always prepends, delete every digit (so
# ports and PIDs vary freely), SHA-256 the rest, compare against ALLOW.
#
# Updated for the modern view: adds `sysctl -n hw.ncpu`, needed to turn load
# average into a CPU percentage. Regenerate ALLOW whenever server.py's remote
# commands change, or the board will quietly show this machine offline.
set -u

c="${SSH_ORIGINAL_COMMAND-}"
if [ -z "$c" ]; then
  echo "netops-allow: interactive login is not permitted with this key" >&2
  exit 1
fi

pre='export PATH="$HOME/.orbstack/bin:$PATH"; '
case "$c" in
  "$pre"*) r=${c#"$pre"} ;;
  *)       r=$c ;;
esac

h=$(printf '%s' "$r" | tr -d '0-9' | shasum -a 256 | awk '{print $1}')

ALLOW='
0de5a9f983b2be8997314daf673a551ddb1e4f8dd0323a8fa1431bbb873bbe62
10d1bb387bd7bd35aa071fcc5907e20ec5413270ce40c8dfb57a5e02ed967399
124ead40b01cf08d41f925baf74f32c7be0b60e4ab837f9f1665d9fbffeca7f4
18fe868e36de791bd40937ada526c59d32f5b0b441a3074367794968cfce2b73
43bedb1e8c3f5e48445871c0073f815824c546091a080072716b08ebd7bfc7e5
985938dd1bc1c99cf0f7cab335c9011618b2af203ac6230be7118079122c9cd2
ade414deb13749a13c1cb906af03ae5934426af29fefe4b4356f1c119566e460
be730ae73df6f691e2df2a904df54b82a6d646b463729ae7f81e01d3b35c268e
dd291cd6294bafef2a7e9c378eb320e87198d6dae214272addb569775750c802
df99ab9ee97b3a5d7f4bba8ca29ac30707b6b1145223d99c44ba92d632d240de
ea23c03a42b3b2ea39eb61604b7c7776ee3ab2a4fcac108a5dd03ff0299996cc
'
for a in $ALLOW; do
  if [ "$h" = "$a" ]; then
    exec /bin/sh -c "$c"
  fi
done

echo "netops-allow: command not permitted" >&2
logger -t netops-allow "denied: $r" 2>/dev/null
exit 1
