#!/bin/sh
# Hourly SMART refresh for the netops board (installed in `crontab -l`).
# Keeps its own log bounded so it can never grow without limit.
LOG=/home/tokyo/netops/data/smart_collect.log
# Local host first, then each remote pulled over ssh. Deliberately ONE chain in
# ONE cron slot: both writers do a read-modify-write on smart.json, and running
# them in separate slots lets a slow ssh pull overlap the local run, where the
# later os.replace silently drops the other host's key for an hour. (smart_pull
# also takes an flock, so a hand-run stays safe too.)
{ date "+%Y-%m-%d %H:%M:%S"
  /usr/bin/python3 /home/tokyo/netops/smart_collect.py
  /usr/bin/python3 /home/tokyo/netops/smart_pull.py macair
} >>"$LOG" 2>&1
tail -n 300 "$LOG" >"$LOG.tmp" && mv "$LOG.tmp" "$LOG"
