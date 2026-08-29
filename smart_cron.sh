#!/bin/sh
# Hourly SMART refresh for the netops board (installed in `crontab -l`).
# Keeps its own log bounded so it can never grow without limit.
LOG=/home/tokyo/netops/data/smart_collect.log
{ date "+%Y-%m-%d %H:%M:%S"; /usr/bin/python3 /home/tokyo/netops/smart_collect.py; } >>"$LOG" 2>&1
tail -n 300 "$LOG" >"$LOG.tmp" && mv "$LOG.tmp" "$LOG"
