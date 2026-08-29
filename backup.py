#!/usr/bin/env python3
"""NETOPS backup — DEAD CODE on the Lenovo homeserver. DO NOT WIRE UP.

!! STATUS 2026-08-19: this script is NOT invoked by anything on this host —
!! no crontab, no systemd unit/timer, not called from server.py. Verified by
!! grep over /etc/systemd/system, /etc/cron*, and netops/. It is a leftover
!! from the Mac era of NETOPS and every path in it points at macOS locations
!! that do not exist here:
!!     SOURCES[vaultwarden].root  ~/.vaultwarden/self-host      -> ABSENT
!!                                (real data lives in /home/tokyo/vaultwarden-data)
!!     SOURCES[asta].root         ~/Library/Application Support/Asta/backend -> ABSENT
!!     BACKUP_DIR                 ~/Backups/netops               -> ABSENT
!!
!! It was left unfixed ON PURPOSE. Repointing only the vaultwarden root would
!! leave the `asta` source still broken (macOS-only path) and would create a
!! SECOND, competing vaultwarden backup implementation — a restore-time trap.
!! Vaultwarden, DocuSeal and Affine are now backed up for real by:
!!     /home/tokyo/noxa/noxa-backup.sh   (noxa-backup.timer, daily 03:23)
!!     -> /home/tokyo/backups/{noxa,vaultwarden,docuseal,affine}/
!!        mirrored to /media/backups/* when /media is mounted
!! Extend THAT script, not this one. If you ever revive this file, fix all
!! three paths above and add it to a timer — it does nothing until you do.

Consistent, timestamped snapshots of self-hosted data.

Run:      python3 backup.py            -> snapshot everything in SOURCES
          python3 backup.py --list     -> show existing backups
          python3 backup.py --selftest

Live SQLite databases (vaultwarden) are copied with SQLite's backup API, not
`cp`, so a snapshot taken while the container is running is never torn. No
downtime, stdlib only.

Off-site note: these archives contain SECRETS (vault DB, rsa_key, admin creds).
Do NOT push them anywhere off-box unencrypted. Add nextcloud/docuseal by
appending a dict to SOURCES once they exist.
"""
import glob
import os
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import time
from pathlib import Path

HOME = Path.home()
BACKUP_DIR = HOME / "Backups" / "netops"
KEEP = 14  # keep the newest N archives per source, prune the rest

# Each source: name, root dir, sqlite dbs to snapshot consistently, and dirs to
# skip (regenerable caches / scratch). Everything else under root is copied.
ASTA = HOME / "Library" / "Application Support" / "Asta" / "backend"
SOURCES = [
    {
        "name": "vaultwarden",
        "root": HOME / ".vaultwarden" / "self-host",
        "sqlite": ["data/db.sqlite3"],
        "skip": {"data/icon_cache", "data/tmp"},
    },
    {
        # asta's dir is 12G (.venv, playwright, TTS models — all regenerable), so
        # this is include-only: grab just the ~44M of live data + secrets.
        "name": "asta",
        "root": ASTA,
        "sqlite": ["asta.db", "rag_fts.db"],
        "include": [".env", "chroma_db",
                    "data/backup-models.json", "data/openrouter_catalog.json"],
    },
    # {"name": "nextcloud", "root": ..., "sqlite": [...], "skip": {...}},  # add when deployed
]


def _snapshot_sqlite(src_path, dst_path):
    """Consistent copy of a live SQLite db via the backup API (handles WAL)."""
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(dst_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def _copy(full, root, stage_dir):
    """Copy a file or dir (preserving its relative path) into the staging tree."""
    rel = os.path.relpath(full, root)
    dst = stage_dir / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    if os.path.isdir(full):
        shutil.copytree(full, dst, dirs_exist_ok=True)
    else:
        shutil.copy2(full, dst)


def _stage(source, stage_dir):
    """Stage source: sqlite dbs snapshotted; the rest by include-list or full walk-minus-skip."""
    root = Path(source["root"])
    sqlite_rel = set(source.get("sqlite", []))

    for db_rel in sqlite_rel:
        src = root / db_rel
        if src.exists():           # mode=ro won't create it; a missing db must not abort the run
            _snapshot_sqlite(src, stage_dir / db_rel)
        else:
            print(f"skip {source['name']}: {db_rel} not found")

    if "include" in source:  # include-only: grab just the listed paths (dir 12G, want 44M)
        for rel in source["include"]:
            full = root / rel
            if full.exists():
                _copy(str(full), root, stage_dir)
        return

    skip = {str(root / s) for s in source.get("skip", ())}
    drop = {str(root / db) + suf for db in sqlite_rel for suf in ("-wal", "-shm")}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if os.path.join(dirpath, d) not in skip]
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root)
            if rel in sqlite_rel or full in drop:
                continue  # snapshot already handled this db; skip live file + WAL/SHM
            _copy(full, root, stage_dir)


def _archive(source, stamp):
    root = Path(source["root"])
    if not root.exists():
        print(f"skip {source['name']}: {root} not found")
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    out = BACKUP_DIR / f"{source['name']}-{stamp}.tar.gz"
    with tempfile.TemporaryDirectory() as tmp:
        stage = Path(tmp) / source["name"]
        stage.mkdir()
        _stage(source, stage)
        with tarfile.open(out, "w:gz") as tar:
            tar.add(stage, arcname=source["name"])
    print(f"ok   {out.name}  ({out.stat().st_size/1024**2:.1f}M)")
    return out


def _prune(name):
    archives = sorted(glob.glob(str(BACKUP_DIR / f"{name}-*.tar.gz")))
    for old in archives[:-KEEP]:
        os.remove(old)
        print(f"prune {os.path.basename(old)}")


def run():
    stamp = time.strftime("%Y%m%d-%H%M%S")
    for source in SOURCES:
        if _archive(source, stamp):
            _prune(source["name"])


def _list():
    for f in sorted(glob.glob(str(BACKUP_DIR / "*.tar.gz"))):
        p = Path(f)
        print(f"{p.stat().st_size/1024**2:7.1f}M  {p.name}")


def _selftest():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        # a live db with an open WAL — the torn-copy hazard we guard against
        db = tmp / "live.sqlite3"
        con = sqlite3.connect(db)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE t(x)")
        con.executemany("INSERT INTO t VALUES(?)", [(i,) for i in range(100)])
        con.commit()  # rows in WAL, not yet checkpointed into the main file
        snap = tmp / "snap.sqlite3"
        _snapshot_sqlite(db, snap)
        con.close()
        got = sqlite3.connect(snap).execute("SELECT count(*) FROM t").fetchone()[0]
        assert got == 100, f"snapshot lost rows: {got}"

        # prune keeps newest KEEP
        global BACKUP_DIR
        BACKUP_DIR = tmp / "b"
        BACKUP_DIR.mkdir()
        for i in range(KEEP + 5):
            (BACKUP_DIR / f"svc-2026010{i:02d}.tar.gz").write_text("x")
        _prune("svc")
        left = glob.glob(str(BACKUP_DIR / "svc-*.tar.gz"))
        assert len(left) == KEEP, f"prune kept {len(left)}, want {KEEP}"

        # a configured sqlite db that is ABSENT must be skipped, not abort the run
        src_root = tmp / "src"
        src_root.mkdir()
        (src_root / "keep.txt").write_text("data")
        out = _archive({"name": "miss", "root": src_root, "sqlite": ["nope.db"]},
                       "20260101-000000")
        assert out and out.exists(), "missing db aborted the archive instead of skipping"
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    elif "--list" in sys.argv:
        _list()
    else:
        run()
