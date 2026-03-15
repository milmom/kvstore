#!/usr/bin/env python3
import sys
import os

DB_FILE = "data.db"

# In-memory index (NO dict/map)
keys = []
vals = []


def find_key_index(k: str) -> int:
    """Return index of key k in keys[], or -1 if not found."""
    for i in range(len(keys)):
        if keys[i] == k:
            return i
    return -1


def set_kv(k: str, v: str) -> None:
    """Set key to value in in-memory store (last write wins)."""
    idx = find_key_index(k)
    if idx == -1:
        keys.append(k)
        vals.append(v)
    else:
        vals[idx] = v


def get_kv(k: str):
    """Get value for key from in-memory store, or None if missing."""
    idx = find_key_index(k)
    if idx == -1:
        return None
    return vals[idx]


def replay_log() -> None:
    """Rebuild in-memory index by replaying append-only log."""
    if not os.path.exists(DB_FILE):
        return

    with open(DB_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            # Expect: SET <key> <value>
            parts = line.split(" ", 2)
            if len(parts) == 3 and parts[0] == "SET":
                _, k, v = parts
                set_kv(k, v)


def append_set_to_disk(k: str, v: str) -> None:
    """Append SET record to disk and fsync immediately."""
    with open(DB_FILE, "a", encoding="utf-8") as f:
        f.write(f"SET {k} {v}\n")
        f.flush()
        os.fsync(f.fileno())


def main() -> None:
    replay_log()

    # Read commands from stdin (works for piping / black-box testing)
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue

        parts = raw.split(" ", 2)
        cmd = parts[0].upper()

        if cmd == "EXIT":
            return

        if cmd == "SET":
            # SET <key> <value>
            if len(parts) < 3:
                print("ERROR", flush=True)
                continue

            k = parts[1]
            v = parts[2]

            append_set_to_disk(k, v)
            set_kv(k, v)

            print("OK", flush=True)
            continue

        if cmd == "GET":
            # GET <key>
            if len(parts) < 2:
                print("ERROR", flush=True)
                continue

            k = parts[1]
            v = get_kv(k)

            if v is None:
                print("NULL", flush=True)
            else:
                print(v, flush=True)
            continue

        # Unknown command
        print("ERROR", flush=True)


if __name__ == "__main__":
    main()
