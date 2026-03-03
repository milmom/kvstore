#!/usr/bin/env python3
import sys
import os

DB_FILE = "data.db"

# In-memory index (NO dict/map)
keys = []
vals = []

def find_key_index(k: str) -> int:
    for i in range(len(keys)):
        if keys[i] == k:
            return i
    return -1

def set_kv(k: str, v: str) -> None:
    idx = find_key_index(k)
    if idx == -1:
        keys.append(k)
        vals.append(v)
    else:
        vals[idx] = v

def get_kv(k: str):
    idx = find_key_index(k)
    if idx == -1:
        return None
    return vals[idx]

def replay_log() -> None:
    if not os.path.exists(DB_FILE):
        return
    with open(DB_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(" ", 2)
            if len(parts) >= 3 and parts[0] == "SET":
                k = parts[1]
                v = parts[2]
                set_kv(k, v)

def append_set_to_disk(k: str, v: str) -> None:
    with open(DB_FILE, "a", encoding="utf-8") as f:
        f.write(f"SET {k} {v}\n")
        f.flush()
        os.fsync(f.fileno())

def main():
    replay_log()

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue

        parts = raw.split(" ", 2)
        cmd = parts[0].upper()

        if cmd == "EXIT":
            break

        if cmd == "SET":
            if len(parts) < 3:
                sys.stdout.write("ERROR\n")
                sys.stdout.flush()
                continue
            k = parts[1]
            v = parts[2]
            append_set_to_disk(k, v)
            set_kv(k, v)
            sys.stdout.write("OK\n")
            sys.stdout.flush()
            continue

        if cmd == "GET":
            if len(parts) < 2:
                sys.stdout.write("ERROR\n")
                sys.stdout.flush()
                continue
            k = parts[1]
            v = get_kv(k)
            if v is None:
                sys.stdout.write("NULL\n")
            else:
                sys.stdout.write(v + "\n")
            sys.stdout.flush()
            continue

        sys.stdout.write("ERROR\n")
        sys.stdout.flush()

if __name__ == "__main__":
    main()
