#!/usr/bin/env python3
import re
import sys

pattern = re.compile(r'time\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*ms', re.IGNORECASE)

def extract_times(path):
    times = []
    # Read from file or stdin if path is "-"
    f = sys.stdin if path == "-" else open(path, "r", encoding="utf-8", errors="ignore")
    try:
        for line in f:
            for m in pattern.finditer(line):
                times.append(float(m.group(1)))
    finally:
        if f is not sys.stdin:
            f.close()
    return times

def main():
    if len(sys.argv) < 2:
        print("Usage: python avg_time_ms.py <logfile or ->", file=sys.stderr)
        sys.exit(2)

    times = extract_times(sys.argv[1])
    if not times:
        print("No 'time : ... ms' entries found.")
        sys.exit(1)

    avg = sum(times) / len(times)
    print(f"Count: {len(times)}")
    print(f"Average time: {avg:.3f} ms")

if __name__ == "__main__":
    main()
