#!/usr/bin/env python3
import re
import sys

# Captura números como 43, 43.24, 0.5 seguidos de "ms" (insensible a mayúsculas)
# Evita pegarse a palabras (p.ej., "x43msy" no matchea).
MS_PATTERN = re.compile(r'(?<!\w)(\d+(?:\.\d+)?)\s*ms\b', re.IGNORECASE)

def extract_ms(path):
    vals = []
    f = sys.stdin if path == "-" else open(path, "r", encoding="utf-8", errors="ignore")
    try:
        for line in f:
            for m in MS_PATTERN.finditer(line):
                vals.append(float(m.group(1)))
    finally:
        if f is not sys.stdin:
            f.close()
    return vals

def main():
    if len(sys.argv) < 2:
        print("Usage: python avg_time_ms.py <logfile or ->", file=sys.stderr)
        sys.exit(2)

    vals = extract_ms(sys.argv[1])
    if not vals:
        print("No '<number> ms' entries found.")
        sys.exit(1)

    total = sum(vals)
    avg = total / len(vals)
    print(f"Count: {len(vals)}")
    print(f"Total: {total:.3f} ms")
    print(f"Average: {avg:.3f} ms")

if __name__ == "__main__":
    main()
