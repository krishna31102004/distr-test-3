import sys
import random

TARGET_BYTES = 60 * 1024 * 1024
TARGET_LINES = 750000
_FILLER_PREFIX = "{i} normal log entry, nothing special here -- "
_FILLER_PAD_CHAR = "."

PATTERN_COUNTS = {
    "zqrare": 5,
    "zqmedium": 30000,
    "zqbulk": 150000,
}


def build_lines(target_bytes, target_lines, pattern_counts):
    lines = []

    for pattern, count in pattern_counts.items():
        for i in range(count):
            lines.append(f"{i} {pattern}: something went wrong")

    known_count = len(lines)
    filler_count = max(target_lines - known_count, 0)

    known_bytes = sum(len(l.encode()) + 1 for l in lines)
    remaining_bytes = max(target_bytes - known_bytes, 0)

    if filler_count > 0:
        target_line_bytes = max(remaining_bytes // filler_count, 1)
    else:
        target_line_bytes = 1

    for i in range(filler_count):
        prefix = _FILLER_PREFIX.format(i=i)
        pad_needed = target_line_bytes - len(prefix.encode()) - 1
        pad_needed = max(pad_needed, 0)
        line = prefix + (_FILLER_PAD_CHAR * pad_needed)
        lines.append(line)

    random.shuffle(lines)
    return lines


def main():
    if len(sys.argv) not in (2, 3):
        print("Usage: python3 generate_perf_logs.py <vm_number> [output_dir]")
        sys.exit(1)

    vm_number = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) == 3 else "."
    filename = f"{out_dir}/machine.{vm_number}.log"

    lines = build_lines(TARGET_BYTES, TARGET_LINES, PATTERN_COUNTS)

    with open(filename, "w") as f:
        f.write("\n".join(lines) + "\n")

    import os
    size_mb = os.path.getsize(filename) / (1024 * 1024)
    print(f"Wrote {filename}: {len(lines)} lines, {size_mb:.1f} MB")
    for pattern, count in PATTERN_COUNTS.items():
        print(f"  pattern '{pattern}': {count} occurrences")


if __name__ == "__main__":
    main()