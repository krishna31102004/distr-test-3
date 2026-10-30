import sys
import time
import csv

from client import fan_out_query, load_machines
import client as client_module

DEFAULT_PERF_MACHINES_FILE = "perf_machines.txt"
DEFAULT_TRIALS = 5

CATEGORIES = {
    "rare": "zqrare",
    "infrequent": "zqmedium",
    "frequent": "zqbulk",
}


def run_trials(pattern, trials):
    latencies = []
    for trial_num in range(1, trials + 1):
        start = time.time()
        results = fan_out_query(pattern)
        elapsed = time.time() - start

        failed = [r for r in results if not r["success"]]
        if failed:
            print(f"  WARNING trial {trial_num}: "
                  f"{len(failed)} machine(s) failed to respond: "
                  f"{[f['host'] + ':' + str(f['port']) for f in failed]}")

        total_matches = sum(r.get("count", 0) for r in results if r["success"])
        latencies.append(elapsed)
        print(f"  trial {trial_num}: {elapsed:.4f}s "
              f"({total_matches} total matches, "
              f"{len(results) - len(failed)}/{len(results)} machines responded)")
    return latencies


def main():
    perf_machines_file = sys.argv[1] if len(sys.argv) >= 2 else DEFAULT_PERF_MACHINES_FILE
    trials = int(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_TRIALS

    client_module.CONFIG_FILE = perf_machines_file

    machines = load_machines(perf_machines_file)
    print(f"Using {len(machines)} machines from {perf_machines_file}: {machines}")
    print(f"Running {trials} trials per category\n")

    all_rows = []

    for category, pattern in CATEGORIES.items():
        print(f"=== {category} (pattern: '{pattern}') ===")
        latencies = run_trials(pattern, trials)
        for i, lat in enumerate(latencies, start=1):
            all_rows.append((category, i, lat))
        print()

    with open("perf_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["category", "trial", "latency_seconds"])
        writer.writerows(all_rows)

    print("Raw results written to perf_results.csv")


if __name__ == "__main__":
    main()