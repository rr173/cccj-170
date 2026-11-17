#!/usr/bin/env python3
"""Run the complete test suite.  All processes use isolated interpreters."""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

SUITES = [
    ("process-driven crash/restart experiments",
     [sys.executable, "-I", os.path.join(HERE, "run_experiments.py")]),
    ("corruption, truncation and locking",
     [sys.executable, "-I", os.path.join(HERE, "test_corruption.py")]),
    ("randomized differential vs independent reference",
     [sys.executable, "-I", os.path.join(HERE, "test_random_diff.py"), "20"]),
    ("repeated crashes during recovery",
     [sys.executable, "-I", os.path.join(HERE, "test_recovery_recrash.py")]),
    ("online backup concurrency, cutoff and crash matrix",
     [sys.executable, "-I", os.path.join(HERE, "test_backup.py")]),
    ("online history reclamation (WAL + checkpoints)",
     [sys.executable, "-I", os.path.join(HERE, "test_reclaim.py")]),
]


def main():
    failed = []
    for title, cmd in SUITES:
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            failed.append(title)
    print("\n" + "#" * 70)
    if failed:
        print("FAILED SUITES:")
        for t in failed:
            print("  -", t)
        return 1
    print("ALL SUITES PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
