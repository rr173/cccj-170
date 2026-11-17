"""Stable fault-injection points for crash and restart experiments.

Faults are controlled externally through marker files so the test harness
never has to reach into engine internals::

    <store>/faults/<POINT>.fault   -> JSON {"mode": "exit", "code": 99}

When the engine reaches ``POINT`` with an armed marker it performs a
hard process exit via ``os._exit`` - no Python ``finally`` blocks, no clean
close - which is the supported crash model.  Each marker is consumed at
most once (renamed to ``.fired``) unless it is marked ``repeat``, so a
recovery can be crashed at exactly one phase and the next open completes.

Stable point names (part of the public test contract):

* ``commit_before_decision_fsync`` - log decision framed, not yet synced
* ``commit_after_decision_fsync``  - durable decision, reply not sent
* ``abort_before_decision_fsync``
* ``checkpoint_before_publish``    - complete snapshot built, not published
* ``checkpoint_during_switch``     - staged file promoted, current not switched
* ``recovery_after_record_scan``   - WAL analyzed, no pages touched
* ``recovery_mid_cleanup``         - some transactions resolved, some not
* ``recovery_mid_page_write``      - some affected pages rewritten
* ``backup_after_barrier``         - cutoff checkpoint published, no file
                                     of the copy has been read yet
* ``backup_mid_page_copy``         - at least one page copied, more remain
* ``backup_after_page_copy``       - all pages copied, before the WAL copy
* ``backup_after_wal_copy``        - cutoff-prefix WAL copied, before verify
* ``backup_before_publish``        - fully verified staging, not renamed
* ``backup_after_publish``         - target renamed (valid backup exists),
                                     before the backup call returns
"""

import json
import os
import time

POINTS = (
    "commit_before_decision_fsync",
    "commit_after_decision_fsync",
    "abort_before_decision_fsync",
    "checkpoint_before_publish",
    "checkpoint_during_switch",
    "recovery_after_record_scan",
    "recovery_mid_cleanup",
    "recovery_mid_page_write",
    "backup_after_barrier",
    "backup_mid_page_copy",
    "backup_after_page_copy",
    "backup_after_wal_copy",
    "backup_before_publish",
    "backup_after_publish",
)
DEFAULT_CODE = 99


class FaultInjector:
    def __init__(self, faults_dir: str, enabled: bool = True):
        self.dir = faults_dir
        self.enabled = enabled

    def arm(self, point: str, code: int = DEFAULT_CODE, repeat: bool = False,
            mode: str = "exit"):
        if point not in POINTS:
            raise KeyError(f"unknown fault point {point}")
        if mode not in ("exit", "block", "event"):
            raise ValueError(f"unknown fault mode {mode}")
        os.makedirs(self.dir, exist_ok=True)
        path = os.path.join(self.dir, point + ".fault")
        with open(path, "w") as f:
            json.dump({"mode": mode, "code": code, "repeat": repeat}, f)
            f.flush()
            os.fsync(f.fileno())
        _fsync_dir(self.dir)

    def arm_block(self, point: str):
        """At this point the thread waits until a release marker appears."""
        self.arm(point, mode="block")

    def arm_event(self, point: str):
        """At this point touch a one-shot event file, then keep running."""
        self.arm(point, mode="event")

    def disarm(self, point: str):
        path = os.path.join(self.dir, point + ".fault")
        if os.path.exists(path):
            os.remove(path)
            _fsync_dir(self.dir)

    @staticmethod
    def armed_points(store_dir: str):
        d = os.path.join(store_dir, "faults")
        if not os.path.isdir(d):
            return []
        return sorted(n[:-len(".fault")] for n in os.listdir(d)
                      if n.endswith(".fault"))

    def release_block(self, point: str):
        """Allow a thread parked in ``block`` mode at ``point`` to proceed."""
        rel = os.path.join(self.dir, point + ".release")
        with open(rel, "w") as f:
            f.write("go")
            f.flush()
            os.fsync(f.fileno())
        _fsync_dir(self.dir)

    def block_reached(self, point: str):
        return os.path.exists(os.path.join(self.dir, point + ".waiting"))

    def event_fired(self, point: str):
        return os.path.exists(os.path.join(self.dir, point + ".event"))

    def reach(self, point: str):
        """Called by the engine at a stable point.

        ``exit`` mode hard-kills the process (crash model).  ``block`` mode
        parks the calling thread until an external release marker appears,
        giving tests a deterministic interleave seam.  ``event`` mode touches
        a marker file once and continues, letting a driver observe that the
        engine truly reached the point without stopping it.
        """
        if not self.enabled:
            return
        path = os.path.join(self.dir, point + ".fault")
        if not os.path.exists(path):
            return
        with open(path) as f:
            try:
                cfg = json.load(f)
            except json.JSONDecodeError:
                cfg = {}
        mode = cfg.get("mode", "exit")
        code = int(cfg.get("code", DEFAULT_CODE))

        if mode == "block":
            # Publish a durable "reached" signal, then park.  Block faults
            # are consumed by the explicit release rather than self-removing.
            waiting = os.path.join(self.dir, point + ".waiting")
            with open(waiting, "w") as f:
                f.write(str(os.getpid()))
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(self.dir)
            rel = os.path.join(self.dir, point + ".release")
            for _ in range(6000):           # bounded wait (~10 min)
                if os.path.exists(rel):
                    break
                time.sleep(0.1)
            else:
                raise TimeoutError(f"block fault {point} never released")
            for p in (waiting, rel, path):
                try:
                    os.remove(p)
                except OSError:
                    pass
            _fsync_dir(self.dir)
            return

        if mode == "event":
            ev = os.path.join(self.dir, point + ".event")
            with open(ev, "w") as f:
                f.write(str(os.getpid()))
                f.flush()
                os.fsync(f.fileno())
            _fsync_dir(self.dir)
            if not cfg.get("repeat"):
                try:
                    os.replace(path, path + ".fired")
                    _fsync_dir(self.dir)
                except OSError:
                    pass
            return

        # Default: hard kill, bypassing buffers/finally (crash model).
        if not cfg.get("repeat"):
            try:
                os.replace(path, path + ".fired")
                _fsync_dir(self.dir)
            except OSError:
                pass
        os._exit(code)


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
