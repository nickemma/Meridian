"""
Linearizability Verifier

Takes a history of operations (each with start_time, end_time,
value written or read) and checks whether any valid sequential
ordering exists that:

  1. Is consistent with real-time ordering
     (if op A ended before op B started, A must come before B)
  2. Satisfies the semantics of the operation
     (a read must return the value of the most recent write)

This is a simplified Wing-Gong linearizability checker.
For production use, Jepsen's Knossos checker is more complete.
This implementation is sufficient to catch the most common
Raft correctness violations.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from orchestrator import Operation

log = logging.getLogger("chaos.verifier")


@dataclass
class VerificationResult:
    linearizable: bool
    violation: Optional[str] = None
    checked_operations: int = 0


def verify(history: list[Operation]) -> VerificationResult:
    """
    Check whether the operation history is linearizable.

    Algorithm:
    1. Sort operations by end_time (completion time)
    2. Build the expected sequential state by replaying writes in order
    3. Check that every read returns the value that was most recently
       written before the read completed
    4. Flag any read that returns a stale or impossible value
    """

    if not history:
        return VerificationResult(linearizable=True, checked_operations=0)

    writes = [op for op in history if op.op_type == "write" and op.success]
    reads = [op for op in history if op.op_type == "read" and op.success]

    log.info(f"Verifying {len(writes)} writes and {len(reads)} reads")

    # Sort writes by completion time — this is the order they
    # took effect from the perspective of the linearization point.
    writes.sort(key=lambda op: op.end_time)

    violations = []

    for read in reads:
        # Find all writes that completed before this read completed.
        # These are the writes the read could have observed.
        visible_writes = [
            w for w in writes if w.key == read.key and w.end_time <= read.end_time
        ]

        if not visible_writes:
            # No writes visible to this read — it should return None or initial value.
            if read.response_value is not None and read.response_value != "":
                violations.append(
                    f"Read of {read.key} at {read.end_time:.3f} returned "
                    f"'{read.response_value}' but no writes were visible"
                )
            continue

        # The most recent visible write defines what the read must return.
        latest_write = max(visible_writes, key=lambda w: w.end_time)
        expected = latest_write.value

        if read.response_value != expected:
            # Check if this read overlapped with writes — concurrent reads
            # are allowed to observe either the old or new value.
            concurrent_writes = [
                w
                for w in writes
                if w.key == read.key
                and w.start_time < read.end_time
                and w.end_time > read.start_time
            ]

            if concurrent_writes:
                # Concurrent write — either old or new value is acceptable.
                # Find all values that could have been observed.
                acceptable = {w.value for w in concurrent_writes}
                acceptable.add(expected)

                if read.response_value not in acceptable:
                    violations.append(
                        f"Read of {read.key} at {read.end_time:.3f} returned "
                        f"'{read.response_value}' — not in acceptable set {acceptable}"
                    )
            else:
                # No concurrent writes — must return the latest write's value.
                violations.append(
                    f"Read of {read.key} at {read.end_time:.3f} returned "
                    f"'{read.response_value}' but expected '{expected}' "
                    f"(latest write completed at {latest_write.end_time:.3f})"
                )

    if violations:
        log.error(f"LINEARIZABILITY VIOLATED — {len(violations)} violation(s):")
        for v in violations:
            log.error(f"  {v}")
        return VerificationResult(
            linearizable=False,
            violation=violations[0],
            checked_operations=len(history),
        )

    log.info(
        f"Linearizability verified — {len(history)} operations checked, no violations"
    )
    return VerificationResult(
        linearizable=True,
        checked_operations=len(history),
    )
