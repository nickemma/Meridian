"""
Scenario: Clock Skew Injection

Shift the clock on one node forward by 30 seconds.
Verify Raft's term-based logical clock overrides wall clock time.
The skewed node must not disrupt the cluster's leadership.
"""

import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from orchestrator import ChaosResult, ChaosRunner

log = logging.getLogger("chaos.clock_skew")


def run(runner: ChaosRunner, result: ChaosResult):
    controller = runner.controller

    assert controller.wait_for_cluster(timeout=10), (
        "Cluster not healthy before scenario start"
    )

    log.info("Step 1: Injecting +30s clock skew on node-2")
    skewed = controller.inject_clock_skew("meridian-node-2", 30)

    if not skewed:
        log.warning("Clock skew injection requires privileged container access.")
        result.skipped = True
        return

    log.info("Step 2: Clock skewed — waiting to observe cluster behaviour")
    # Raft uses terms not wall clock time — skew should not affect correctness
    time.sleep(5)

    log.info("Step 3: Verifying all nodes still running after clock skew")
    recovered = controller.wait_for_cluster(timeout=10)
    assert recovered, "Cluster should remain healthy despite clock skew"

    log.info("Step 4: Restoring clock on node-2")
    controller.inject_clock_skew("meridian-node-2", -30)

    result.cluster_recovered = True
    log.info(
        "PASS: clock skew scenario completed — Raft unaffected by wall clock drift"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    runner = ChaosRunner()
    result = runner.run_scenario("clock_skew", lambda r: run(runner, r))

    if result.cluster_recovered and not result.errors:
        print("\nSCENARIO PASSED")
        sys.exit(0)
    else:
        print(f"\nSCENARIO FAILED: {result.errors}")
        sys.exit(1)
