"""
Scenario: Node Kill and Recovery

Kill one node while the cluster is healthy.
Verify the cluster continues operating with 2/3 nodes.
Restart the killed node and verify it rejoins correctly.
"""

import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from orchestrator import ChaosResult, ChaosRunner

log = logging.getLogger("chaos.node_kill")


def run(runner: ChaosRunner, result: ChaosResult):
    controller = runner.controller

    # Verify cluster is healthy before starting
    assert controller.wait_for_cluster(timeout=10), (
        "Cluster not healthy before scenario start"
    )

    log.info("Step 1: Cluster healthy — killing node-3")
    killed = controller.kill_node("meridian-node-3")
    assert killed, "Failed to kill node-3"

    # Wait for the cluster to detect the failure and stabilise
    time.sleep(3)
    log.info("Step 2: node-3 killed — cluster should continue with 2 nodes")

    # Verify node-1 and node-2 are still running
    for name in ["meridian-node-1", "meridian-node-2"]:
        container = controller.get_node(name)
        assert container is not None and container.status == "running", (
            f"{name} should still be running after node-3 kill"
        )

    log.info("Step 3: node-1 and node-2 still running — restarting node-3")
    restarted = controller.restart_node("meridian-node-3")
    assert restarted, "Failed to restart node-3"

    # Wait for node-3 to replay WAL and rejoin
    time.sleep(3)
    log.info("Step 4: node-3 restarted — verifying full cluster recovery")

    recovered = controller.wait_for_cluster(timeout=10)
    assert recovered, "Cluster did not recover after node restart"

    result.cluster_recovered = True
    log.info("PASS: node kill and recovery scenario completed successfully")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    runner = ChaosRunner()
    result = runner.run_scenario("node_kill", lambda r: run(runner, r))

    if result.cluster_recovered and not result.errors:
        print("\nSCENARIO PASSED")
        sys.exit(0)
    else:
        print(f"\nSCENARIO FAILED: {result.errors}")
        sys.exit(1)
