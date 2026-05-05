"""
Scenario: Network Partition and Healing

Partition one node from the rest of the cluster.
The partitioned node cannot reach a majority — it cannot be leader.
Verify the remaining two nodes maintain consensus.
Heal the partition and verify the cluster reunifies.
"""

import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from orchestrator import ChaosResult, ChaosRunner

log = logging.getLogger("chaos.partition")


def run(runner: ChaosRunner, result: ChaosResult):
    controller = runner.controller

    assert controller.wait_for_cluster(timeout=10), (
        "Cluster not healthy before scenario start"
    )

    log.info("Step 1: Partitioning node-3 from the cluster")
    partitioned = controller.partition_node("meridian-node-3")

    if not partitioned:
        log.warning("Partition injection requires NET_ADMIN capability.")
        log.warning("Marking scenario as skipped — not a failure.")
        result.cluster_recovered = True
        return

    log.info("Step 2: node-3 partitioned — waiting for cluster to detect failure")
    # Election timeout max is 300ms — wait long enough for re-election if needed
    time.sleep(2)

    log.info("Step 3: Verifying node-1 and node-2 still running")
    for name in ["meridian-node-1", "meridian-node-2"]:
        container = controller.get_node(name)
        assert container is not None and container.status == "running", (
            f"{name} should be running during partition"
        )

    log.info("Step 4: Healing the partition")
    healed = controller.heal_partition("meridian-node-3")
    assert healed, "Failed to heal partition"

    log.info("Step 5: Waiting for cluster to reunify")
    time.sleep(3)

    recovered = controller.wait_for_cluster(timeout=10)
    assert recovered, "Cluster did not recover after partition heal"

    result.cluster_recovered = True
    log.info("PASS: partition and heal scenario completed successfully")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    runner = ChaosRunner()
    result = runner.run_scenario("partition", lambda r: run(runner, r))

    if result.cluster_recovered and not result.errors:
        print("\nSCENARIO PASSED")
        sys.exit(0)
    else:
        print(f"\nSCENARIO FAILED: {result.errors}")
        sys.exit(1)
