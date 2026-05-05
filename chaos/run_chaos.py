"""
Meridian Chaos Suite — Full Run

Runs all scenarios in sequence and reports results.
Exit code 0 = all scenarios passed.
Exit code 1 = one or more scenarios failed.
"""

import logging
import sys
import time

import docker  # type: ignore
from docker.errors import NotFound  # type: ignore
from orchestrator import ChaosRunner
from scenarios import clock_skew, node_kill, partition

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("chaos")


def cluster_is_running() -> bool:
    """Check if the Meridian cluster containers exist and are running."""
    client = docker.from_env()
    node_names = ["meridian-node-1", "meridian-node-2", "meridian-node-3"]
    for name in node_names:
        try:
            container = client.containers.get(name)
            if container.status != "running":
                return False
        except NotFound:
            return False
    return True


def main():
    log.info("Meridian Chaos Suite Starting")
    log.info("=" * 60)

    if not cluster_is_running():
        log.error("Cluster is not running. Start it with: make cluster-up")
        sys.exit(1)

    log.info("Cluster is running. Waiting 3s for leader election...")
    time.sleep(3)

    runner = ChaosRunner()
    results = []

    # Run all scenarios
    scenarios = [
        ("node_kill", lambda r: node_kill.run(runner, r)),
        ("partition", lambda r: partition.run(runner, r)),
        ("clock_skew", lambda r: clock_skew.run(runner, r)),
    ]

    for name, fn in scenarios:
        result = runner.run_scenario(name, fn)
        results.append(result)
        # Wait between scenarios for cluster to fully stabilise
        time.sleep(5)

    # Print summary
    log.info("\n" + "=" * 60)
    log.info("CHAOS SUITE SUMMARY")
    log.info("=" * 60)

    passed = 0
    failed = 0

    for result in results:
        status = "PASS" if (result.cluster_recovered and not result.errors) else "FAIL"
        if status == "PASS":
            passed += 1
        else:
            failed += 1
        log.info(f"  {status}  {result.scenario} ({result.duration_seconds:.1f}s)")
        if result.errors:
            for err in result.errors:
                log.info(f"       ERROR: {err}")

    log.info(f"\n{passed} passed, {failed} failed")

    if failed > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
