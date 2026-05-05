"""
Meridian Chaos Orchestrator

Controls a 3-node Docker cluster, injecting failures and
verifying the cluster recovers correctly after each scenario.
"""

import logging
import random
import subprocess
import time
from dataclasses import dataclass, field
from this import s
from typing import Optional

import docker  # type: ignore
from docker.errors import NotFound  # type: ignore

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("chaos")


@dataclass
class Operation:
    """A single operation submitted to the cluster during a chaos run."""

    op_type: str  # "write" or "read"
    key: str
    value: Optional[str]
    start_time: float  # unix timestamp when op was submitted
    end_time: float  # unix timestamp when op completed
    success: bool
    response_value: Optional[str] = None
    error: Optional[str] = None


@dataclass
class ChaosResult:
    """Result of a single chaos scenario run."""

    scenario: str
    operations: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    duration_seconds: float = 0.0
    cluster_recovered: bool = False
    skipped: bool = False


class ClusterController:
    """
    Controls the 3-node Meridian cluster via Docker.
    Provides primitives for killing nodes, partitioning the network,
    and injecting clock skew.
    """

    NODE_NAMES = ["meridian-node-1", "meridian-node-2", "meridian-node-3"]

    def __init__(self):
        self.client = docker.from_env()

    def get_node(self, name: str):
        """Get a Docker container by name."""
        try:
            return self.client.containers.get(name)
        except NotFound:
            return None

    def kill_node(self, name: str) -> bool:
        """
        Kill a node by stopping its container.
        The node loses all in-memory state — simulates a crash.
        WAL on the volume survives.
        """
        container = self.get_node(name)
        if container is None:
            log.warning(f"Node {name} not found")
            return False

        container.stop(timeout=1)
        log.info(f"[chaos] killed node {name}")
        return True

    def restart_node(self, name: str) -> bool:
        """
        Restart a killed node.
        The node replays its WAL and rejoins the cluster.
        """
        container = self.get_node(name)
        if container is None:
            log.warning(f"Node {name} not found for restart")
            return False

        container.start()
        log.info(f"[chaos] restarted node {name}")
        # Give the node time to replay WAL and rejoin
        time.sleep(2)
        return True

    def partition_node(self, name: str) -> bool:
        """
        Isolate a node from the rest of the cluster using iptables.
        The container stays running but cannot send or receive
        any packets to/from other cluster nodes.
        """
        container = self.get_node(name)
        if container is None:
            return False

        # Drop all traffic to/from the meridian network for this container
        cmd = [
            "docker",
            "exec",
            name,
            "sh",
            "-c",
            "iptables -I INPUT -j DROP && iptables -I OUTPUT -j DROP",
        ]
        result = subprocess.run(cmd, capture_output=True)
        success = result.returncode == 0
        if success:
            log.info(f"[chaos] partitioned node {name}")
        else:
            log.error(f"[chaos] partition failed for {name}: {result.stderr}")
        return success

    def heal_partition(self, name: str) -> bool:
        """
        Remove iptables rules to restore network connectivity.
        """
        cmd = [
            "docker",
            "exec",
            name,
            "sh",
            "-c",
            "iptables -F INPUT && iptables -F OUTPUT",
        ]
        result = subprocess.run(cmd, capture_output=True)
        success = result.returncode == 0
        if success:
            log.info(f"[chaos] healed partition for {name}")
            time.sleep(1)  # allow reconnection
        return success

    def inject_clock_skew(self, name: str, skew_seconds: int) -> bool:
        """
        Shift the system clock on a node forward or backward.
        Positive skew_seconds = clock runs ahead.
        Negative skew_seconds = clock runs behind.

        Clock skew tests whether Raft's logical clock (terms)
        correctly overrides wall clock time for safety decisions.
        """
        import time

        # get current epoch time + skew
        new_time = int(time.time() + skew_seconds)
        cmd = ["docker", "exec", name, "date", "-s", f"@{new_time}"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        success = result.returncode == 0

        if success:
            log.info(f"[chaos] injected {skew_seconds}s clock skew on {name}")
        else:
            log.error(
                f"[chaos] clock skew injection failed for {name}: {result.stderr}"
            )

        return success

    def all_nodes_healthy(self) -> bool:
        """
        Return True if all 3 nodes are running.
        Does not check Raft health — just container health.
        """
        for name in self.NODE_NAMES:
            container = self.get_node(name)
            if container is None or container.status != "running":
                return False
        return True

    def wait_for_cluster(self, timeout: float = 10.0) -> bool:
        """
        Wait until all nodes are running.
        Returns True if cluster is up within timeout, False otherwise.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.all_nodes_healthy():
                return True
            time.sleep(0.5)
        return False

    def random_node(self) -> str:
        """Pick a random node name."""
        return random.choice(self.NODE_NAMES)

    def random_follower(self) -> str:
        """
        Pick a random non-leader node.
        We detect the leader by checking logs — simplified here
        to just pick a random node for the chaos suite.
        """
        return random.choice(self.NODE_NAMES)


class ChaosRunner:
    """
    Runs chaos scenarios against the Meridian cluster.
    Records all operations for linearizability verification.
    """

    def __init__(self):
        self.controller = ClusterController()
        self.history: list[Operation] = []

    def record_operation(
        self,
        op_type: str,
        key: str,
        value: Optional[str],
        start: float,
        end: float,
        success: bool,
        response_value: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Operation:
        op = Operation(
            op_type=op_type,
            key=key,
            value=value,
            start_time=start,
            end_time=end,
            success=success,
            response_value=response_value,
            error=error,
        )
        self.history.append(op)
        return op

    def run_scenario(self, name: str, fn) -> ChaosResult:
        """Run a single chaos scenario and return the result."""
        log.info(f"\n{'=' * 60}")
        log.info(f"SCENARIO: {name}")
        log.info(f"{'=' * 60}")

        result = ChaosResult(scenario=name)
        start = time.time()

        try:
            fn(result)
            result.cluster_recovered = self.controller.wait_for_cluster(timeout=15)
        except Exception as e:
            result.errors.append(str(e))
            log.error(f"Scenario {name} failed with exception: {e}")
        finally:
            result.duration_seconds = time.time() - start

        if result.skipped:
            status = "SKIPPED"
        elif result.cluster_recovered and not result.errors:
            status = "PASSED"
        else:
            status = "FAILED"
        log.info(f"Scenario {name}: {status} ({result.duration_seconds:.1f}s)")
        return result
