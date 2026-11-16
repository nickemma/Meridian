#!/usr/bin/env python3
"""Run the secure, tenant-scoped storage demo until Ctrl-C (Python stdlib only)."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dir', default='.meridian/local', help='persistent local demo directory')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    work = Path(args.dir).resolve()
    processes = []
    sockets = []
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        # Fail before launching anything if a fixed demo port is occupied.
        for base in (8080, 9090, 9080):
            for index in range(1, 4):
                sock = socket.socket()
                sockets.append(sock)
                sock.bind(('127.0.0.1', base + index))
        work.mkdir(parents=True, exist_ok=True)
        pki = work / 'pki'
        if not pki.exists():
            subprocess.run([str(root / 'bin/meridian-pki'), '-out', str(pki)], check=True)
        for sock in sockets:
            sock.close()
        policies = ','.join(
            f'/tenants/{tenant}/{kind}/:{kind}:{version}'
            for tenant in ('a', 'b')
            for version, kind in enumerate(('strong', 'causal', 'eventual'), 1)
        )
        endpoints = {}
        for index in range(1, 4):
            node = f'node-{index}'
            env = {key: value for key, value in os.environ.items() if not key.startswith('MERIDIAN_')}
            env.update({
                'MERIDIAN_NODE_ID': node,
                'MERIDIAN_RAFT_PORT': str(9090 + index),
                'MERIDIAN_CLIENT_PORT': str(8080 + index),
                'MERIDIAN_DATA_DIR': str(work / f'{node}-data'),
                'MERIDIAN_PEERS': ','.join(f'node-{j}=127.0.0.1:{9090+j}' for j in range(1, 4) if j != index),
                'MERIDIAN_POLICIES': policies,
                'MERIDIAN_TLS_CA': str(pki / 'ca.pem'),
                'MERIDIAN_TLS_CERT': str(pki / f'{node}.pem'),
                'MERIDIAN_TLS_KEY': str(pki / f'{node}.key'),
                'MERIDIAN_CLIENT_POLICIES': str(pki / 'clients.json'),
            })
            with (work / f'{node}.log').open('ab') as log:
                proc = subprocess.Popen([str(root / 'bin/meridian')], env=env, stdout=log, stderr=log)
            processes.append(proc)
            endpoints[node] = {'pid': proc.pid, 'client': f'127.0.0.1:{8080+index}', 'metrics': f'http://127.0.0.1:{9080+index}/metrics'}
        (work / 'endpoints.json').write_text(json.dumps(endpoints, indent=2) + '\n')
        print(f'Nodes launched; credentials, data and logs: {work}', flush=True)
        print('Client ports: 8081, 8082, 8083. Metrics: 9081, 9082, 9083.', flush=True)
        print('Wait for a leader using meridianctl status. Ctrl-C stops all child nodes; data is retained.', flush=True)
        while not stopping:
            for index, proc in enumerate(processes, 1):
                if proc.poll() is not None:
                    raise RuntimeError(f'node-{index} exited ({proc.returncode}); inspect its log in {work}')
            time.sleep(0.25)
        return 0
    except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
        print(f'Local cluster failed: {exc}', file=sys.stderr)
        return 1
    finally:
        for sock in sockets:
            sock.close()
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


if __name__ == '__main__':
    sys.exit(main())
