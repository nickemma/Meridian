#!/usr/bin/env python3
"""Enforce the internal/<module>/{domain,ports,application,adapters} layering.

Within a module:  domain <- ports <- application <- adapters (never upward).
Across modules:   only another module's domain/ports may be imported, except
                  by adapters, which act as composition roots.
Domain packages must not import gRPC, Prometheus or generated protobuf code.
"""
import subprocess
import sys

RANK = {"domain": 0, "ports": 1, "application": 2, "adapters": 3}
PREFIX = "github.com/nickemma/meridian/internal/"
INFRA = ("google.golang.org/grpc", "github.com/prometheus/", "github.com/nickemma/meridian/proto/")

listing = subprocess.run(
    ["go", "list", "-tags", "storageffi", "-f", '{{.ImportPath}} {{join .Imports " "}}', "./internal/..."],
    capture_output=True, text=True, check=True,
).stdout

violations = []
for line in listing.splitlines():
    package, *imports = line.split()
    parts = package[len(PREFIX):].split("/")
    if len(parts) != 2 or parts[1] not in RANK:
        violations.append(f"{package}: not inside a domain/ports/application/adapters layer")
        continue
    module, layer = parts
    for imported in imports:
        if not imported.startswith(PREFIX):
            if layer == "domain" and imported.startswith(INFRA):
                violations.append(f"{module}/{layer} imports infrastructure {imported}")
            continue
        other_module, other_layer = imported[len(PREFIX):].split("/")[:2]
        if other_module == module and RANK[other_layer] > RANK[layer]:
            violations.append(f"{module}/{layer} imports higher layer {module}/{other_layer}")
        elif other_module != module and RANK[other_layer] >= RANK["application"] and layer != "adapters":
            violations.append(f"{module}/{layer} imports {other_module}/{other_layer} across modules")

if violations:
    print("\n".join(violations))
    sys.exit(1)
print("layering OK")
