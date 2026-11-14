"""Checks of the rendered chart that need no cluster.

tests/helm.nix renders the chart once per set of values into $HELM_RENDERS,
next to the envoy config of nixos/lb.nix for the same topology.
"""

import functools
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

RENDERS = Path(os.environ["HELM_RENDERS"])
RELEASE = "t-nix-grpc-farm"
HOSTNAME = "kubernetes.io/hostname"
SDS_CERT = (
    "transport_socket",
    "typed_config",
    "common_tls_context",
    "tls_certificate_sds_secret_configs",
    0,
    "name",
)


@functools.cache
def render(name: str) -> list[dict[str, Any]]:
    text = (RENDERS / f"{name}.yaml").read_text()
    return [doc for doc in yaml.safe_load_all(text) if doc]


def dig(obj: Any, *path: str | int) -> Any:
    """obj[path[0]][path[1]]..., or None where the chart left something out."""
    for key in path:
        try:
            obj = obj[key]
        except (KeyError, IndexError, TypeError):
            return None
    return obj


def named(items: list[dict[str, Any]] | None, name: str) -> dict[str, Any] | None:
    return next((item for item in items or [] if item.get("name") == name), None)


def pick(name: str, kind: str, suffix: str = "") -> dict[str, Any]:
    (found,) = (
        doc
        for doc in render(name)
        if doc["kind"] == kind and doc["metadata"]["name"] == RELEASE + suffix
    )
    return found


def pod(name: str, deployment: str) -> dict[str, Any]:
    return pick(name, "Deployment", "-" + deployment)["spec"]["template"]["spec"]


def container(name: str, deployment: str, which: str) -> dict[str, Any]:
    found = named(pod(name, deployment)["containers"], which)
    assert found is not None
    return found


def envoy(name: str) -> dict[str, Any]:
    return json.loads(pick(name, "ConfigMap", "-lb")["data"]["envoy.json"])


def upstreams(name: str) -> list[dict[str, Any]]:
    """The clusters that connect somewhere on behalf of a client."""
    clusters = envoy(name)["static_resources"]["clusters"]
    return [cluster for cluster in clusters if cluster["name"] != "admin"]


def values_of(obj: Any, key: str) -> list[Any]:
    if isinstance(obj, dict):
        nested = [v for value in obj.values() for v in values_of(value, key)]
        return ([obj[key]] if key in obj else []) + nested
    if isinstance(obj, list):
        return [v for item in obj for v in values_of(item, key)]
    return []


def test_every_set_of_values_renders() -> None:
    files = sorted(RENDERS.glob("*.yaml"))
    assert files
    for file in files:
        docs = render(file.stem)
        assert docs, file.name
        assert all(doc.get("kind") for doc in docs), file.name


def renamed(name: str) -> str:
    return {"x86_64-linux": "x86", "aarch64-linux": "arm"}.get(name, name)


def without_sds_paths(obj: Any) -> Any:
    if isinstance(obj, dict):
        obj = {key: without_sds_paths(value) for key, value in obj.items()}
        if "path_config_source" in obj:
            obj["path_config_source"]["path"] = "X"
    elif isinstance(obj, list):
        obj = [without_sds_paths(item) for item in obj]
    return obj


def without_address(socket_address: dict[str, Any]) -> None:
    socket_address["address"] = "X"
    socket_address.pop("ipv4_compat", None)


def normalized_cluster(cluster: dict[str, Any]) -> dict[str, Any]:
    cluster["name"] = renamed(cluster["name"])
    cluster.pop("dns_lookup_family", None)
    if assignment := cluster.get("load_assignment"):
        assignment["cluster_name"] = renamed(assignment["cluster_name"])
        for endpoint in assignment["endpoints"][0]["lb_endpoints"]:
            without_address(endpoint["endpoint"]["address"]["socket_address"])
    return cluster


def normalized(envoy_config: dict[str, Any]) -> dict[str, Any]:
    """What chart and module must agree on.

    They name clusters differently by design (the chart by group, the module
    by system), reach nodes differently (a headless Service, host names) and
    keep SDS files in different places. The chart also has a stats listener
    in front of an admin that stays on loopback.
    """
    static = without_sds_paths(envoy_config["static_resources"])
    static["clusters"] = sorted(
        (normalized_cluster(c) for c in static["clusters"] if c["name"] != "admin"),
        key=lambda cluster: cluster["name"],
    )
    (listener,) = (lis for lis in static["listeners"] if lis["name"] != "stats")
    static["listeners"] = [listener]
    without_address(listener["address"]["socket_address"])
    manager = listener["filter_chains"][0]["filters"][0]["typed_config"]
    for route in manager["route_config"]["virtual_hosts"][0]["routes"]:
        if "route" in route:
            route["route"]["cluster"] = renamed(route["route"]["cluster"])
    return static


def test_envoy_config_is_that_of_the_nixos_module() -> None:
    nixos = json.loads((RENDERS / "envoy-nixos.json").read_text())
    assert normalized(envoy("default")) == normalized(nixos)


def test_envoy_never_panics_over_schedulers_of_which_one_serves() -> None:
    sched = named(upstreams("default"), "sched")
    assert dig(sched, "common_lb_config", "healthy_panic_threshold", "value") == 0


def test_workers_spread_and_scheduler_replicas_avoid_each_other() -> None:
    spread = pod("default", "worker-x86")["topologySpreadConstraints"][0]
    assert spread["topologyKey"] == HOSTNAME
    assert spread["whenUnsatisfiable"] == "ScheduleAnyway"
    assert spread["nodeTaintsPolicy"] == "Honor"
    assert spread["matchLabelKeys"] == ["pod-template-hash"]
    required = pod("default", "worker-arm")["topologySpreadConstraints"][0]
    assert required["whenUnsatisfiable"] == "DoNotSchedule"
    for replica in (0, 1):
        (apart,) = pod("default", f"scheduler-{replica}")["affinity"][
            "podAntiAffinity"
        ]["requiredDuringSchedulingIgnoredDuringExecution"]
        assert apart["topologyKey"] == HOSTNAME
        # A rollout's surge pod may share the node of the one it replaces.
        expression = apart["labelSelector"]["matchExpressions"][0]
        assert expression["values"] == [str(replica)]


def test_runtime_class_is_set_only_on_request() -> None:
    assert "runtimeClassName" not in pod("default", "worker-x86")
    assert pod("runtime-class", "worker-x86")["runtimeClassName"] == "cgroup-writable"


@pytest.mark.parametrize("name", ["default", "cert-manager", "in-cluster"])
def test_worker_is_not_ready_once_its_nix_daemon_container_died(name: str) -> None:
    probe = container(name, "worker-x86", "nix-grpc-daemon")["readinessProbe"]
    assert probe["exec"]["command"] == ["nix", "store", "info", "--store", "daemon"]


def test_surge_scheduler_gets_ready_while_the_old_one_leads() -> None:
    probe = pod("in-cluster", "scheduler-0")["containers"][0]["readinessProbe"]
    assert "service" not in probe["grpc"]


def test_cert_manager_names_the_secrets_and_covers_every_scheduler_service() -> None:
    worker = pick("cert-manager", "Certificate", "-worker")
    for scheduler in ("-scheduler-0", "-scheduler-1", ""):
        assert f"{RELEASE}{scheduler}.default.svc" in worker["spec"]["dnsNames"]
    assert pick("cert-manager", "Certificate", "-lb")["spec"]["commonName"] == "lb"
    tls = named(pod("cert-manager", "scheduler-0")["volumes"], "tls")
    assert dig(tls, "secret", "secretName") == f"{RELEASE}-worker-tls"


def test_service_app_protocol_is_set_only_on_request() -> None:
    assert "appProtocol" not in pick("default", "Service")["spec"]["ports"][0]
    port = pick("cert-manager", "Service")["spec"]["ports"][0]
    assert port["appProtocol"] == "kubernetes.io/h2c"


def test_metrics_are_scraped_the_way_the_values_ask_for() -> None:
    kinds = {doc["kind"] for doc in render("cert-manager")}
    assert "VMPodScrape" in kinds
    assert "PodMonitor" not in kinds


def envoy_certs(name: str) -> tuple[str, set[str]]:
    """The certificate shown to clients and those shown to the nodes."""
    listener = envoy(name)["static_resources"]["listeners"][0]
    return (
        dig(listener["filter_chains"][-1], *SDS_CERT),
        {dig(cluster, *SDS_CERT) for cluster in upstreams(name)},
    )


def test_public_certificate_faces_clients_and_the_farm_ca_the_nodes() -> None:
    assert envoy_certs("default") == ("lb", {"lb"})
    public = f"{RELEASE}-public"
    names = {doc["metadata"]["name"] for doc in render("cert-manager")}
    assert not any(name.startswith(public) for name in names)

    assert envoy_certs("public-cert-manager") == ("public", {"lb"})
    certificate = pick("public-cert-manager", "Certificate", "-public")["spec"]
    assert certificate["issuerRef"]["name"] == "letsencrypt"
    assert certificate["dnsNames"] == ["farm.example.com"]
    volume = named(pod("public-cert-manager", "lb")["volumes"], "tls-public")
    assert dig(volume, "secret", "secretName") == f"{public}-tls"

    assert envoy_certs("public-secret") == ("public", {"lb"})
    volume = named(pod("public-secret", "lb")["volumes"], "tls-public")
    assert dig(volume, "secret", "secretName") == "public"


def test_builders_get_the_farm_certificate_under_the_service_name() -> None:
    # They dial the .svc name, which the public certificate lacks.
    listeners = envoy("public-cert-manager")["static_resources"]["listeners"]
    chain = listeners[0]["filter_chains"][0]
    assert f"{RELEASE}.default.svc" in chain["filter_chain_match"]["server_names"]
    assert dig(chain, *SDS_CERT) == "lb"


def test_external_scheduler_leaves_only_workers_on_a_node_port() -> None:
    deployments = [d for d in render("external") if d["kind"] == "Deployment"]
    assert sorted(d["metadata"]["name"] for d in deployments) == [
        f"{RELEASE}-worker-arm",
        f"{RELEASE}-worker-x86",
    ]
    assert f"{RELEASE}-lb" not in (RENDERS / "external.yaml").read_text()

    x86 = container("external", "worker-x86", "nix-grpc-daemon")
    assert "--scheduler=farm.example.com:50051" in x86["args"]
    assert dig(named(x86["ports"], "grpc"), "hostPort") == 50052
    assert dig(named(x86["ports"], "metrics"), "hostPort") == 9465
    arm = container("external", "worker-arm", "nix-grpc-daemon")
    assert dig(named(arm["ports"], "metrics"), "hostPort") is None

    # A second pod cannot take the port on the same node, so no surge.
    strategy = pick("external", "Deployment", "-worker-x86")["spec"]["strategy"]
    assert strategy["rollingUpdate"]["maxSurge"] == 0


def test_external_worker_advertises_the_address_of_its_node() -> None:
    x86 = container("external", "worker-x86", "nix-grpc-daemon")
    shell, flag, script, *rest = x86["command"]
    assert shell == "/bin/sh"
    script = script.replace("exec /bin/nix-grpc-daemon", "echo", 1)

    def advertised(node: str) -> str:
        done = subprocess.run(
            ["sh", flag, script, *rest],
            env={**os.environ, "NODE_NAME": node},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        return done.stdout.strip()

    assert advertised("n1") == "--advertise=10.0.0.1:50052"
    assert advertised("n2") == "--advertise=10.0.0.2:50052"
    assert "no advertise address" in advertised("n3")
    terms = pod("external", "worker-x86")["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]["nodeSelectorTerms"]
    assert terms[0]["matchExpressions"][0]["values"] == ["n1", "n2"]

    # Without a map the node's InternalIP is the address.
    arm = container("external", "worker-arm", "nix-grpc-daemon")
    assert arm["command"] == ["/bin/nix-grpc-daemon"]
    assert "--advertise=$(HOST_IP):50052" in arm["args"]


def test_extra_volumes_reach_the_nix_daemon_container_only() -> None:
    for group in ("x86", "arm"):
        spec = pod("extra-volumes", f"worker-{group}")
        volume = named(spec["volumes"], "nix-secrets")
        assert dig(volume, "secret", "secretName") == "nix-github"
        mounted = {
            box["name"]: mount["mountPath"]
            for box in spec["containers"]
            for mount in box.get("volumeMounts", [])
            if mount["name"] == "nix-secrets"
        }
        assert mounted == {"nix-daemon": "/etc/nix/secrets"}
    assert named(pod("default", "worker-x86")["volumes"], "nix-secrets") is None


@pytest.mark.parametrize("name", ["default", "cert-manager"])
@pytest.mark.parametrize("deployment", ["worker-x86", "scheduler-0", "lb"])
def test_only_the_ca_certificate_is_mounted(name: str, deployment: str) -> None:
    # Under cert-manager the CA secret holds the CA key as well.
    items = [
        volume["secret"].get("items")
        for volume in pod(name, deployment)["volumes"]
        if dig(volume, "secret", "secretName") in ("ca", f"{RELEASE}-ca")
    ]
    assert items == [[{"key": "ca.crt", "path": "ca.crt"}]]


def test_envoy_admin_is_on_loopback_behind_two_paths() -> None:
    config = envoy("default")
    assert config["admin"]["address"]["socket_address"]["address"] == "127.0.0.1"
    stats = named(config["static_resources"]["listeners"], "stats")
    assert sorted(values_of(stats, "path")) == ["/ready", "/stats/prometheus"]


def test_upstream_names_are_verified() -> None:
    for cluster in upstreams("default"):
        context = dig(cluster, *SDS_CERT[:3], "combined_validation_context")
        sans = dig(
            context, "default_validation_context", "match_typed_subject_alt_names"
        )
        assert dig(sans, 0, "matcher", "exact") == "worker", cluster["name"]


def test_long_streams_are_not_capped_at_envoys_default() -> None:
    for cluster in upstreams("default"):
        limit = dig(cluster, "circuit_breakers", "thresholds", 0, "max_requests")
        assert limit == 1_000_000, cluster["name"]


def test_worker_settings_reach_nix_daemon() -> None:
    conf = pick("default", "ConfigMap", "-worker-x86")["data"]["farm.conf"]
    assert "build-dir = /build" in conf.splitlines()


def test_changed_upstream_cache_rolls_the_workers() -> None:
    def checksum(name: str) -> str:
        template = pick(name, "Deployment", "-worker-x86")["spec"]["template"]
        return template["metadata"]["annotations"]["checksum/config"]

    assert checksum("default") != checksum("other-upstream-cache")
