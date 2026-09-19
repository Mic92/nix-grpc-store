# Set up a build farm

A build farm is a group of workers behind one address. Clients send
builds to that address, each derivation is built once even if several
clients ask for it, work spreads over all workers, and outputs land in an
S3 binary cache through [niks3](https://github.com/Mic92/niks3).

![build farm](farm.svg)

This guide walks you through setting up the cache, the workers, the
balancer, and two kinds of client: a CI host and a developer laptop.

## How it works

Every worker runs the same `nix-grpc-daemon`. One of them is also the
**scheduler**: it keeps the queue and decides which worker builds what.

* A client opens one connection to the scheduler and lists the
  derivations it wants. For each one the scheduler answers *cached*
  (outputs are already in the binary cache), *go to worker X*, or *no
  worker can build this* (wrong system or missing feature). Derivations
  on the longest chain go first; among workers with a free slot the
  scheduler prefers the one that already has the inputs.
* Each worker holds a connection to the scheduler saying which system
  and features it offers and how many builds it takes. The scheduler
  tells it which derivation to expect before the client shows up. A
  build nobody announced is turned away and the client asks the
  scheduler again. A second client for a build that is already running
  attaches to it.
* The balancer (envoy) sends scheduler traffic to the scheduler, sends
  each build to the worker the scheduler named, and spreads everything
  else (uploads, queries, downloads) over the workers of the right
  system.

Nothing is stored on disk. Restart the scheduler and running builds
finish, workers and clients reconnect, and clients ask again for what is
still missing. A clean stop tells them first, so they come back at once
and print nothing. After a crash they notice on their own and retry for
up to two minutes. Restart a worker and its builds fail and are sent
elsewhere. A single node with no `scheduler` setting is a farm of one.

## Before you begin

You need:

* A [niks3](https://github.com/Mic92/niks3) instance, an S3 bucket behind
  it, and its API token and public signing key. A single node can do
  without. Outputs then stay in its store.
* One or more NixOS machines per system type to act as workers. One of
  them (or a separate small machine) is the scheduler.
* One machine with a public DNS name for the balancer. It can also be a
  worker.
* A small private CA. It signs three kinds of certificate:

  | Certificate | CN | Used by |
  |---|---|---|
  | worker server cert | `worker-<host>`, SAN = address envoy dials | each worker; also shown to the scheduler |
  | balancer client cert | `lb-<host>` | envoy, towards workers |
  | CI client cert | `ci-<host>` | each CI host, towards envoy |

  The balancer's public listener uses a regular ACME certificate, so
  clients don't need the private CA.

All modules below come from `nix-grpc-store.nixosModules.default`.

## Step 1: Configure a worker

```nix
services.nix-grpc-daemon = {
  enable = true;
  listen = "[::]:50052";
  advertise = "10.0.0.5:50052";
  roles = [ "builder" ];              # add "scheduler" on exactly one worker
  scheduler = "10.0.0.4:50052";       # leave out on that worker
  schedulerCaFile = "/run/keys/farm-ca.crt";
  idleTimeout = null;
  maxJobs = 8;
  minFree = "20G";
  tls = {
    certFile = "/run/keys/worker.crt";
    keyFile = "/run/keys/worker.key";
    clientCaFile = "/run/keys/farm-ca.crt";
  };
  trustedProxies = [ "lb-*" ];
  accessRules = [
    { cn = "ci-*"; role = "trusted"; }
    { cn = "worker-*"; role = "trusted"; }
  ];
  niks3 = {
    url = "https://niks3.example.com";
    tokenFile = "/run/keys/niks3-token";
    cacheUrl = "https://cache.example.com";
    publicKeys = [ "cache.example.com-1:…" ];
  };
};
```

| Option | What it does |
|---|---|
| `roles` | `builder`, `scheduler` or both (the default). Exactly one node per farm has `scheduler`. |
| `scheduler` | Address of the scheduler node. Workers connect to it directly, not through the balancer. |
| `advertise` | The address the balancer knows this worker by. Must match its entry in the balancer's `workers`. |
| `maxJobs` | Concurrent builds on this worker. Keep it at or below the local nix-daemon's `max-jobs`. |
| `minFree` | Below this much free disk the worker takes no new builds until space returns. |
| `trustedProxies` | Peers with a matching client cert are balancers. The worker takes the real client identity from the `x-forwarded-client-cert` header they set. Other peers can't set it. |
| `accessRules` | Maps CNs to roles. CI needs `trusted`. So do the other workers, for their connection to the scheduler. |
| `workerName` | Name clients see in `worker: building …` lines and in `nix_grpc_build_info`. Defaults to the hostname. |

Repeat for every worker. Workers for different systems use the same
config. `nix.settings.system-features` decides which
`requiredSystemFeatures` a worker accepts: a derivation that needs `kvm`
only goes to workers that list it.

Builds run in `nix-daemon.service`, RPCs and downloads in
`nix-grpc-daemon.service`. Give both a `CPUWeight`/`MemoryHigh` so a
heavy build can't starve the daemon.

### Optional: accept developer tokens

To let people without a client certificate use the farm, add an OIDC
provider. The schema matches niks3's `--oidc-config`.

```nix
services.nix-grpc-daemon.oidc.providers.sso = {
  issuer = "https://auth.example.com/realms/internal";
  audience = "nix-farm";
  rules = [ { bound_claims.groups = [ "developers" ]; scopes = [ "write" ]; } ];
};
```

`write` lets a token holder request builds. It does not let them upload
unsigned paths or open the worker-protocol tunnel.

## Step 2: Configure the balancer

```nix
services.nix-grpc-farm-lb = {
  enable = true;
  listen = "[::]:50051";
  workers = {
    x86_64-linux  = [ "10.0.0.4:50052" ];
    aarch64-linux = [ "10.0.0.5:50052" "10.0.0.6:50052" ];
  };
  scheduler = "10.0.0.4:50052";
  tls = {
    certFile = "/var/lib/acme/farm.example.com/fullchain.pem";
    keyFile  = "/var/lib/acme/farm.example.com/key.pem";
    clientCaFile = "/run/keys/farm-ca.crt";
    upstream = {
      certFile = "/run/keys/lb.crt";
      keyFile  = "/run/keys/lb.key";
      caFile   = "/run/keys/farm-ca.crt";
    };
  };
};
security.acme.certs."farm.example.com" = {
  group = "envoy";
  reloadServices = [ "envoy.service" ];
};
```

| Option | What it does |
|---|---|
| `workers` | Worker addresses per system, as in their `advertise`. Requests for a system not listed go to `defaultSystem` (first entry by default). |
| `scheduler` | The worker with the `scheduler` role. Defaults to the first worker of `defaultSystem`. |
| `tls.certFile`/`keyFile` | Public server certificate. |
| `tls.clientCaFile` | Verify client certificates against this CA and forward the subject to workers. Clients without a certificate are still accepted so tokens work. |
| `tls.upstream.*` | The certificate envoy presents to workers, and the CA to verify them. |

To check that envoy sees your workers:

```console
$ curl -s localhost:9901/clusters | grep health_flags
x86_64-linux::10.0.0.4:50052::health_flags::healthy
aarch64-linux::10.0.0.5:50052::health_flags::healthy
sched::10.0.0.4:50052::health_flags::healthy
```

`accessLog = true` adds one journal line per connection with the client
certificate subject and, for failed TLS handshakes, the reason.

## Step 3: Connect a CI host

The CI host's nix-daemon uses the farm through the standard build hook.

```nix
programs.nix-grpc-store.enable = true;
nix.distributedBuilds = true;
nix.buildMachines = map (system: {
  hostName = "grpc://farm.example.com:50051?system=${system}&client-cert=/run/keys/ci.crt&client-key=/run/keys/ci.key";
  protocol = null;
  inherit system;
  supportedFeatures = [ "big-parallel" "kvm" "nixos-test" ];
  maxJobs = 64;
}) [ "x86_64-linux" "aarch64-linux" ];
```

One entry per system matters: the hook copies inputs before it says what
it will build, and `system=` is how the balancer knows which workers
those uploads (and `builders-use-substitutes` fetches) belong to.

The hook runs inside `nix-daemon.service`. If you filter its egress with
`nix.firewall`, the module already allows port 50051. For another
balancer port set `programs.nix-grpc-store.daemonEgressPorts`.

For each derivation the hook asks the scheduler for a worker and sends
the build there. If the worker doesn't have the `.drv` closure, the
plugin uploads it and retries. The worker streams the build log back and
the hook copies the outputs from it when it's done.

CI needs the `trusted` role because the build hook uploads locally built,
unsigned inputs.

To verify, build something that isn't cached:

```console
$ nix build --max-jobs 0 --impure --expr '(import <nixpkgs> {}).runCommand "farm-test" {} "date > $out"'
building '/nix/store/…-farm-test.drv' on 'grpc://farm.example.com:50051?…'...
worker-1: building …-farm-test.drv
```

## Step 4: Connect a developer laptop

Developers use a token instead of a certificate and let the farm build
from their local evaluation:

```console
$ nix build --store 'grpc://farm.example.com:50051?token-file=./jwt' --eval-store auto .#pkg
```

`--eval-store auto` is required so the derivations exist locally. The
whole dependency graph goes to the scheduler at once, so independent
derivations build in parallel across workers.

The result is in the cache, not the local store. To fetch it:

```console
$ nix copy --from https://cache.example.com $(nix build … --print-out-paths)
```

Tip: wrap the token fetch (an OIDC device-code login is a few lines of
`curl`) and the `nix build` call in a script and expose it as
`nix run .#nix-farm`.

## On Kubernetes

The Helm chart deploys the same pieces: a scheduler Deployment (one
replica, the new pod is up before the old one leaves), a Deployment per worker group, envoy in front and a
[harmonia-gc](https://github.com/nix-community/harmonia) sidecar per
worker for disk space. niks3 is its own release, see its
[Kubernetes page](https://github.com/Mic92/niks3/wiki/Kubernetes).

```console
$ helm install farm oci://ghcr.io/mic92/charts/nix-grpc-farm -n farm --create-namespace -f values.yaml
```

```yaml
# values.yaml
niks3:
  serverURL: http://niks3.niks3.svc      # where workers push and the scheduler checks
  cacheURL: https://cache.example.com    # where everyone substitutes from
  publicKeys: ["cache.example.com-1:…"]
  auth:
    serviceAccountToken: {enabled: true} # or existingSecret with key `token`
workers:
  x86-64:
    system: x86_64-linux
    replicas: 4
    maxJobs: 2
    features: [big-parallel, kvm]
    store: {sizeLimit: 200Gi}
    nodeSelector: {kubernetes.io/arch: amd64}
  aarch64:
    system: aarch64-linux
    replicas: 2
    nodeSelector: {kubernetes.io/arch: arm64}
gc:
  ensureFree: 40G                        # above every group's minFree (20G)
tls:
  clientCA: {existingSecret: farm-ca}    # ca.crt
  lb: {existingSecret: farm-lb-tls}      # kubernetes.io/tls, e.g. from cert-manager
  worker: {existingSecret: farm-worker-tls, serverName: worker}
auth:
  accessRules: [{cn: "ci-*", role: trusted}]
lb:
  service: {type: LoadBalancer}
```

`helm install` prints the farm address and a `builders =` line.

Things to know:

* **Sandbox.** Builds are sandboxed, which needs the `nix-daemon`
  container to run privileged. Where that is forbidden set
  `sandbox.enabled: false` and builds run unsandboxed as root in the
  container.
* **Store volume.** `/nix` is an `emptyDir` seeded from the image at pod
  start. `gc.ensureFree` and `minFree` see the free space of the node
  disk behind it, shared by all pods on the node (`sizeLimit` evicts, it
  is not a quota). Size them for that disk and keep `ensureFree` above
  `minFree`.
* **Resources.** Builds run in the `nix-daemon` container, RPCs in
  `nix-grpc-daemon`. Size `resources` and `daemonResources` separately.
* **Worker certificate.** Workers reach the scheduler through its
  Service, so the certificate in `tls.worker` must also cover
  `<release>-nix-grpc-farm-scheduler.<namespace>.svc`, and its CN must
  be in `auth.workerCNs`.
* **Identity to niks3.** With `niks3.auth.serviceAccountToken.enabled` the
  pods present a projected service account token (audience `niks3`).
  Allow `<namespace>:<release>-nix-grpc-farm` with scope `write` in the
  niks3 chart's `auth.workloadIdentity.allowedServiceAccounts`. The two
  releases share no secret.
* **Identity of clients.** CI pods in the cluster can skip certificates:

  ```yaml
  auth:
    workloadIdentity:
      enabled: true
      allowedServiceAccounts: ["ci:builder"]
  ```

  and mount a token for the farm:

  ```yaml
  volumes:
    - name: farm-token
      projected:
        sources:
          - serviceAccountToken: {audience: nix-grpc-farm, path: token}
  ```

  then `nix build --store 'grpc://farm-nix-grpc-farm.farm.svc:50051?token-file=/var/run/secrets/farm/token&ca-cert=…' --eval-store auto`.
  Tokens need TLS on the balancer (`tls.lb`).
* **Bring your own balancer.** `lb.enabled: false` drops envoy. Whatever
  replaces it must send `/nix.remote.Scheduler/*` to the scheduler
  Service, send a request with an `x-nix-worker: IP:port` header to that
  pod, route the rest by `x-nix-system` to the matching group's headless
  Service, and health-check `grpc.health.v1`.
* **Monitoring.** `metrics.podMonitor.enabled` scrapes workers, scheduler
  and envoy. `grafanaDashboard.enabled` ships the dashboard as a
  ConfigMap for the Grafana sidecar, with its `instance` variable set to
  `pod`.

## Managing the farm

**Add a worker.** Deploy it with the Step 1 config, add its address under
`workers.<system>`, redeploy the balancer. It takes queued builds as soon
as it has connected to the scheduler.

**Drain a worker.** `systemctl stop nix-grpc-daemon` stops new builds
arriving and returns once running builds have published. After
`TimeoutStopSec` (1h) the rest is killed and the clients are sent to
another worker. A second SIGTERM cancels right away. `nixos-rebuild
switch` only signals the worker and the next connection starts the new
generation.

**Restart the scheduler.** Safe at any time, see [How it works](#how-it-works).
New builds wait until it is back.

**See what a worker did.** Each build is one journal line:

```
event=rpc method=BuildDerivation cn=ci-build01 duration_s=42 …
```

Set `logLevel = "debug"` on the scheduler to also log each request and
assignment.

**Metrics.** Set `services.nix-grpc-daemon.metricsListen` on workers and
scrape envoy's admin port (`/stats/prometheus`). The scheduler adds
`nix_grpc_sched{kind="queued|workers|clients"}`. A Grafana dashboard for
all of it ships as `nixos/grafana/farm.json` (flake:
`nix-grpc-store.dashboards.farm`):

```nix
services.grafana.provision.dashboards.settings.providers = [{
  name = "nix-farm";
  options.path = pkgs.linkFarm "dashboards" { "farm.json" = inputs.nix-grpc-store.dashboards.farm; };
}];
```

The `prefix` and `instance` variables at the top cover pipelines that
rename things (telegraf: prefix `prometheus_`, worker label `host`. On
Kubernetes set `instance` to `pod` or `node`).

## Troubleshooting

**`… pass --eval-store auto`** — you ran `nix build --store grpc://…`
without `--eval-store auto`, so the derivations only exist in your local
store.

**`TLS handshake failed` / `could not reach the server`** — rerun with
`NIX_GRPC_DEBUG=1` (for the build hook, put it in
`systemd.services.nix-daemon.environment`) to see which CA bundle and
client certificate were loaded plus gRPC's handshake trace. On the
balancer, `accessLog = true` shows the same connection from the server side.

**`server requires a TLS client certificate or bearer token`** — the request
reached a worker with neither a forwarded certificate nor a token. Check
that the client passes `client-cert`/`client-key` or `token-file`, and that
envoy's `tls.clientCaFile` is set.

**`no access rule matches '<cn>'`** — the certificate verified but its CN
isn't in `accessRules`.

**`no connected, non-draining worker for system … offers features {…}`** —
no worker for that system (with those `system-features`) is connected to
the scheduler, or all of them are low on disk or stopping. Look for
`event=scheduler_disconnected` in the workers' journal.

**`asking the scheduler again` keeps repeating** — the client reaches a
different worker than the scheduler picked. A worker's `advertise` does
not match its entry in the balancer's `workers`.

**A worker shows `failed_active_hc`** — it's stopping or envoy can't
complete mTLS to it. Check `journalctl -u nix-grpc-daemon` on the worker.
