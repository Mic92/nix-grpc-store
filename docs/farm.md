# Set up a build farm

A build farm is a group of interchangeable workers behind one address.
Clients send builds to that address, each derivation is built once even if
several clients ask for it, and outputs land in an S3 binary cache through
[niks3](https://github.com/Mic92/niks3).

![build farm](farm.svg)

This guide walks you through setting up the cache, the workers, the
balancer, and two kinds of client: a CI host and a developer laptop.

## How it works

There is no scheduler. Coordination comes from two places:

* **The balancer** (envoy) routes each request by the `x-nix-system` header
  (and `x-nix-features` where workers differ, e.g. `kvm`) and picks a
  worker by hashing `x-nix-drv`, so the same derivation tends to reach the
  same worker. This is an optimisation only: every worker
  publishes what it accepts or reports as valid to the cache before
  answering, and substitutes from the cache whatever it lacks (uploaded
  paths' references, the `.drv` closure, build inputs, outputs to serve).
  Any request may land on any worker.
* **The cache** (niks3) hands out *build claims*. Before building, a worker
  claims the output paths and is told `built` (already cached), `wait`
  (another worker has it) or `build`. Results are published with the claim
  token, so a worker that lost its claim can't overwrite the winner.

Workers keep no state. You can add, remove or reboot them at any time.

## Before you begin

You need:

* A [niks3](https://github.com/Mic92/niks3) instance (any version with
  [build claims](https://github.com/Mic92/niks3/wiki/Build-Claims)), an S3
  bucket behind it, and its API token and public signing key.
* One or more NixOS machines per system type to act as workers.
* One machine with a public DNS name for the balancer. It can also be a
  worker.
* A small private CA. It signs three kinds of certificate:

  | Certificate | CN | Used by |
  |---|---|---|
  | worker server cert | any, SAN = address envoy dials | each worker |
  | balancer client cert | `lb-<host>` | envoy, towards workers |
  | CI client cert | `ci-<host>` | each CI host, towards envoy |

  The balancer's public listener uses a regular ACME certificate, so
  clients don't need the private CA.

All modules below come from `nix-grpc-store.nixosModules.default`.

## Step 1: Configure a worker

```nix
services.nix-grpc-daemon = {
  enable = true;
  listen = "10.0.0.5:50052";
  idleTimeout = null;
  tls = {
    certFile = "/run/keys/worker.crt";
    keyFile = "/run/keys/worker.key";
    clientCaFile = "/run/keys/farm-ca.crt";
  };
  trustedProxies = [ "lb-*" ];
  accessRules = [ { cn = "ci-*"; role = "trusted"; } ];
  farm = {
    enable = true;
    niks3Url = "https://niks3.example.com";
    tokenFile = "/run/keys/niks3-token";
    cacheUrl = "https://cache.example.com";
    publicKeys = [ "cache.example.com-1:…" ];
    maxJobs = 8;
    minFree = "20G";
  };
};
```

| Option | What it does |
|---|---|
| `listen` | Address the balancer connects to. Only the balancer needs to reach it. |
| `trustedProxies` | Peers with a matching client cert are balancers. The worker takes the real client identity from the `x-forwarded-client-cert` header they set. Other peers can't set it. |
| `accessRules` | Maps forwarded CNs to roles. CI needs `trusted`. |
| `farm.maxJobs` | Concurrent farm builds on this worker. Keep it at or below the local nix-daemon's `max-jobs`. |
| `farm.minFree` | Below this much free disk the worker reports unhealthy and the balancer stops sending work until space returns. |
| `workerName` | Name clients see in `worker: building …` lines and in `nix_grpc_build_info`. Defaults to the hostname. On Kubernetes the chart sets it to the node name. |

Repeat for every worker. Workers for different systems use the same config.

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
  # Only 10.0.0.5 has /dev/kvm. Drvs with requiredSystemFeatures = [ "kvm" ]
  # go there, a worker without the feature would bounce them.
  features.aarch64-linux.kvm = [ "10.0.0.5:50052" ];
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
| `workers` | Worker addresses per system. Requests for a system not listed go to `defaultSystem` (first entry by default). |
| `tls.certFile`/`keyFile` | Public server certificate. |
| `tls.clientCaFile` | Verify client certificates against this CA and forward the subject to workers. Clients without a certificate are still accepted so tokens work. |
| `tls.upstream.*` | The certificate envoy presents to workers, and the CA to verify them. |

To check that envoy sees your workers:

```console
$ curl -s localhost:9901/clusters | grep health_flags
x86_64-linux::10.0.0.4:50052::health_flags::healthy
aarch64-linux::10.0.0.5:50052::health_flags::healthy
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
it will build, and `system=` is how the balancer knows which cluster those
uploads (and `builders-use-substitutes` fetches) belong to. It also gives
each system its own upload lock on the client.

The hook runs inside `nix-daemon.service`. If you filter its egress with
`nix.firewall`, the module already allows port 50051; for another
balancer port set `programs.nix-grpc-store.daemonEgressPorts`.

For each derivation the hook sends one `BuildDerivation` call. If the
worker doesn't have the `.drv` closure, the plugin uploads it and retries.
The worker streams the build log back and the hook copies the outputs from
the worker when it's done. If another worker built them, the worker fetches
them from the cache first.

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

`--eval-store auto` is required. The farm doesn't accept the worker-protocol
tunnel, so derivations must exist locally and are uploaded per build.

The result is in the cache, not the local store. To fetch it:

```console
$ nix copy --from https://cache.example.com $(nix build … --print-out-paths)
```

Tip: wrap the token fetch (an OIDC device-code login is a few lines of
`curl`) and the `nix build` call in a script and expose it as
`nix run .#nix-farm`.

## Managing the farm

**Add a worker.** Deploy it with the Step 1 config, add its address under
`workers.<system>`, redeploy the balancer.

**Drain a worker.** Run `systemctl stop nix-grpc-daemon.socket` on it.
Envoy's health check ejects it, running builds finish, clients waiting on
it retry on another worker.

**See what a worker did.** Each build is one journal line:

```
event=rpc method=BuildDerivation cn=ci-build01 duration_s=42 …
```

Set `services.nix-grpc-daemon.logLevel = "debug"` to also log claim
decisions.

**Metrics.** Set `services.nix-grpc-daemon.metricsListen` on workers and
scrape envoy's admin port (`/stats/prometheus`). A Grafana dashboard for
both ships as `nixos/grafana/farm.json` (flake: `nix-grpc-store.dashboards.farm`):

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

**`is a build farm, pass --eval-store auto`** — you ran `nix build --store
grpc://…` without `--eval-store auto`, or something tried to open the
worker-protocol tunnel (`nix store info`, `nix copy --to`).

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

**A worker shows `failed_active_hc`** — it's draining (low disk, socket
stopped) or envoy can't complete mTLS to it. Check `journalctl -u
nix-grpc-daemon` on the worker.

**Builds queue but don't start** — `farm.maxJobs` slots are taken, or the
worker's local nix-daemon is at `max-jobs`. The farm slot is held while
waiting for the local one.
