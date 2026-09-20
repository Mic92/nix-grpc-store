# Run a build farm on Kubernetes

The `nix-grpc-farm` Helm chart runs the build farm from
[Set up a build farm](farm.md) on a Kubernetes cluster. This topic covers
installing the chart, connecting CI jobs that run in the cluster,
connecting machines outside it, and day-to-day operation.

Read [How it works](farm.md#how-it-works) first. The moving parts are the
same here: a scheduler, groups of workers, envoy as the balancer in
front, and [niks3](https://github.com/Mic92/niks3) as the binary cache
that receives every output. niks3 is installed from its own chart, see
[niks3 on Kubernetes](https://github.com/Mic92/niks3/wiki/Kubernetes).

## Before you begin

The chart expects:

* A niks3 release in the cluster, and its public signing key.
* [cert-manager](https://cert-manager.io). Without it, three TLS
  Secrets have to be created and rotated by hand.
* Privileged pods allowed on the worker nodes, for the Nix sandbox. The
  sandbox can be turned off instead.

## Step 1: Install the chart

Write a `values.yaml`. The example below runs four x86_64 and two
aarch64 workers, a standby scheduler, and lets CI pods in the `ci`
namespace use the farm with their service account.

```yaml
niks3:
  serverURL: http://niks3.niks3.svc       # API, as the workers reach it
  cacheURL: https://cache.example.com     # binary cache, as everyone reaches it
  publicKeys: ["cache.example.com-1:…"]
  auth:
    serviceAccountToken:
      enabled: true

workers:
  x86-64:
    system: x86_64-linux
    replicas: 4
    maxJobs: 2
    features: [big-parallel, kvm]
    nodeSelector: {kubernetes.io/arch: amd64}
  aarch64:
    system: aarch64-linux
    replicas: 2
    nodeSelector: {kubernetes.io/arch: arm64}

scheduler:
  replicas: 2

tls:
  certManager:
    enabled: true

auth:
  accessRules:
    - {cn: "ci-*", role: trusted}         # client certificates, see step 3
  workloadIdentity:
    enabled: true
    allowedServiceAccounts: ["ci:runner"] # <namespace>:<serviceaccount>, see step 2

metrics:
  podMonitor:
    enabled: true                         # or vmPodScrape for VictoriaMetrics
```

Install it:

```console
$ helm install farm oci://ghcr.io/mic92/charts/nix-grpc-farm \
    --namespace farm --create-namespace -f values.yaml
```

Then tell niks3 to accept uploads from the farm. In the niks3 chart's
values, add the farm's service account with scope `write`:

```yaml
auth:
  workloadIdentity:
    enabled: true
    allowedServiceAccounts: ["farm:farm-nix-grpc-farm"]
    scopes: [write]
```

When all pods are ready, `helm status farm -n farm` prints the farm's
address and configuration snippets for clients with the release's values
filled in.

## Adjust the farm to the cluster

The example above is enough to get going. The sections below show what
to add for the situations most clusters run into.

### Build without privileged pods

Builds run in the Nix sandbox, and for that the `nix-daemon` container
is privileged. On clusters that forbid privileged pods, turn the sandbox
off. Builds then run as root in the container with no isolation from
each other.

```yaml
sandbox:
  enabled: false
```

### Size the store and the disk watermarks

A worker's `/nix` is an `emptyDir` on the node's disk. When free space
there drops below `gc.ensureFree`, the harmonia-gc sidecar deletes
unused store paths. If it still drops below `minFree`, the worker stops
taking builds. Both thresholds are measured against the node's disk,
not per pod, so keep `ensureFree` well above `minFree`.
`store.sizeLimit` is not a quota: going over it evicts the pod.

For nodes with a 500 GB disk and two workers each:

```yaml
workerDefaults:
  minFree: 30G           # stop accepting builds
  store:
    sizeLimit: 200Gi     # evict a runaway pod before the node fills
  buildDir:
    medium: Memory       # build in tmpfs
    sizeLimit: 16Gi
gc:
  ensureFree: 80G        # start deleting
  keepRecent: 1d         # but never what was used in the last day
```

### Give builds CPU and memory

A worker pod has two containers. `nix-daemon` runs the builds, so put
the CPU and memory requests there (`resources`). `nix-grpc-daemon` only
handles RPCs and copies store paths, a few hundred MiB is plenty
(`daemonResources`).

```yaml
workers:
  x86-64:
    maxJobs: 4
    cores: 4             # per build, 0 = all
    resources:
      requests: {cpu: "8", memory: 16Gi}
      limits: {memory: 32Gi}
    daemonResources:
      requests: {cpu: 250m, memory: 256Mi}
```

### Certificates

If cert-manager is installed, let the chart use it:

```yaml
tls:
  certManager:
    enabled: true
```

cert-manager then creates a private CA for the farm and issues the
balancer and worker certificates from it. It also creates an Issuer,
`farm-nix-grpc-farm`, that signs the client certificates in step 3.

The balancer certificate is valid for the Service name,
`farm-nix-grpc-farm.farm.svc`. Clients that connect from outside the
cluster use a public name instead, so list that name too:

```yaml
tls:
  certManager:
    enabled: true
    lb:
      extraDnsNames: [farm.example.com]
```

Without cert-manager, the three Secrets come from elsewhere and the
chart only needs their names. The comments in `values.yaml` describe what each
certificate must contain.

```yaml
tls:
  clientCA: {existingSecret: farm-ca}        # ca.crt
  lb: {existingSecret: farm-lb-tls}          # kubernetes.io/tls, CN lb
  worker: {existingSecret: farm-worker-tls}  # kubernetes.io/tls, CN worker,
                                             # SAN farm-nix-grpc-farm-scheduler-0.farm.svc, …
```

### Run a standby scheduler

A second scheduler on another node takes over within seconds when the
first one goes away. The two agree on who is active through a lock in
niks3's database.

```yaml
scheduler:
  replicas: 2
```

Worker and balancer pods already prefer separate nodes (`spread: true`).
Scheduler replicas insist on it.

### Autoscale a worker group

The scheduler exports how many builds are waiting per system and
feature set (see [Autoscale builders](farm.md#managing-the-farm) for
the labels). A KEDA `ScaledObject` on a group's Deployment turns that
into replicas. Scaling down deletes pods, which drain like any other
pod delete.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: farm-workers-x86-64
  namespace: farm
spec:
  scaleTargetRef:
    name: farm-nix-grpc-farm-worker-x86-64
  minReplicaCount: 1
  maxReplicaCount: 16
  cooldownPeriod: 600
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: >-
          max(nix_grpc_sched_system{system="x86_64-linux",features="",kind="queued"})
          - max(nix_grpc_sched_system{system="x86_64-linux",features="",kind="unplaceable"})
        threshold: "4"       # queued builds per added replica
```

A group with `features: [kvm]` scales on `features="kvm"` in the same
way, independently of the plain group.

### Scrape metrics and install the dashboard

Enable the resource the monitoring stack watches. The dashboard
ConfigMap is for Grafana's sidecar. Open the dashboard with its
`instance` variable set to `pod`.

```yaml
metrics:
  podMonitor: {enabled: true}     # Prometheus operator
  # vmPodScrape: {enabled: true}  # VictoriaMetrics operator
grafanaDashboard:
  enabled: true
```

### Use another balancer

To put an existing gRPC-capable proxy or service mesh in front instead
of envoy, disable the bundled one:

```yaml
lb:
  enabled: false
```

The chart still creates the scheduler Service and one headless Service
per worker group. The replacement proxy needs these routes, in order:

| Match | Route to |
|---|---|
| path prefix `/nix.remote.Scheduler/` | Service `farm-nix-grpc-farm-scheduler` |
| header `x-nix-worker: <ip>:<port>` | that address (the worker the scheduler picked) |
| header `x-nix-system: <system>` | headless Service `farm-nix-grpc-farm-worker-<group>` of that system |
| anything else | the `defaultGroup` workers |

Backends speak HTTP/2 and answer `grpc.health.v1.Health/Check`.
`nixos/lb.nix` and the chart's `_envoy.tpl` are working references.

## Step 2: Connect CI jobs in the cluster

Pods in the cluster authenticate with their Kubernetes service account
token instead of a certificate. The farm verifies the token with the
API server and accepts it if `<namespace>:<serviceaccount>` is listed in
`auth.workloadIdentity.allowedServiceAccounts`.

A CI job then runs in a pod that has nix with the `grpc://` plugin,
mounts such a token, and passes the farm's address to nix. For the
first part there is a ready-made image,
`ghcr.io/mic92/nix-grpc-farm-client`: nix with the plugin loaded, git,
openssh, nix-eval-jobs and nix-fast-build. It works as the job
image of a plain Job as well as of GitHub Actions runner scale sets,
GitLab's Kubernetes executor or Buildkite agents.

1. Make sure the runner's namespace and service account are listed in
   `auth.workloadIdentity.allowedServiceAccounts` (step 1 allowed
   `ci:runner`).

1. Give the runner's namespace a copy of the farm CA, so pods there can
   verify the balancer's certificate:

   ```console
   $ kubectl -n farm get secret farm-nix-grpc-farm-lb-tls \
       -o go-template='{{index .data "ca.crt" | base64decode}}' \
     | kubectl -n ci create configmap nix-grpc-farm-ca --from-file=ca.crt=/dev/stdin
   ```

1. In the runner's pod template, use the image, mount the token and the
   CA, and set two environment variables: `FARM` with the store URL and
   `NIX_CONFIG` with the cache. `helm status farm -n farm` prints this
   block with the release's address and keys:

   ```yaml
   serviceAccountName: runner
   containers:
     - name: build
       image: ghcr.io/mic92/nix-grpc-farm-client:0.1
       env:
         - name: FARM
           value: grpc://farm-nix-grpc-farm.farm.svc:50051?token-file=/var/run/secrets/nix-grpc-farm/token&ca-cert=/var/run/secrets/nix-grpc-farm/ca.crt
         - name: NIX_CONFIG
           value: |
             substituters = https://cache.example.com https://cache.nixos.org
             trusted-public-keys = cache.example.com-1:… cache.nixos.org-1:…
       volumeMounts:
         - name: nix-grpc-farm
           mountPath: /var/run/secrets/nix-grpc-farm
           readOnly: true
   volumes:
     - name: nix-grpc-farm
       projected:
         sources:
           - serviceAccountToken:
               audience: nix-grpc-farm
               path: token
           - configMap:
               name: nix-grpc-farm-ca
               items: [{key: ca.crt, path: ca.crt}]
   ```

1. In the job, build against `$FARM`. Evaluation happens in the pod,
   builds on the farm, and outputs go from the worker straight to the
   cache. Nothing is copied into the pod:

   ```console
   $ nix-fast-build --store "$FARM" --no-download --skip-cached
   $ nix build --store "$FARM" --eval-store auto .#packages.x86_64-linux.default
   $ nix flake check --store "$FARM" --eval-store auto
   ```

   When a later step needs the result in the pod, to push a container
   image or deploy a closure, leave out `--no-download` and
   nix-fast-build copies each output back and links it as
   `result-<attr>`. With plain nix, `nix copy --from "$FARM" <path>`
   does the same.

Tools that cannot take `--store` can use the farm as a remote builder
from `NIX_CONFIG` alone. nix then uploads inputs and downloads every
output, so this moves more data than the commands above:

```
builders = grpc://farm-nix-grpc-farm.farm.svc:50051?token-file=…&ca-cert=… x86_64-linux - 8
max-jobs = 0
builders-use-substitutes = true
```

The client image is optional. Any image with nix works once the plugin
is installed, for example with
`nix profile install github:Mic92/nix-grpc-store` and
`plugin-files = /root/.nix-profile/lib/nix/plugins` in `NIX_CONFIG`.

## Step 3: Connect machines outside the cluster

CI hosts and laptops outside the cluster authenticate with a client
certificate, the same way as in the NixOS guide. cert-manager issues it.

1. Make the balancer reachable. Set `lb.service.type: LoadBalancer`, or
   route to the `farm-nix-grpc-farm` Service with a Gateway API
   `TLSRoute` in passthrough mode. Add the public name to
   `tls.certManager.lb.extraDnsNames` and run `helm upgrade`.

1. Request a certificate from the farm's Issuer. Its common name must
   match a pattern in `auth.accessRules`:

   ```yaml
   apiVersion: cert-manager.io/v1
   kind: Certificate
   metadata:
     name: ci-build01
     namespace: farm
   spec:
     secretName: ci-build01-tls
     commonName: ci-build01
     usages: [client auth]
     issuerRef:
       name: farm-nix-grpc-farm
   ```

1. Copy the three files out of the Secret and onto the machine:

   ```console
   $ for f in ca.crt tls.crt tls.key; do
       kubectl -n farm get secret ci-build01-tls \
         -o go-template="{{index .data \"$f\" | base64decode}}" > "$f"
     done
   ```

1. Continue with [Step 3: Connect a CI host](farm.md#step-3-connect-a-ci-host)
   or [Step 4: Connect a developer laptop](farm.md#step-4-connect-a-developer-laptop)
   of the NixOS guide, using these files and the public name from above.

The farm can also accept OIDC tokens from other issuers in place of
certificates, for example the ID tokens GitHub Actions hands to
workflows. Add the issuer under `auth.oidcProviders`. The format is the
same as niks3's OIDC configuration.

## Operating the farm

[Managing the farm](farm.md#managing-the-farm) describes draining,
failover, metrics and the dashboard. The Kubernetes equivalents:

**Add or remove workers.** Change `workers.<group>.replicas` and run
`helm upgrade`, or add another group. New pods take queued builds as
soon as they have registered with the scheduler.

**Drain a worker.** Delete the pod. Rollouts and scale-downs do the same.
The worker stops accepting builds, finishes and uploads the ones it has,
and then exits. Kubernetes waits up to `terminationGracePeriodSeconds`
(one hour) before it kills what is left, and those builds restart on
another worker.

**Restart or upgrade.** `helm upgrade` and
`kubectl rollout restart` are safe at any time. Workers are replaced one
by one and drain first. While the scheduler restarts, running builds
continue and new ones wait a few seconds. With two scheduler replicas
the standby takes over instead.

**Logs.** Each build is one line in the worker's `nix-grpc-daemon`
container:

```console
$ kubectl -n farm logs -l app.kubernetes.io/component=worker -c nix-grpc-daemon | grep event=rpc
```

Set `logLevel: debug` to also see the scheduler's decisions.
