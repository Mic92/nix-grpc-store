# Cross-cluster worker networking decision

## Decision rule (investigation in progress)

We want one farm with its scheduler/balancer on build1, workers on build1 and
build2, and one niks3 cache. Adding 20 workers on build2 must not require
20 Tailscale endpoints, per-worker services/forwarders, or changes on build1.
Outbound-only workers are optional; individually routable workers are fine if
the routing can be declared once per cluster. Preserve exact-worker build
routing and active/standby scheduler failover.

First test whether a build2 **node subnet** can be routed to the build1 Envoy
pods with Tailscale. If this requires only cluster-level routing/ACL and a
fixed balancer-side networking setup, prefer it over a new data-plane protocol.
If it needs per-worker or per-node routing configuration, fragile privileged
sidecars, or intrusive cluster networking changes, implement the worker tunnel
outlined below instead. The five-slot ingress/forwarder proposal in vortex
PR #86850 and tailscale-gitops PR #1251 does not meet the scaling requirement.

### Read-only cluster preflight (2026-10-21)

- Contexts: `build-blc` = build1, `build-dlr` = build2. Both Cilium clusters
  allocate pods from `10.244.0.0/16` (`kube-system/cilium-config`,
  `cluster-pool-ipv4-cidr`); routing build2's pod CIDR into build1 is ambiguous.
  Both use VXLAN and Cilium cluster-pool IPAM, so Kubernetes Node `podCIDR` is
  empty; CiliumNode objects carry the actual per-node pod CIDRs.
- Build2 nodes `fr02-s120-aw04-ct07` and `fr02-s120-aw04-ct08` currently have
  internal IPs `10.164.3.47` and `10.164.3.48` (both Ready). Build1 nodes
  currently use `10.62.25.x`. These two build2 IPs were not seen among
  build1's current pod or Service IPs. This does **not** establish the full
  allocated build2 node prefix or exclude all other local/advertised routes.
- Neither cluster currently has a Tailscale Connector. Existing egress
  ProxyGroups are for named egress Services, not a general IP-subnet route.
  `policy-base.hujson` has no auto-approval for these node IPs, and the
  build1 service grant covers named build2 worker Services, not node IPs.
  A read-only `tailscale status --json` from an existing build1 egress pod
  showed no visible route covering either build2 node IP; this view is not
  an authoritative inventory of every tailnet route.
- Existing Tailscale ProxyGroup pods run `tailscale/tailscale:v1.102.2`
  privileged with `TS_USERSPACE=false`. A test sidecar must use kernel
  routing in the **same pod network namespace** as the application container;
  a stand-alone egress ProxyGroup or `acceptRoutes` on a ProxyClass alone does
  not give arbitrary application pods a route. No hostPort probe was run yet.
- Two existing build2 `nix-builder-amd64` pods are SSH builders, not the new
  gRPC farm workers. Do not modify their Services, pods, or listening ports.
  The current build2 worker-only values advertise per-node VIPs; the farm's
  Helm worker template otherwise advertises `$(POD_IP):50051` and would need
  explicit node-IP/`hostPort` support after a successful network probe.

### Two-node, reversible proof of concept (documented only; do not start yet)

**Current stop point:** This is a plan, not authorization to run the test.
No Connector, route approval, scoped grant, probe pods, or sidecar credentials
have been created. Cluster queries so far have been read-only. Wait for an
explicit go-ahead before applying resources or changing tailnet policy.

**Experiment delivery:** The temporary Kubernetes resources can be created
with imperative `kubectl apply`; neither an `sre-infra` nor a `vortex` merge is
needed for the probe. Kubernetes manifests alone are insufficient: route
approval and a scoped grant for the build1 probe identity live in the
**tailnet policy**, outside Kubernetes. Arrange a temporary admin-console/API
policy change with its owners (or their preferred process), recording the
original policy and checking whether tailscale-gitops reconciliation would
overwrite it. Do not confuse an advertised route with an approved, usable
route. If direct temporary policy changes are not permitted or would race
GitOps, stop and agree on another way to stage the policy; no PR or merge is
presumed for this experiment. Permanent configuration, if the test succeeds,
should be proposed as reviewed GitOps changes.

**Gate 0: agree on scope before creating anything.** Confirm the build2
allocated node subnet with the cluster/network owner and inspect tailnet
advertised routes for conflicts. For the first probe, advertise *only*
`10.164.3.47/32` and `10.164.3.48/32`; never guess/advertise a broad `/24`.
Approve the temporary Connector routes through the normal Tailscale policy
workflow (autoApprovers or explicit admin approval) and grant a dedicated,
scoped build1 **probe identity** TCP access to those node destinations on an
agreed high test port. Ensure test identity tag ownership, key/secret handling,
policy validation, and route approval are agreed with the tailnet owners.
Prepare and review the exact temporary manifests and policy diff before
applying anything; do not make unrecorded live policy changes.

**Gate 1: isolated listener and client.** In build2 `build-platform`, create
two short-lived, labeled test listener pods pinned to ct07 and ct08, each
binding the *same* harmless test `hostPort` on its own node and responding
with a distinct node/pod identity. Check the port is unused and admission
permits `hostPort` before applying. No production worker, scheduler, or
`50051` listener is involved. In build1 `build-platform`, create one labeled
test pod containing an ordinary client container and a Tailscale sidecar with
kernel TUN, `accept-routes`, and no Tailscale DNS takeover. Use an ephemeral,
least-privilege Tailscale auth/state mechanism and remove credentials at
cleanup. Do **not** attach the sidecar to the production Envoy yet.

**Gate 2: prove *the actual packet path*.** From the ordinary client
container, connect repeatedly to each node IP at the chosen port. Confirm
different identities are returned, inspect `ip route get` from the shared
pod network namespace, the sidecar's accepted routes and traffic counters,
and the build2 listener logs. Compare against a build1 control pod without
the Tailscale sidecar: successful TCP alone is inconclusive if an existing
underlay route carried it. Check normal Kubernetes DNS/Service access and
connectivity to the farm/cache remain intact. Failure at any gate means stop,
record evidence, and clean up rather than widening privileges or routes.

**Gate 3: check the real scaling property.** Only after the `/32` test works,
obtain approval for the real, non-overlapping node allocation and replace the
two `/32`s with one authorized prefix. On a **third** build2 node, deploy or
move the same probe listener and reach it from the *unchanged* build1 client
pod; make no new egress Service, forwarder, ACL grant, or per-node build1
configuration. Restart the test sidecar/Connector to check reconnection.
If a third node needs another route/policy entry, this design has failed its
scaling criterion even though the original two connections worked.

**Gate 4: later farm canary, not part of the initial probe.** Add automatic
worker `NODE_IP:50051` advertisement and fixed `hostPort` in the worker chart,
and the proven egress mechanism to *each* Envoy replica. Confirm workers fit
the one-worker-per-node/port scheduling constraint, or choose an alternative
stable addressing method. Test an assigned build on the intended worker,
store upload/download and niks3, pod replacement, route failover, and
active/standby scheduler switchover. Check that Envoy cannot silently send a
pinned build to a different worker (its current override-host policy has a
least-request fallback). Do not cut over production based only on a TCP probe.

**Cleanup:** delete only the labeled probe pods, temporary Connector, and
temporary credentials; restore the previously recorded tailnet policy via
the same approved process, removing only the probe routes/grants. Verify the
tailnet routes/devices and both clusters return to their pre-test state. Do
not remove existing farm resources.

## Alternative if cluster-level routing fails: upstream worker tunnel

### Current local status and deployment gate (2026-10-22)

The `external-workers` branch has an **uncommitted, local-only** reverse-data
implementation. Direct workers remain the default. No build2 resources,
tailnet routes, or cluster policy have been changed for this work; the separate
Tailscale node-subnet probe remains paused.

- Assignment IDs fence `Expect`/`Done`/`Revoke` and exact-worker selection.
  Reverse registration is bound to a verified pod UID from an exclusive-audience
  projected JWT, an independently authorized TLS client certificate, and a
  session generation. The active scheduler owns the registry; loss of
  leadership or session replacement invalidates stale routes.
- The scheduler gateway validates the original caller and live assignment,
  mints a short-lived, method-scoped capability, and relays `BuildDerivation`
  to the selected worker over a bounded reverse stream. The production worker
  dispatches to its existing build routine without an insecure loopback re-dial
  or trusting client-supplied worker metadata. Direct-worker routing is
  unchanged.
- `UploadBuildInputs` has a scheduler gateway route and a production worker
  importer. The worker checks its live `Expect`, delegated caller and flags,
  spools NARs on disk, and requires a complete zstd frame and client
  half-close before installing spooled paths. Cancellation, stale assignments
  and queue overflow fail closed rather than falling back to the scheduler
  store or another worker.
- All 23 local Meson tests pass, the Darwin package builds, and
  `git diff --check` is clean. Tests include TLS relay to a **simulated** worker,
  large framed uploads, cancellation/revocation, and a real NAR transferred
  between isolated local Nix stores after spooling. They do **not** yet prove
  a real pinned build or store transfer through the production TLS worker
  dispatcher, including its signature policy.
- The client now has an assignment-pinned `UploadBuildInputs` serializer that
  reuses the bulk NAR framing and sends the full local derivation closure (a
  scheduler-store validity query would not describe the worker's store).
  The reverse `NOT_FOUND` branch remains **fail-closed** pending a real-store
  production TLS import test. The new serializer is compiled but has not been
  exercised through that path; it must not be considered a passing proof.
  In particular, an unsigned local closure sent by a write-role caller must
  fail the production signature check; the fixture must explicitly cover both
  authorized signed imports and this rejection before enabling the retry.
- The isolated-store `reverse-import` test now streams a poorly compressible
  NAR whose compressed bytes exceed the 4 MiB queue, with a concurrent and
  paced producer/consumer; it passes locally. An unpaced direct producer
  exhausted the bounded handoff and was rejected as designed. This still does
  **not** test the real TLS gateway, gRPC backpressure, or production dispatcher.
  The original unsigned path was content-addressed, which Nix legitimately
  accepts without a signature. Clearing its content address to model an
  input-addressed, unsigned derivation output makes
  `LocalStore::addToStore(CheckSigs)` reject it. This distinction now has a
  local assertion (and the trusted `NoCheckSigs` counterpart succeeds), but
  still must be tested through the production dispatcher
  and daemon trust boundary before enabling the retry.
  A locally generated binary-cache key now signs the simulated output and
  Nix verifies its signature independently; the gateway/worker still needs
  an end-to-end import test with that public key configured as trusted.
- An unpaced-producer assumption in `reverse-request-test` was racy against
  the intentionally nonblocking 4 MiB pipe. The successful-transfer test is
  paced; a separate `reverse-upload-test` now asserts overflow cancels the
  pipe and cannot acknowledge completion. The three focused tests passed ten
  repeated local runs each. This does not stand in for TLS production proof.
- The response-queue disconnect test now blocks on a same-sized terminal
  message rather than a smaller `OK` that could fit in remaining queue space;
  25 focused runs pass. The complete 23-test local suite and Helm/NixOS route
  parity check pass after these corrections; three repeated complete suites
  (69 test executions) also pass.

### Production TLS import fixture (2026-10-22)

The `reverse-production` Meson test now uses the production scheduler,
registration, gateway and worker dispatcher, a pod-bound JWT, and mTLS. Its
worker store is accessed through a **separate nix-daemon process** with only
root in `trusted-users` and a fixture-generated binary-cache public key in
`trusted-public-keys`. On Darwin the gateway process is an untrusted daemon
client; on the Linux builder pod the fixture runs as root, so the write-role
capability check, rather than daemon-user trust, enforces `check_sigs=true`. A
poorly compressible NAR exceeding the 4 MiB reverse queue is uploaded to one
live assignment twice by a separately certified **write-role** client: the
unsigned input-addressed path is rejected with `check_sigs=true` and remains
absent; the same path signed with that key is accepted. The reverse capability
also rejects `check_sigs=false` for a write caller before the importer. The
complete 24-test Meson suite and five repeated runs of this
fixture pass. The fixture's initial hang was a test-only second blocking
`Schedule::Read` caused by checking `!assignment` after calling `Read`.

The imported NAR reads back correctly from the isolated rooted local store;
`unix://` NAR readback through a diverted nix-daemon still tries the host
`/nix/store`, on Linux as well as Darwin. Darwin cannot build with a diverted
store. On the isolated `arm64-02` Linux builder pod (Nix 2.34.8), the fixture
now schedules a real signed derivation to the exact reverse worker, observes
`NOT_FOUND`, uploads that derivation to its live assignment, retries the pinned
RPC, checks its successful output path and reads the built content via the
rooted local store. Twenty repeated runs passed. This is a manual upload/retry
fixture, **not** the client-driven retry or a remote-cache publication proof.

The Linux run exposed a control/data ordering race: the worker's `Done`
retired the scheduler assignment before the gateway accepted the result. The
gateway now acknowledges the complete result over the reverse session before
the worker sends control-plane `Done`, keeping fencing active through delivery.
It requires matching scheduler and worker versions. `src/client/build.cc`
still keeps reverse `NOT_FOUND` fail-closed until client-driven closure upload,
build, readback and cache publication are proved. No staging connectivity or
cluster/tailnet changes were made for this local pod test.

**Local work required before a real deployment test:**

1. Complete the client missing-input path in `src/client/build.cc`: for a
   reverse build returning `NOT_FOUND`, send the derivation closure through
   `UploadBuildInputs` to that same live assignment and retry the pinned build.
    Do not enable the retry until a pinned build, output readback and
    publication pass through a build-capable isolated worker. Keep the existing fail-closed behavior in the
   meantime; never send the closure to the scheduler store or a random worker.
 2. Extend the production TLS fixture with the actual `GrpcStore` client
    missing-input path (not its current manual upload/retry), then prove
    output readback/publication through the intended cache. Large-NAR import
    and a manually retried pinned Linux build already pass locally.
3. Exercise multiple workers and concurrent builds/uploads, cancellation
   during transfer and build, assignment revocation, session replacement,
   stale routing, standby takeover, and recovery from interrupted streams
   against the production path. Confirm no request silently reaches a
   different worker and direct workers still behave as before.
4. Before rollout, validate mixed-version `Revoke.assign_id` behavior or use a
   coordinated scheduler/worker rollout (an older scheduler sends zero, which
   a new worker ignores). Decide whether short-lived offline JWT verification
   suffices for pod deletion/token revocation or TokenReview/live-pod
   validation is required. Check readiness and Helm/NixOS Envoy-route parity.

Only after these local gates should we identify and wire **scoped
build2→build1 staging connectivity** for a two-cluster test. Staging
connectivity is **not needed yet**; none of this authorizes starting the
paused Tailscale probe or making cluster changes.

### Implementation sketch (design draft; transport not yet selected)

Prototype work is now authorized for local development (2026-10-21). Do not
interpret this as authorization for cluster deployment or for the separately
paused Tailscale subnet probe. First slice: fence `BuildDerivation` by the
worker's current `(drv_path, assign_id)` Expect, and make newly issued IDs
different across scheduler incarnations. Direct local builds retain their
existing transport. Focused `scheduler` and `dispatcher-session` Meson tests
pass on macOS. The reverse data stream, routing gateway, worker-only deployment,
and cross-cluster connectivity are **not** implemented by this slice.

We have enough repository context to draft a *candidate*, but not enough to
commit to a transport yet. Keep the existing
client-facing farm URL, scheduler election, local direct workers, and niks3
publication. Put a shared, identity-aware RPC gateway on the farm side. A
tunnel worker opens an outbound authenticated connection to the active farm
and registers a stable logical worker ID, separate from its old IP `addr`.
The active scheduler maintains a generation-fenced mapping from ID to live
tunnel. Envoy routes tunneled build/store RPCs to the gateway, which selects
the exact worker for pinned builds and a live compatible worker for unpinned
store RPCs, then relays them over independent streams to that worker's local
RPC handler. The gateway must not interpret a client-supplied worker ID as
authorization to reach arbitrary workers.

**Proposed division of work:**

1. Specify worker IDs, registration handshake, transport capability/version,
   and assignment/session generation in `proto/nix_remote.proto`. Keep the
   existing IP-addressed `Hello.addr` / `Assigned.worker_addr` path for older
    workers and clients; do not overload an IP field with an opaque ID until
    every consumer (including EDS and Envoy) has an explicit handling path.
    Review client compatibility if a new routing header or token is required.

2. Introduce a worker-initiated, multi-stream data transport separate from
   `WorkerSession` so long NAR transfers and builds cannot block `Expect`,
   `Revoke`, or `Done`. Define frame/stream IDs, headers, trailers, terminal
   status, flow control, quotas, timeouts, cancellation, and stream-close
   behavior. Reuse the existing worker RPC handlers rather than duplicate
   store/build logic. Evaluate a conventional HTTP/2 reverse proxy or an
   existing gRPC transport before inventing application-level frames.
3. In `dispatcher`, `scheduler`, `coordinator`, and EDS, add tunnel capability
   and readiness to worker membership. A disconnected tunnel must make a
   worker unassignable; an old control or data session must never deregister
   a replacement. Fence `Done`, `Expect`, and routing by session generation
   and assignment, including the Hello-running reconciliation on failover.
4. Route tunneled and direct workers separately at the balancer. For a pinned
   build, require a valid, live assignment and return an explicit retryable
   error when that specific worker is gone; **never** fall back to a different
   worker. For unpinned store operations retain the current system-based
   semantics, including upload/query/download and `Connect`. Keep local
   workers on the existing direct EDS path. Update *both* Helm Envoy config
   and its NixOS counterpart (`checks.helm` compares them).
5. Authenticate the worker/tunnel and authorize each relayed RPC with the
   effective original caller role, credentials, and method policy. Do not
   trust forwarded auth metadata from clients or turn the gateway into an
   implicit trusted client. Preserve TLS/CA, OIDC, trusted-proxy behavior,
   client cancellation, and worker build timeout semantics.
6. Expose explicit health/readiness and bounded active-stream/byte metrics;
   run multi-worker, multi-Envoy, scheduler switchover, partial disconnect,
   stale assignment, long log stream, large NAR, and mixed-version tests.
   Ship as opt-in first; direct workers remain the default until the canary
   proves the complete path.

Local validation (2026-10-21): the Darwin `xfcc-test does not exist` failure
was a linker issue, not a missing Meson test dependency. Passing the raw
`-no_uuid` flag through `NIX_LDFLAGS` on Darwin in
`nix/packages/nix-grpc-store.nix` fixes it; the path-based Nix build completed
and all 12 Meson tests passed. The earlier mentions of an unresolved full
check above record the state *before* this fix. The current changes are still
local only and the staging image predates assignment-ID admission checks.

A short-lived experimental reverse-store implementation was compiled locally
and then removed before use: relaying into the worker's own gRPC listener via
plaintext loopback was incompatible with its TLS/ACL configuration, and its
slot lifecycle could race disconnects. Do not treat that compilation as a
tunnel PoC. The next implementation must first establish the worker-side RPC
dispatch boundary without bypassing authorization, define the authenticated
worker-ID binding and stream lifecycle/cancellation, and add an actual local
gateway test for one pinned build *and* a store transfer. Direct workers and
the cluster subnet probe are unchanged.

The scheduler now rejects `Done` for a different `(worker, drv_path,
assign_id)` instead of retiring the current placement or accepting stale
locality hints. The builder also reports completion only if its assignment is
still the current Expect; `Revoke` is likewise assignment-scoped. These are
prerequisites, **not** a reverse transport; they do not make direct Envoy's
override-host fallback fail closed.
The new `Revoke.assign_id` requires a coordinated scheduler/worker rollout:
an older scheduler sends zero, which a new worker will ignore. Keep the
staging image paired with its scheduler until mixed-version behavior is
explicitly tested.

Next local slice: `ReverseRegistry` owns exact-worker, generation-fenced
session handles and is cleared on scheduler leadership loss. A replacement
cancels its predecessor; late disconnects cannot evict the new handle. Its
test covers standby rejection, authenticated-ID matching at the registry
boundary, replacement, concurrent late close, unknown-worker fail-closed lookup, disconnect and
leadership change. The coordinator owns the registry, but **nothing registers
a data session or routes an RPC through it yet**. In particular, registration
must receive a peer ID from verified credentials, not a claimed Hello field.
This slice does not establish a tunnel, a pinned build or store transfer.
The current Helm chart issues a **shared** worker certificate whose CN is
`worker` (`deploy/helm/nix-grpc-farm/templates/certificates.yaml`). It cannot
authenticate 20 distinct logical IDs by checking `Hello.worker_id == CN`;
before any registration RPC is exposed, provision pod-bound projected tokens
with a tunnel-only audience (or distinct per-worker certificates) and test
rotation and replacement.

Next transport decision to prototype locally: use one worker-initiated
connection per data session, with independently cancellable RPCs and bounded
concurrency/bytes. Compare an HTTP/2 reverse-proxy approach (which may retain
the existing gRPC method framing) with an explicit framed stream **before**
adding either to the protocol. The gateway must identify and authorize the
original caller first; the worker must receive a verifiable delegated identity
or use the same service handlers with an explicit, scoped caller context. An
insecure local gRPC re-dial is not a substitute. Only after the worker-side
auth boundary is proven should the registry be connected to the control
session and EDS/Envoy routing.

### Opt-in worker identity boundary (local code, not enabled)

For the eventual reverse-data registration, use a **separate** OIDC verifier
with an exclusive `grpc://worker-tunnel` audience and a provider restricted to
the staging/build2 worker service account and namespace. A worker presents a
short-lived pod-bound Kubernetes projected service-account JWT over TLS; the
gateway verifies its issuer, signature, expiration, audience, subject, SA and
namespace. Require *exactly one* tunnel audience; a JWT also valid for the
client audience is denied. `WorkerIdentity::bind` derives `pod:<namespace>:<pod-uid>` from
verified claims and requires the claimed ID to match. A deployment's common
service-account subject or the chart's shared TLS CN alone is not unique
enough. A replacement pod has a different UID and cannot be claimed with the
old pod's token. Registering also requires elected-leader status and a live
control session bound to this ID; neither has been wired to this function.

This *does not* replace the existing client `Auth` verifier: the tunnel token
must not be accepted as a client `trusted` role or authorize store/build calls.
Keep tunnel OIDC config and audience separate from the client config, and
reject any attempts to register via TLS CN, anonymous ACL, or a normal client
bearer. The code only checks cryptographically verified claims: offline JWKS
verification does **not** check whether Kubernetes has since deleted a pod or
revoked its token. Before exposing registration, require a short projected
token lifetime and decide whether Kubernetes TokenReview / live-pod validation
is necessary for immediate revocation; secure the gateway against token
replay and constrain reconnections to its verified pod UID. File-watcher or
per-RPC token-file reads must handle projected token rotation. No cluster
tokens or certificates have been provisioned for this yet.
Session registration must have a deadline no later than its token expiration;
before then the worker must reauthenticate with a fresh projected token (or
reconnect) and the gateway must cancel the old data stream on expiry. A
one-time verification at stream creation must not grant perpetual access.
`WorkerIdentity::bind` returns the verified token expiry with the bound ID
so the future transport can enforce that deadline; it does not itself open
or renew any streams.

For relayed client calls, authenticate the original caller *at the gateway*
with the existing `Auth` rules, including OIDC and trusted-proxy handling.
The gateway must send a method-scoped, short-lived identity/capability bound
to the exact worker session and assignment, and the worker must verify that
capability before invoking the existing handler with a scoped `Caller`.
Never forward the caller's bearer token, believe arbitrary client metadata,
or re-dial the worker through an insecure local listener. `Connect` stays
trusted-only; `AddMultipleToStore` must retain its write-vs-trusted signature
handling and `StoreInfo` must reflect the caller's *original* role. None of
these delegations or transports are implemented by the identity boundary.

### Reverse transport choice for the local spike

Use an explicit, bounded gRPC bidi data stream, **not** `WorkerSession` and
not an HTTP/2 stream swapped underneath gRPC C++: the public synchronous
gRPC service has no supported API for accepting an arbitrary remote HTTP/2
stream as an already-authenticated server call. A transparent HTTP/2 reverse
proxy could retain wire framing, but would still require a separately
specified per-request caller identity and TLS termination at the worker; it
does not solve the authorization boundary. Frame only the needed NixRemote
methods initially: request start with method, worker ID, assignment (for
builds), scoped authenticated caller and stream ID; bounded data frames;
response headers/data/terminal status; explicit cancel. Limit bytes in
flight and concurrent streams, stop accepting on leader loss, and reject any
unknown method or stream ID. Do not buffer an entire NAR or build log. The
worker dispatches using the existing handlers with an *explicit* caller
context, never by forging XFCC, bearer headers, or loopback TLS. This is a
design direction, not implemented or validated yet. A one-worker local test
must prove both a pinned build and a store transfer before enabling it.

### Local registration stream (no data path)

`ReverseSession.Register` is an opt-in, worker-initiated gRPC stream on the
elected scheduler, enabled only with the dedicated reverse OIDC configuration
and a TLS listener. It accepts a single worker ID, binds it to the signed
pod UID from the separate tunnel-audience token, and registers a generation-
fenced handle in `ReverseRegistry`. It acknowledges that generation and waits
for disconnect. Replacement, loss of leadership, and token expiry cancel the
stream; any second message is rejected rather than interpreted as data. A
TLS-backed loopback integration test exercises registration, independent pod
IDs, replacement/late close, invalid tokens/IDs, plaintext rejection,
standby rejection, leadership loss, and expiry. The existing direct-worker
path remains unchanged; an opt-in daemon worker can now originate this stream,
but no deployment enables it.

This is **not yet safe to use as a data tunnel**: control and data can share a
verified pod identity, but production client handlers are not wired to relay
frames and there is no worker-side caller-aware data dispatch or pinned build
and store transfer through it.
The option's provider config and audience must be dedicated to registration;
before rollout, pin the issuer/provider configuration, review token-replay
and Kubernetes revocation semantics, and verify mixed-version behavior.

### Reverse frame contract (partially wired locally; not operational)

**Oct 21 local increment:** `ReverseRelay` now serializes bounded unary
requests on a generation-fenced, scheduler-selected pod data session and
decodes bounded streamed responses. The production scheduler-only
`BuildDerivation` handler selects that path for a live pod assignment;
certificate-only direct workers still use their existing route. The relay
checks replacement, assignment ownership, client cancellation and a bounded
deadline, and refuses to fall back to a scheduler's local store. The TLS OIDC
fixture exercises an authenticated client RPC over a live `Register` stream,
with `ReverseRequest` validating its signed open, framed request and half
close before a test responder returns framed data. The responder is **test
only**: the fixture does not run the production worker build or transfer a
store path. The production worker build branch now exists but remains
unproven by a real local build.
The worker request decoder and late-cancel response tests now run locally;
the client upload/download RPC shapes are not yet connected to the relay.
Cancellation and completion races still require a production-worker TLS
integration test. Do not enable a reverse build in staging based on the
simulated TLS responder.
The TLS fixture additionally relays an actual `BuildDerivation` RPC shape:
its test worker verifies the decoded request against the signed capability
and current dispatcher assignment, then returns a **simulated** build chunk.
It does not call the real worker build handler or prove a pinned Nix build.
**Worker dispatch increment:** the opt-in worker data session now validates incoming
frames and bounded complete requests, then invokes the existing build routine
directly with the verified delegated caller (never through a loopback gRPC
re-dial). Up to four executor threads per registration keep the reader and
separate control stream responsive; responses pass through a bounded,
single-writer queue. Stream-specific cancellation and replacement tear down
pending work. Tests now cover completion after cancellation; the reader does
not wait behind a full response queue to enqueue cancellation status. This
session also closes its response queue before joining build executors, so a
blocked log producer cannot hang disconnect teardown. This
is wired production code, but **not yet a local pinned-build proof**: the TLS
integration fixture still has a simulated responder, and
the authorized reverse store-transfer path has not been proven. Do not
enable the reverse worker in staging until those gates pass.
The next local slice defines `UploadBuildInputs` as a separate client-streamed
RPC with assignment coordinates only on its first frame. Its gateway
capability must be minted from the authenticated client and exact live
assignment. The gateway RPC forwards bounded upload frames through the exact
reverse session. The worker now accepts upload Opens into a bounded pipe and
runs the existing local Nix importer under the delegated role. It checks its
live `Expect` before importing and before publishing, and requires the client
half-close with no trailing data or incomplete zstd frame **before installing
any spooled path**. This production path compiles, but
has **not** passed a real pinned-build or store-transfer integration test;
do not deploy it yet. No upload falls back to the scheduler.
The registration reader checks `Expect` on Open and on each decoded upload
chunk, with a bounded queue that fails closed on overflow instead of waiting
behind the importer. A stop/disconnect cancels upload sources. Tests cover
revocation (including while the importer waits for bytes), stale coordinates,
cancellation, >4 MiB of upload chunks, and
decompression of a real 5 MiB zstd payload, including rejection of truncated
and trailing zstd input; those framing tests do **not** import a NAR.
`reverse-import-test` now also exports a real NAR from an isolated temporary
Nix store, frames and compresses it as an upload, then imports it into a
second isolated store only after spooling, validating the complete zstd
frame and half-close; it verifies identical NAR contents. This exercises
the transport decoder, zstd source and Nix import framing with a trusted
test caller; it does **not** use the production TLS worker dispatcher, its
signature policy, or a real pinned build.
`ReverseFrames::waitData` now provides bounded, cancellable producer
backpressure on the gateway side. Worker-side queue overflow fails closed
without blocking the shared registration reader behind an importer.
`ReverseUpload` validates a bounded first frame against the upload-specific
capability and requires later frames to omit assignment coordinates and flags.
These shape tests are not an end-to-end NAR upload.
The worker-side `ReverseUpload::Decoder` now reconstructs bounded protobufs
across 64 KiB transport frames and hands them off one at a time; malformed,
partial, and mismatched-assignment frames fail closed. The decoder test
feeds more than 4 MiB in many chunks; it tests bounded framing, **not** a
store import. A TLS gateway relay test also sends more than 4 MiB through
the bounded queue to a simulated worker that returns `UNIMPLEMENTED`; the
client receives that worker status. Helm and NixOS Envoy now route the exact
upload RPC to the scheduler (not by a client-supplied worker header).
The next gate is a TLS fixture exercising the **real** worker dispatcher and
store importer, with a pinned build and a real transfer larger than 4 MiB.
Only a successful worker finish can be returned as upload success; cancellation,
replacement, invalid flags, or stale assignments must not fall back to the
scheduler store. Do not enable client retry after a build's `NOT_FOUND`
response until that real-store test passes.
Scheduler `Assigned.reverse` now marks pod-bound workers for the client; the
client sends those build RPCs to a dedicated Envoy scheduler route (the header
is a routing hint, not server-side authorization). Direct assignments keep
the existing worker route. A reverse build returning `NOT_FOUND` fails closed
instead of uploading inputs to the scheduler or a random direct worker; the
store-transfer path still needs a real local proof.

The proto reserves `stream_id`-keyed open/data/half-close/cancel frames in
the farm→worker direction, and data/finish in the worker→farm direction.
The paragraphs below record earlier local milestones; statements that they
were not yet connected are superseded by the current local status above.
Production **upload dispatch** exists, but client missing-input retry and a
production TLS build/store-transfer proof remain unimplemented.
`ReverseFrames` is a stand-alone bounded outgoing queue (64 KiB data frames,
4 MiB queued per session with control-frame headroom, 64 logical streams); tests cover unknown methods,
unopened and stale IDs, never-reused IDs, duplicate opens, half-close, cancellation, queue
exhaustion and teardown. No handler uses this queue; the separate capability
and inbound/gateway primitives are not connected. `Register` still rejects **all** frames after its
registration acknowledgement. Merely supplying nonempty capability bytes is
not authorization. This protocol reservation does not constitute a local
pinned-build or store-transfer proof; worker dispatch and the authenticated
gateway boundary must precede enabling frames on a live session. The standalone
inbound validator checks individual frames, but live receive-queue limits,
response ordering, gRPC cancellation, and per-request deadlines remain unimplemented.

### Per-registration caller capability (local-only, not dispatched)

The registration ack now delivers a freshly generated 256-bit HMAC key over
the pod-bound TLS stream, and the generation-fenced registry holds the same
key only for the matching registration. `ReverseCapability` mints/verifies a
30-second capability tied to that worker ID, session generation, never-reused
stream ID, method, caller name/role, and (for builds) drv path, assignment ID,
and build mode. Verification rejects tampering, wrong key/worker/generation/
stream/method, expiry, unknown methods, underprivileged roles, and repair builds
without trusted role or an authenticated named caller. Request-level helpers check a decoded build against its
capability and reject unsigned/repair uploads from write-role callers. Tests
exercise replacement key rotation and these boundaries.

**Not an operational authorization path yet:** the gateway helper is exercised
by a test RPC but has no production inbound hookup; the worker has no frame reader or handler dispatch,
so no capability is consumed by a live RPC. The key and capability must never
be logged or accepted from client-supplied metadata. The worker will need to
verify each open against its active session and apply the original caller to
the existing handler, then cross-check the decoded request and the live
assignment. Registration continues to reject all data frames.

`ReverseInbound` now locally validates worker-bound incoming open/data/
half-close/cancel frames and refuses duplicate/late IDs, forged key or
generation fields, missing/invalid capabilities, oversized data, and data
after half-close. Its events are **not** connected to a gRPC stream or any
NixRemote handler; the queue, inbound parser, and capability verifier remain
separate primitives until a bounded dispatcher and caller-aware handler
boundary exist. A debugger traced sporadic Darwin post-test crashes to
OpenSSL static cleanup overlapping gRPC event-engine thread termination.
The TLS integration tests now finish contexts before shutdown, explicitly
quiesce gRPC, and opt out of OpenSSL process-exit cleanup in those test
binaries; repeated local runs pass. Confirm with the full Nix build.

`ReverseResponses` now independently validates worker→gateway data and
bounded final statuses on exact, non-reused stream IDs, with a 4 MiB session
queue, final-status headroom, cancellation cleanup, and fail-closed handling
of unknown/late/oversize frames. Blocking per-stream dequeue cannot steal
another caller's response; the outgoing queue can wake a dedicated writer
without polling. Both are still disconnected from `Register`,
which rejects data after registration; no live request or response is relayed.
`reverse-roundtrip-test` composes the standalone queue, inbound verifier,
response validator and registry in memory, including build-request assignment
matching and rejection of replay after registration replacement. This is a
contract test only; it does **not** involve gRPC transport or run a build.

The TLS `Register` stream now has a dedicated writer for bounded gateway→worker
frames and validates worker→gateway frames only against reserved, exact stream
IDs. Reconnect/leadership loss closes both queues and wakes blocked callers.
The OIDC integration test exercises two interleaved requests and responses
over a real TLS bidirectional stream; it manually reserves the streams and does not dispatch a
NixRemote request. There is still no client-facing relay, worker dispatch, or
pinned-build/store-transfer proof.
The `ReverseGateway` now has a separate enqueue boundary that rechecks the
current exact session, build assignment and signed stream ID before reserving
the response and enqueueing an open; tests cover reassignment, displacement,
and tampering with the prepared assignment. No production handler calls this
boundary yet. Full Darwin package builds have passed 19/19 tests with the
control identity/address fencing and production TLS scheduler tests.
`Dispatcher::ownsAssignment` now provides a lock-protected read of the actual
live scheduler assignment, worker address, active WorkerSession ownership and
remaining followers. Tests deny stale IDs, cancellation, worker loss and
replacement Hellos without a matching running assignment. It is not yet
wired as the gateway's assignment callback or to a client RPC.
`Dispatcher::assignmentWorker` also derives the worker address from a live
derivation/assignment pair, rather than accepting a client routing header.
Important mismatch: `Hello.addr` currently advertises a pod IP (`$(POD_IP):50051`),
while `ReverseRegistry` is indexed by a JWT-verified `pod:<namespace>:<uid>`.
An opt-in control-session binding now verifies the same exclusive-audience
pod JWT independently of the shared TLS worker certificate and matches its
pod UID against `Hello.reverse_worker_id`. The dispatcher tracks that UID
with the current WorkerSession, refuses replacement by a different pod on
the same address, and `assignmentPod` returns it only for a live assignment
whose control credential has not expired. Cert-only direct sessions remain
supported but never yield a pod route. `podOwnsAssignment` is the exact-ID
predicate to supply to the gateway once its live handler is wired. The OIDC
integration test now uses the production `SchedulerService` with real mTLS and
an independent tunnel-audience JWT: a live `Expect` resolves to the verified
pod UID, while a mismatched UID, wrong audience, missing token or missing
client certificate or duplicated bearer cannot create a reverse route. The
daemon now has opt-in `--reverse-worker-id` and `--reverse-token-file` flags:
it sends the projected tunnel token on both control and data sessions, sets
`Hello.reverse_worker_id`, and re-registers data separately on disconnect.
The gateway also requires a directly authorized trusted TLS client certificate
for data registration, independently of the signed pod token. The worker
reader currently cancels any incoming data frame; no local RPC dispatch exists.
The same TLS test confirms that an existing cert-only direct worker can
receive an `Expect`, but `assignmentPod` returns no reverse route for it.
It also opens control and data registrations for the same signed pod on the
same TLS server, verifies their identity matches while assigned, and checks
that data-session replacement fences the old generation without displacing
the control assignment. Closing the data session leaves the control assignment
intact but no usable data route.
The same local TLS fixture exposes a test-only NixRemote handler using
`ReverseGateway::prepareBuild` wired to `Dispatcher::assignmentPod` and
`podOwnsAssignment`: a forged `x-nix-worker` cannot change its selected pod;
data disconnect and scheduler cancellation fail closed. Its newer test-only
relay sends request frames and returns a simulated result; it does not invoke
the worker's production handler or run a real build.
Existing direct WorkerSessions using ordinary scheduler bearer tokens continue
through the normal client Auth path; an exclusively verified tunnel token must
also have an authorized TLS client certificate to bind a reverse pod. A normal
bearer or cert-only session cannot claim `Hello.reverse_worker_id`. Neither a
different verified pod nor a direct worker can replace a live pod-bound address;
a pod-bound worker likewise cannot take a live direct worker's address.
Never use the client `x-nix-worker` header as proof. Neither a green build
nor an OIDC-only data registration proves the control/data binding end to end.

`ReverseGateway::prepare` provides a separate client-auth
boundary: it identifies the real incoming gRPC peer, rejects anonymous
delegation even if direct anonymous access is enabled, authorizes the method
and build mode, checks a scheduler-owned exact-worker assignment for builds,
finds only the selected live registration, and mints a capability for that
session. The local TLS test checks authenticated minting, role/repair and
assignment refusal, unknown-worker and standby refusal, replacement, and rejection of
forged client metadata. This is **not yet an operational gateway**:
`prepareBuild` now accepts a scheduler-owned derivation/assignment→pod resolver
and fails closed when the selected pod has no registration; its TLS test
checks that a nonexistent pod does not fall back to the previous worker.
`prepareBuild` authorizes the real client before asking the scheduler for a
route, so an anonymous caller cannot probe live assignment existence.
The production scheduler-only BuildDerivation handler now uses that assignment
callback and the framed relay. Worker-side dispatch and client store-transfer
RPCs remain unwired, and there is no local pinned build/store transfer proof.
Direct-worker routing remains the default.

**Design decisions still needed before implementation:**

- **Gateway placement and reachability.** An Envoy replica cannot look up a
  tunnel held by an arbitrary scheduler pod. Candidate: make the elected
  scheduler also host the gateway and route all tunneled RPCs via the existing
  active/standby scheduler cluster. Prove health checks remove a yielding
  scheduler and that no load-balanced hop can land on a standby; identify the
  data-plane capacity and failover impact of sending *all* remote store data
  through the leader. Alternatively design shared gateway replicas plus an
  explicit tunnel-location registry, with correspondingly more complexity.
- **Transport and auth implementation.** The local spike chose a bounded
  framed bidi protocol and a per-session caller capability. Wire both to
  live calls with bounded receive queues and worker-side handler dispatch,
  preserving each original client's role rather than granting worker-wide
  trusted privileges.
- **Pinned-build semantics.** `BuildDerivationRequest.assign_id` is checked by
  the local worker code but not by the deployed staging image. Validate it
  with a pinned end-to-end build before deploying it. Specify binding to the expected drv and
  worker/session generation, handling retries/second attach and post-failover
  reassignments. Envoy `override_host` currently has a least-request fallback;
  the new pinned path must fail closed even during endpoint churn.
- **Failure model and rollout.** Decide how in-flight calls behave when the
  leader or tunnel disappears, how the next leader reconstructs its registry
  from reconnecting workers, and how old clients/workers negotiate direct vs.
  tunneled routing without misinterpreting a worker ID as an IP. Benchmark
  gateway memory/CPU/network and concurrent uploads before committing to a
  single-leader data plane.

These are *architecture choices*, not reasons to implement the entire plan
up front. A short protocol/sequence design and a narrow end-to-end spike
(one worker, one pinned build and one store upload via the proposed gateway)
should settle them before broad changes to the scheduler, chart, or client.

### Local test kickoff

The cluster subnet probe above remains paused. For the tunnel option, start
locally with a session-fencing regression: when a second WorkerSession
registers the same worker address, late messages and closure of the original
session must not deregister the new session or steal its assignments. This
exercises the control-plane invariant needed before a reverse data channel
can rely on worker identity. The farm end-to-end harness under
`tests/e2e/farm/` (two workers, Envoy, active/standby schedulers) is the
subsequent proving ground for a tunneled build and store upload. This
regression alone does **not** demonstrate the data tunnel or settle its
transport/auth design. Do not deploy anything to either cluster as part of
these local tests.

Local result (2026-10-21): the modified daemon and new regression executable
compile under a path-based Nix build (the usual Git-backed `.#default` omits
the untracked test file). Direct Meson tests `dispatcher-session` and
`scheduler` both pass. The new regression covers replacement by a second
session with the same address, rejection of messages from the displaced
session, late closure, scheduler restart notifications, and delivery of an
assignment only to the replacement. The Nix package's check phase stopped
before running tests with `xfcc-test does not exist`; resolve that
check-environment issue separately before calling the full package build
green. No data tunnel has been implemented or tested yet. A separate
attempted dependency override was blocked by GitHub API rate limiting; it
is not a build or regression-test result.

### Imperative build1 staging farm (baseline deployed 2026-10-21)

Use `nix-build-farm-staging` as the staging namespace and Helm release on
build1, with staging-only certificates, service account, and a cluster-internal
client endpoint. Use an independently provisioned **staging niks3 instance
with its own election database and cache storage/signing key**; merely
renaming the farm release or Kubernetes namespace does not isolate it from
the production niks3 `/api/farm/lead` lock. Point both staging workers and
staging schedulers exclusively at that niks3 instance. Do not reuse the
production cache write token, hostname, client ingress, or worker credentials.
If staging needs production cache hits, consider a read-only upstream
substituter separately; never point the staging scheduler at the production
niks3 API. Budget for the namespace's unprivileged Nix pod user namespaces,
storage, and image pull permissions before deployment.

For fast iteration, keep staging values outside the production
ArgoCD ApplicationSet and use an explicit namespace/context and Helm release
for each manual upgrade. Start with one build1 scheduler, one build1 worker,
one balancer, an internal-only client, and a pinned, immutable test image.
Verify a small build, exact-worker routing, upload, cache readback, and the
worker's identity in the logs. Only then add one build2 worker via the
candidate tunnel (after the tunnel transport exists); verify its build and
upload without a per-worker VIP or a production policy change. Expand to two
schedulers and two workers for failover, reconnect, cancellation, and stale
assignment tests. Keep a Helm release history/values snapshot for rollback;
remove only labeled staging resources on teardown. Use a distinct SA/OIDC
grant for the staging niks3 endpoint rather than copying a production grant.

This setup tests the **farm protocol and deployment**, not whether a Tailscale
node-subnet route works: that cluster network probe remains separately gated
above. Current code has no reverse data path, so a staging release today can
validate the baseline and image/credential plumbing, but cannot yet establish
that a build2 worker can serve a build through the tunnel.

Gate-1 baseline result: after agreement to disposable storage, created only
`nix-build-farm-staging` on `build-blc`, with `emptyDir` Postgres and RustFS
(`deploy/staging/cache-backends.yaml`), a staging bucket, independent cache
token/signing key/DB credentials held in namespace Secrets, and a dedicated
niks3 Helm release (`deploy/staging/niks3-values.yaml`). The cache has an
internal read proxy; neither cache nor farm has external ingress. The farm
release (`deploy/staging/build1-values.yaml`) has one scheduler, one ARM
worker, and one ClusterIP balancer with staging-only TLS. All six Deployments
are ready. Loss/replacement of either backend pod destroys its `emptyDir`
data; the secrets, including the signing key, are not tracked in Git.

The `deploy/staging/smoke-test.yaml` Job built `staging-smoke` on the ARM
worker (confirmed in its first run's output), signed and uploaded the output
to staging niks3, fetched its `.narinfo`, copied the NAR back through niks3,
and checked the file contents. The final Job completed. The first two Job
attempts failed only in readback checks (pure evaluation of `/job/job.nix`,
then disabled niks3 read proxy); these were corrected. NikS3 `v1.9.0` lacked
the chart's `--db-file` interface and `v1.11.0` lacked `/api/farm/lead`;
the isolated release now uses `v1.12.0-beta.3` and the scheduler acquired
leadership in its **separate** database.

Updated staging image (2026-10-21): built `path:$PWD#packages.aarch64-linux.docker`
from the uncommitted working tree, including session-ownership fencing, on
the build1 ARM builder. Pushed the linux/arm64 worker/scheduler image to
`europe-west4-docker.pkg.dev/mistral-science/mistralai/nix-grpc-farm-staging-3f47ea5-20261021133401`
and pinned its verified registry digest
`sha256:e81611b0a3fe67761f7c511dac86de05cadd3f47c84db58872c217daca8a4692`
in `deploy/staging/build1-values.yaml` (farm Helm revision 2). A staging-only
ExternalSecret `deploy/staging/gar-pull-secret.yaml` supplies GAR read access;
the balancer and smoke client remain upstream `0.2.1`. All Deployments are
ready and the new scheduler acquired leadership. A *new* derivation,
`staging-smoke-session-fence`, was built by the new ARM worker, uploaded to
staging niks3, then fetched by the client and verified. The first smoke rerun
reused the old cached output, so it was not counted as a fresh build.

This verifies normal build/cache plumbing with the changed image, **not** the
session-replacement race, exact-worker routing across multiple workers,
failover, or a reverse data tunnel. Test replacement/failover separately
before considering gate 2. No build2 resources, Tailscale routes, or
tailnet-policy changes were made. The full Nix check issue noted above is
still unresolved.

Build1 scale/failover exercise (2026-10-21): Helm revision 3 scaled the
digest-pinned ARM workers to two with required node spread. The staging
scheduler reported two connected workers on different nodes. Fresh concurrent
builds from `deploy/staging/two-worker-test.yaml` landed on distinct workers,
and both outputs were downloaded and verified from staging niks3. The first
iteration's `sleep` was absent in the sandbox, so a second iteration used
`read -t 15 x < /dev/zero`; that Job lasted roughly 35 seconds and again
landed on two distinct workers. This confirms normal assignment through
Envoy, not that its override-host fallback will fail closed for a missing
pinned worker.

Helm revision 4 added a second staging scheduler (separate pods/nodes). On
deleting the original active pod, its replacement reacquired the election
lock before the standby, as niks3 prefers the incumbent. This **was not**
standby failover. A controlled `kubectl scale` of that staging scheduler to
zero then made the other scheduler log `scheduler_take_over`, reconnect both
workers (`nix_grpc_sched{kind="workers"} 2`), and complete fresh concurrent
two-worker builds with cache readback. The former leader was restored to one
replica afterward, leaving two workers and two schedulers in staging. This
tests takeover after the old leader exits, **not** uninterrupted in-flight
builds, session replacement races in Kubernetes, or tunnel behavior. No
build2 resources or tailnet policy were touched.

## Problem

We want one `nix-grpc-store` scheduler and balancer on build1, local ARM
builders there, and x86 builders on build2. Today each build2 builder must
have its own address reachable **from** the balancer. In Kubernetes that means
per-worker ingress, return routing, and a stable advertised IP:port through
restarts. We would like to add or remove a remote builder by connecting it to
the farm, without allocating a new inbound address or changing the balancer's
network configuration for that builder.

This is a request for an upstream **protocol/data-path capability**, not just
a Helm chart option. A shared TCP load balancer in front of several workers
cannot route a request to the particular worker chosen for a build.

## What the current protocol does

The builder opens a bidirectional `WorkerSession` to the scheduler. Its
`Hello.addr` is currently an advertised IP:port. The scheduler sends `Expect`
and `Revoke` over that session, but the session does **not** carry build or
store RPCs. An assignment returns `Assigned.worker_addr` to the client; the
client sends it as `x-nix-worker` to the balancer. The scheduler also publishes
connected builders as Envoy EDS endpoints. Envoy then opens a separate
connection to the selected builder for `BuildDerivation`, and uses builder
endpoints for store operations such as uploads, queries, and downloads.

Therefore worker-initiated registration alone does not eliminate the return
connection. Giving multiple builders the same advertised address lets Envoy
dial the wrong one. The current EDS implementation also requires a literal IP
for each endpoint, so advertising a DNS name would not solve the underlying
per-worker routing problem.

Upstream references: [farm architecture](https://github.com/Mic92/nix-grpc-store/blob/main/docs/farm.md#how-it-works),
[`proto/nix_remote.proto`](https://github.com/Mic92/nix-grpc-store/blob/main/proto/nix_remote.proto)
(`Assigned`, `Hello`, `WorkerSession`), and
[`src/daemon/eds.cc`](https://github.com/Mic92/nix-grpc-store/blob/main/src/daemon/eds.cc).

## Desired behavior

1. A remote builder makes **outbound-only** connections to one stable farm
   endpoint. The balancer/scheduler needs no per-builder routable IP:port,
   ingress Service, pinned return Service IP, or forwarding pod. One endpoint
   can accept many workers and automatically incorporate new connections.
2. Registration yields an opaque, connection-independent **worker identity**
   (and a session generation), distinct from a network address. The scheduler
   still places work by system, features, free slots, and locality, and still
   announces `Expect` before sending an assignment to the client.
3. For a pinned build, the client carries the selected identity (or an opaque
   assignment token) to the balancer. The balancer routes **to that exact
   worker**, not a randomly chosen member of a shared ingress. Unpinned
   uploads, queries, and downloads retain their existing system-based routing
   semantics. Both build and store RPCs must have a path to a remote worker.
4. A lost session removes the worker from selection promptly. Reconnect,
   scheduler restart/failover, pod replacement, and graceful drain preserve
   the existing reassignment and in-flight-build semantics; stale assignments
   must never be delivered to a different worker that reuses an identity.
5. Existing local, directly addressable workers and clients continue working
   during a phased rollout.

## Suggested upstream shape: a worker-initiated data tunnel

The simplest operational model would extend the worker's outbound connection
with a **multiplexed, bidirectional data plane** (or add a second outbound
data connection associated with the `WorkerSession`). The farm accepts the
connection and binds it to an authenticated worker identity. When a client
RPC reaches the balancer, a farm-side gateway looks up the selected live
session, opens a stream to that worker, and relays the gRPC request, response,
metadata, trailers, and cancellation. This includes long-running build streams
and large store transfers, not just scheduling messages. The worker handles
the RPC using the same build/store implementation as a directly connected
worker. The scheduler's `Expect`/`Done` bookkeeping and niks3 publication
should remain the source of truth.

The gateway could be implemented in the daemon or beside Envoy, but it must
have a real mapping from worker identity to the live tunnel. Simply changing
`Assigned.worker_addr`, allowing DNS in EDS, or putting an L4 balancer in front
of the workers is insufficient. The client should continue to see the same
single farm address; ideally its existing gRPC store/build-hook protocol would
need no change beyond an optional opaque routing value. If the routing header
changes, specify a backward-compatible version/feature negotiation path.

An alternative is a **shared ingress proxy that routes by worker identity**
and forwards to outbound worker tunnels. That still requires the proxy/tunnel
protocol and an exact-worker lookup; it merely moves the gateway boundary.
It should not require one listener, port, certificate, or Tailscale VIP per
worker. A shared HTTP/2/gRPC gateway may be preferable to forwarding raw TCP
if the worker identity is available in RPC metadata.

## Correctness and security requirements

- Authenticate the worker when it establishes a session and bind its identity
  to that session. Do not trust a client-supplied worker ID or forwarded
  identity header without validating its scheduler-issued assignment and
  authorization. Preserve the current farm CA/OIDC, client roles, and
  `trustedProxies` semantics across the relay; workers must not acquire broader
  privileges simply because they can phone home.
- Specify how assignments are tied to a session generation, how they expire,
  and how an old tunnel is fenced when a worker reconnects. Keep `Expect`
  ordering, concurrent builds, attachment of a second client, and the existing
  `Revoke`/`Done` outcomes correct when a tunnel disappears mid-build.
- Support full-duplex streaming with bounded memory, flow control,
  backpressure, deadlines and cancellation. NAR uploads/downloads and build
  logs can be large; a scheduler control stream must not be blocked by them.
  Avoid making the active scheduler's memory or CPU the bottleneck for all
  build/store traffic, even if it owns the session registry.
- Define how a balancer/gateway instance locates tunnels held by another
  instance, and what happens across gateway or scheduler failover. Health
  checks and metrics should distinguish a registered worker from a reachable
  data tunnel; stop assigning work when its data path is unavailable.
- Preserve local workers' direct Envoy/EDS path where useful. Tunnel-capable
  and legacy address-advertising workers may coexist; do not mistake an
  opaque worker ID for an IP endpoint.

## Acceptance test for our two-cluster case

Start the farm on build1 with a local ARM worker and add five build2 x86
workers **using only outbound worker-to-farm connectivity**. Without adding
per-worker addresses, ports, return forwarders, or ingress Services, verify
that all five register and are individually selected for concurrent uncached
builds. Check that a build reaches the selected worker, its result and store
data are accessible through the client endpoint and published to niks3, and
unrelated store RPCs can use the remote pool. Replace one worker pod, interrupt
one tunnel during an in-flight build, drain another, and restart the scheduler
or gateway. In each case, the client must complete or receive a recoverable
failure; no build or store request may silently reach a different worker.

The existing per-worker Tailscale/forwarder design remains a deployment
workaround until an upstream change actually supplies this data path. This
proposal does not assume upstream has agreed to implement it.
