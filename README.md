# nix-grpc-store

**Status: Alpha.** Interfaces may change; plugin ABI is tied to a specific Nix
version.

Remote Nix store access over gRPC instead of SSH. Use it when you want to
reach a builder or store host through infrastructure that speaks HTTP/2
(load balancers, service meshes, mTLS) rather than opening SSH.

Everything that works over `ssh-ng://` works here: remote builds,
`nix copy`, path queries, GC. The gRPC layer is a thin tunnel to the
`nix-daemon` on the other side. The `nix copy` hot path (path info queries,
bulk import, NAR download) uses dedicated RPCs instead of the tunnel: a whole
batch of paths shares one zstd stream, so copying many small paths needs one
round trip instead of one per path.

## Install

    nix build

This produces `result/bin/nix-grpc-daemon` for the server and
`result/lib/nix/plugins/nix-grpc-store.so` for the client.

## Quick start (NixOS)

Add the flake and enable the modules:

    # flake.nix inputs
    nix-grpc-store.url = "github:Mic92/nix-grpc-store";

    # server
    imports = [ nix-grpc-store.nixosModules.server ];
    services.nix-grpc-daemon.enable = true;
    services.nix-grpc-daemon.tls = { certFile = ./server.pem; keyFile = ./server.key; };

    # client
    imports = [ nix-grpc-store.nixosModules.client ];
    programs.nix-grpc-store.enable = true;

The client module builds the plugin against `config.nix.package.libs`, so it
tracks whatever Nix your system uses. The server module runs the daemon as an
unprivileged `nix-grpc-daemon` user and adds it to `extra-allowed-users` so it
can reach the local `nix-daemon` even when `allowed-users` is restricted.

## Trust model

gRPC clients act as the `nix-grpc-daemon` user, which is not trusted by
default. That is enough for `nix build --store 'grpc://…'`, `nix copy`,
path queries and GC: sources and derivations are content-addressed, signed
cache paths are accepted, and the server builds everything itself.

Using the server as a remote builder (`builders = grpc://…`) also imports
unsigned outputs built on the client, which only trusted users may do. Set
`services.nix-grpc-daemon.trustClients = true` for that; every authenticated
client then has trusted-user privileges, so require client certs
(`tls.clientCaFile`).

## Remote builder

    nix.buildMachines = [{
      hostName = "grpc://builder:50051";
      protocol = null;
      systems = [ "x86_64-linux" "aarch64-linux" ];
      maxJobs = 64;
    }];

Only the nix-daemon needs the plugin loaded. TLS uses the system CA bundle
and the client key pair from `/run/nix-grpc-store` by default (see below).

## Quick start (manual)

On the builder:

    nix-grpc-daemon --listen 0.0.0.0:50051

On the client, add to `nix.conf`:

    plugin-files = /path/to/nix-grpc-store.so

and use it like any other store URI:

    nix store info --store 'grpc://builder:50051?insecure=1'
    nix build nixpkgs#hello --store 'grpc://builder:50051?insecure=1'
    nix copy --to 'grpc://builder:50051?insecure=1' ./result

The daemon proxies to the local `nix-daemon` socket, so gRPC clients get
whatever store privileges the uid running `nix-grpc-daemon` has.

## TLS and mTLS

Server:

    nix-grpc-daemon --listen 0.0.0.0:50051 \
        --tls-cert server.pem --tls-key server.key \
        --client-ca ca.pem              # optional: require client certs

Client:

    nix build --store \
      'grpc://builder:50051?ca-cert=ca.pem&client-cert=me.pem&client-key=me.key' ...

Without `--client-ca` any TLS client can connect; with it, only clients
presenting a certificate signed by that CA are accepted.

## URI parameters

  * `insecure` — plaintext, no TLS (testing only)
  * `ca-cert` — PEM CA to verify the server; defaults to `$NIX_SSL_CERT_FILE`,
    `$SSL_CERT_FILE` or the system CA bundle
  * `client-cert`, `client-key` — PEM pair to present for mTLS; default to
    `$NIX_GRPC_CLIENT_CERT`/`$NIX_GRPC_CLIENT_KEY`, then `client.crt`/`client.key`
    in `$XDG_DATA_HOME/nix-grpc-store`, then `/run/nix-grpc-store`
    (unreadable candidates are skipped)

## Server flags

  * `--listen ADDR` — default `0.0.0.0:50051`
  * `--proxy-socket PATH` — nix-daemon socket, default `/nix/var/nix/daemon-socket/socket`
  * `--tls-cert`, `--tls-key`, `--client-ca` — see above

## Tests

    nix build .#checks.x86_64-linux.vm -L

End-to-end NixOS VM test plus a 256 MiB throughput benchmark against the unix
socket and `perf` counters. See `src/pump.hh` for design notes on chunk
coalescing and flushable zstd.
