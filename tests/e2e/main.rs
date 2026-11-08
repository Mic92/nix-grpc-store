//! Run by tests/nixos-test.nix and tests/farm.nix on the driver host, one module per call.
mod access;
mod acme;
mod bench;
mod farm;
mod fixtures;
mod floating;
mod harness;
mod latency;
mod lifecycle;
mod transfer;
mod upload;
