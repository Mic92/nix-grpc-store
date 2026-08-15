# Real-world benchmark closure, reproducible through the flake's pinned
# nixpkgs. Typical deployment mix: many small paths (locales, configs,
# libraries) and a few large ones, all substitutable from cache.nixos.org.
{ pkgs }:
pkgs.buildEnv {
  name = "bench-closure";
  paths = [
    pkgs.git
    pkgs.python3
    pkgs.curl
    pkgs.jq
    pkgs.htop
  ];
}
