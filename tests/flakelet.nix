# services.nix-grpc-daemon.flakelet.enable hands the host's options to the
# flakelet, which renders the unit the NixOS module would. Evaluation only:
# flakelet itself is stubbed, so this flake needs no input for it.
{
  pkgs,
  module,
  flakeletDaemon,
}:
let
  inherit (pkgs) lib;
  name = "nix-grpc-daemon";
  host = pkgs.nixos {
    imports = [ module ];
    options.services.flakelets = lib.mkOption { type = (pkgs.formats.json { }).type; };
    config = {
      system.stateVersion = lib.trivial.release;
      boot.loader.grub.enable = false;
      fileSystems."/" = {
        device = "none";
        fsType = "tmpfs";
      };
      services.nix-grpc-daemon = {
        enable = true;
        flakelet.enable = true;
        roles = [ "builder" ];
        scheduler = "lb.example.org:443";
        listen = "[::]:50051";
        maxJobs = 16;
        tls = {
          certFile = "/etc/farm/worker.crt";
          keyFile = "/run/secrets/worker.key";
        };
        niks3 = {
          package = throw "the flakelet brings its own niks3";
          url = "https://niks3.example.org";
          cacheUrl = "https://cache.example.org";
          publicKeys = [ "cache.example.org-1:AAAA" ];
        };
      };
      services.flakelets.services.${name}.settings.serviceConfig.MemoryMax = "16G";
    };
  };
  entry = host.config.services.flakelets.services.${name};
  # what flakelet does with the entry on the machine
  rendered =
    (flakeletDaemon {
      types = {
        attrsOf = _: null;
        any = null;
      };
    }).impl
      {
        options = entry.settings;
        inputs = {
          nixpkgs = { inherit pkgs lib; };
          flakelet = { inherit name; };
        };
      };
  inherit (rendered.services.${name}) serviceConfig;
  has = lib.flip lib.hasInfix serviceConfig.ExecStart;
in
assert entry.output == "flakelets.daemon";
assert !(host.config.systemd.services ? ${name});
assert host.config.users.users ? ${name};
# defaults are left to the flakelet's revision
assert !(entry.settings.daemon ? minFree);
assert has "--scheduler lb.example.org:443";
assert has "--max-jobs 16";
assert has "--tls-key /run/secrets/worker.key";
assert has "--niks3 https://niks3.example.org";
assert has "bin/niks3 push";
# an overridden default, and one the flakelet fills in
assert serviceConfig.MemoryMax == "16G";
assert serviceConfig.TimeoutStopSec == "1h";
# a builder with a scheduler is not socket activated
assert !(rendered ? sockets);
pkgs.emptyFile
