# flakelet module (https://github.com/Mic92/flakelet) for nix-grpc-daemon:
# the units update from this flake independently of the host system. The
# host keeps the nix-grpc-daemon user and nix.settings, which
# services.nix-grpc-daemon.flakelet.enable of the NixOS module sets up.
{ niks3 }:
{ types, ... }:
{
  options = {
    daemon = {
      type = types.attrsOf types.any;
      default = { };
      description = ''
        `services.nix-grpc-daemon` options of the NixOS module. `package`
        and `niks3.package` come from this flake.
      '';
    };
    serviceConfig = {
      type = types.attrsOf types.any;
      default = { };
      example = {
        MemoryMax = "16G";
      };
      description = "Extra `[Service]` settings of the daemon unit.";
    };
  };

  impl =
    { options, inputs }:
    let
      inherit (inputs.nixpkgs) pkgs lib;
      inherit (inputs.flakelet) name;
      settings =
        options.daemon
        // lib.optionalAttrs ((options.daemon.niks3 or null) != null) {
          niks3 = {
            package = niks3.packages.${pkgs.stdenv.hostPlatform.system}.niks3;
          }
          // options.daemon.niks3;
        };
      # The NixOS option declarations type-check the settings and fill in
      # the defaults.
      eval = lib.evalModules {
        modules = [
          ../nixos/common.nix
          {
            config._module.args = { inherit pkgs; };
            # common.nix defines these for the host
            options.assertions = lib.mkOption { type = lib.types.listOf lib.types.raw; };
            options.nix = lib.mkOption { type = lib.types.raw; };
            config.services.nix-grpc-daemon = settings // {
              enable = true;
            };
          }
        ];
      };
      cfg = eval.config.services.nix-grpc-daemon;
      unit = import ../nixos/daemon-unit.nix {
        inherit
          cfg
          lib
          pkgs
          name
          ;
        nixPackage = pkgs.nix;
      };
      failed = lib.filter (a: !a.assertion) (eval.config.assertions ++ unit.assertions);
    in
    lib.throwIf (failed != [ ]) (lib.concatMapStringsSep "\n" (a: a.message) failed) (
      {
        services.${name} = unit.service // {
          serviceConfig = unit.serviceConfigDefaults // unit.service.serviceConfig // options.serviceConfig;
        };
      }
      // lib.optionalAttrs unit.socketActivated {
        sockets.${name} = unit.socket;
      }
    );
}
