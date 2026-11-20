{
  config,
  options,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.nix-grpc-daemon;
  builder = lib.elem "builder" cfg.roles;
  unit = import ./daemon-unit.nix {
    inherit cfg lib pkgs;
    nixPackage = config.nix.package;
  };

  # Only what the host set: defaults then come from the flakelet's revision,
  # which may be newer than this module.
  explicit = lib.concatMapAttrs (
    n: o:
    if lib.isOption o then
      lib.optionalAttrs (o.highestPrio < (lib.mkOptionDefault null).priority) { ${n} = o.value; }
    else
      let
        sub = explicit o;
      in
      lib.optionalAttrs (sub != { }) { ${n} = sub; }
  );
  flakeletSettings =
    removeAttrs (explicit options.services.nix-grpc-daemon) [
      "enable"
      "package"
      "flakelet"
      # applied by this module, on the host
      "buildLogRetention"
    ]
    // lib.optionalAttrs (cfg.niks3 != null) {
      niks3 = removeAttrs cfg.niks3 [ "package" ];
    };
in
{
  imports = [ ./common.nix ];

  options.services.nix-grpc-daemon.flakelet.enable = lib.mkOption {
    type = lib.types.bool;
    default = false;
    description = ''
      Run the daemon as a [flakelet](https://github.com/Mic92/flakelet)
      instead of a unit of this system: it then updates from the
      nix-grpc-store flake without a rebuild of the host, and
      {option}`package` and {option}`niks3.package` are ignored. This module
      still configures the user and Nix, and passes the other options on as
      the flakelet's settings. Needs the flakelet NixOS module.
    '';
  };

  config = lib.mkIf cfg.enable (
    lib.mkMerge [
      (lib.optionalAttrs (options ? services.flakelets) {
        services.flakelets = lib.mkIf cfg.flakelet.enable {
          enable = true;
          services.nix-grpc-daemon = {
            flake = lib.mkDefault "github:Mic92/nix-grpc-store";
            output = "flakelets.daemon";
            settings.daemon = flakeletSettings;
          };
        };
      })
      (
        # Older nixpkgs lack nix.buildLogRetention.
        if options ? nix.buildLogRetention then
          {
            nix.buildLogRetention = lib.mkIf (builder && cfg.buildLogRetention != null) (
              lib.mkDefault cfg.buildLogRetention
            );
          }
        else
          {
            systemd.tmpfiles.settings.nix-build-logs = lib.mkIf (builder && cfg.buildLogRetention != null) {
              "/nix/var/log/nix/drvs".e.age = "mM:${cfg.buildLogRetention}";
            };
          }
      )
      {
        assertions = unit.assertions ++ [
          {
            assertion = !cfg.flakelet.enable || options ? services.flakelets;
            message = "services.nix-grpc-daemon.flakelet.enable needs the flakelet NixOS module";
          }
        ];

        # gRPC clients inherit the store privileges of this uid via the proxied
        # nix-daemon connection, so default to a dedicated unprivileged user.
        users.users.nix-grpc-daemon = {
          isSystemUser = true;
          group = "nix-grpc-daemon";
        };
        users.groups.nix-grpc-daemon = { };

        systemd.sockets.nix-grpc-daemon = lib.mkIf (
          unit.socketActivated && !cfg.flakelet.enable
        ) unit.socket;

        systemd.services.nix-grpc-daemon = lib.mkIf (!cfg.flakelet.enable) (
          unit.service
          // {
            # Reload = drain: leave the balancer, finish and publish running
            # builds, then exit. The socket or Restart=always starts the new
            # generation.
            reloadIfChanged = builder;
            serviceConfig =
              unit.service.serviceConfig // lib.mapAttrs (_: lib.mkDefault) unit.serviceConfigDefaults;
          }
        );
      }
    ]
  );
}
