# Command line of nix-grpc-daemon for the given configuration.
{
  cfg,
  lib,
  pkgs,
}:
[
  (lib.getExe cfg.package)
  "--proxy-socket"
  cfg.proxySocket
]
++ lib.optionals (cfg.idleTimeout != null) [
  "--idle-timeout"
  (toString cfg.idleTimeout)
]
++ lib.optionals (cfg.idleTimeout == null) [
  "--listen"
  cfg.listen
]
++ lib.optionals (cfg.tls.certFile != null) [
  "--tls-cert"
  cfg.tls.certFile
  "--tls-key"
  cfg.tls.keyFile
]
++ lib.optionals (cfg.tls.clientCaFile != null) [
  "--client-ca"
  cfg.tls.clientCaFile
]
++ lib.concatMap (rule: [
  "--allow"
  "${rule.cn}=${rule.role}"
]) cfg.accessRules
++ lib.optionals (cfg.anonymousRole != null) [
  "--allow-anonymous"
  cfg.anonymousRole
]
++ lib.concatMap (cn: [
  "--trusted-proxy"
  cn
]) cfg.trustedProxies
++ lib.optionals (cfg.oidc != null) [
  "--oidc-config"
  ((pkgs.formats.json { }).generate "nix-grpc-daemon-oidc.json" cfg.oidc)
]
++ lib.optionals (cfg.workerName != null) [
  "--worker-name"
  cfg.workerName
]
++ lib.optionals (cfg.metricsListen != null) [
  "--metrics-listen"
  cfg.metricsListen
]
++ [
  "--log-level"
  cfg.logLevel
]
++ [
  "--role"
  (lib.concatStringsSep "," cfg.roles)
  "--min-free"
  cfg.minFree
]
++ lib.optionals (cfg.maxJobs != null) [
  "--max-jobs"
  (toString cfg.maxJobs)
]
++ lib.optionals (cfg.scheduler != null) [
  "--scheduler"
  cfg.scheduler
]
++ lib.optionals (cfg.schedulerTokenFile != null) [
  "--scheduler-token-file"
  cfg.schedulerTokenFile
]
++ lib.optionals (cfg.advertise != null) [
  "--advertise"
  cfg.advertise
]
++ lib.optionals (cfg.niks3 != null) (
  [
    "--niks3"
    cfg.niks3.url
    "--niks3-push"
    (lib.escapeShellArgs ([ (lib.getExe cfg.niks3.package) "push" ] ++ cfg.niks3.pushFlags))
  ]
  ++ lib.optionals (cfg.niks3.tokenFile != null) [
    "--niks3-token-file"
    cfg.niks3.tokenFile
  ]
  ++ lib.optionals (cfg.niks3.clientCertFile != null) [
    "--niks3-client-cert"
    cfg.niks3.clientCertFile
    "--niks3-client-key"
    cfg.niks3.clientKeyFile
  ]
)
++ cfg.extraFlags
