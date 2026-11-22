# The NixOS side of tests/e2e: the agent the Rust tests on the driver host run
# their commands through.
{ e2eTests }:
{
  # The socket unit can only bind once the kernel knows vsock.
  boot.initrd.kernelModules = [ "vmw_vsock_virtio_transport" ];

  systemd.sockets.ngs-agent = {
    wantedBy = [ "sockets.target" ];
    socketConfig = {
      # tests/e2e/wire.rs knows the port.
      ListenStream = "vsock::4000";
      Accept = true;
      MaxConnections = 256;
    };
  };

  systemd.services."ngs-agent@" = {
    serviceConfig = {
      ExecStart = "${e2eTests}/bin/ngs-agent";
      StandardInput = "socket";
      StandardOutput = "socket";
      StandardError = "journal";
      WorkingDirectory = "/root";
      # One unit per command would otherwise flood the console.
      LogLevelMax = "notice";
    };
    environment = {
      HOME = "/root";
      USER = "root";
      LOGNAME = "root";
    };
  };
}
