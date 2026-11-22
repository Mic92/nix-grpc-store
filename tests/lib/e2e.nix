# Python for a NixOS testScript: runs the Rust tests on the driver host.
{ e2eTests }:
''
  import os, subprocess

  def run_e2e(nodes, extra_env, *args):
      env = dict(os.environ)
      for name, node in nodes.items():
          env[f"NGS_{name.upper()}_SOCK"] = str(node.vsock_host)
      env.update(extra_env)
      subprocess.run(["${e2eTests}/bin/nix-grpc-e2e", *args, "--nocapture"], env=env, check=True)
''
