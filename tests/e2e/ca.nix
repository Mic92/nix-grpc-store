# Derivations for farm::ca and floating.
{ tag }:
let
  ca = name: script: derivation {
    inherit name;
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" script ];
    __contentAddressed = true;
    outputHashMode = "recursive";
    outputHashAlgo = "sha256";
  };
in
rec {
  # dynamic reads the output of inner, the floating derivation that the
  # text-hashed producer outputs byte for byte.
  inner = ca "ca-inner-${tag}" "echo inner-${tag} > $out";
  producer = derivation {
    name = "ca-inner-${tag}.drv";
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" "read -r d < ${builtins.unsafeDiscardOutputDependency inner.drvPath}; printf %s \"$d\" > $out" ];
    __contentAddressed = true;
    outputHashMode = "text";
    outputHashAlgo = "sha256";
  };
  dynamic = derivation {
    name = "ca-dynamic-${tag}";
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" "read x < ${builtins.outputOf producer.outPath "out"}; echo dynamic-$x > $out" ];
  };
  # floating: slow reads dep, which the build's own connection roots.
  dep = derivation {
    name = "floating-dep-${tag}";
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" "echo dep-${tag} > $out" ];
  };
  # Inner sh, so pkill lets the build end at once.
  slow = ca "floating-slow-${tag}" "read x < ${dep}; /bin/sh -c 'read -t 90 ${tag} < /dev/zero'; echo slow-${tag} > $out";
  nondet = ca "floating-nondet-${tag}" "read u < /proc/sys/kernel/random/uuid; echo $u > $out";
  impure = derivation {
    name = "ca-impure-${tag}";
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" "echo impure-${tag} > $out" ];
    __impure = true;
  };
}
