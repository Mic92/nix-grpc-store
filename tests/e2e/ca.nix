# Derivations for farm::ca and floating.
{ tag }:
{
  impure = derivation {
    name = "ca-impure-${tag}";
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" "echo impure-${tag} > $out" ];
    __impure = true;
  };
}
