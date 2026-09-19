# Merge per-arch image tarballs into one multi-arch OCI archive.
{
  lib,
  stdenvNoCC,
  linkFarm,
  regctl,
  name,
  imageName,
  perArch, # { x86_64-linux = <tarball>; aarch64-linux = <tarball>; }
}:
stdenvNoCC.mkDerivation {
  inherit name;
  passthru = { inherit perArch; };
  dontUnpack = true;
  src = linkFarm "images" (lib.mapAttrsToList (name: path: { inherit name path; }) perArch);
  nativeBuildInputs = [ regctl ];
  installPhase = ''
    refs=()
    for platform in $src/*; do
      ref="ocidir://images:$(basename "$platform")"
      refs+=(--ref "$ref")
      regctl image import "$ref" "$platform"
    done
    regctl index create ocidir://images:latest "''${refs[@]}"
    regctl image export ocidir://images:latest --name ${imageName} > $out
  '';
}
