{
  stdenv,
  fetchFromGitHub,
  cmake,
  openssl,
  nlohmann_json,
}:
# Header-only JWT/JWKS verification (not in nixpkgs).
stdenv.mkDerivation (finalAttrs: {
  pname = "jwt-cpp";
  version = "0.7.2";
  src = fetchFromGitHub {
    owner = "Thalhammer";
    repo = "jwt-cpp";
    rev = "v${finalAttrs.version}";
    hash = "sha256-jzxK21mc7M6+ozxLo5KCqb7A/6gLWNaim5/UC7kZqHE=";
  };
  nativeBuildInputs = [ cmake ];
  propagatedBuildInputs = [
    openssl
    nlohmann_json
  ];
  cmakeFlags = [
    "-DJWT_BUILD_EXAMPLES=OFF"
    "-DJWT_DISABLE_PICOJSON=ON"
    "-DJWT_CMAKE_FILES_INSTALL_DIR=lib/cmake/jwt-cpp"
  ];
})
