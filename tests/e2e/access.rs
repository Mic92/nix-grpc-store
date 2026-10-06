use crate::fixtures::*;
use crate::harness::{MACHINE, metrics};

const MTLS: &str = "nix-grpc-daemon-mtls.service";

fn wait_for_mtls() {
    wait_for_unit(MTLS);
    wait_for_open_port(50052);
}

#[test]
fn gc_is_refused() {
    let p = succeed(&format!("nix store add --store '{STORE}' /etc/hello.nix"));
    let p = p.trim();
    for cmd in ["store gc".to_string(), format!("store delete '{p}'")] {
        let err = fail(&format!("nix {cmd} --store '{STORE}'"));
        assert!(err.contains("workers collect their own stores"), "{err}");
    }
    succeed(&format!("nix path-info --store '{STORE}' '{p}'"));
}

#[test]
fn mtls() {
    wait_for_mtls();
    let p = hello_path();
    let dir = workdir("mtls");
    let anon = anon_store();
    succeed(&format!("nix path-info --store '{anon}' '{p}'"));
    let deny = deny_file(&dir);
    fail(&format!("nix store add --store '{anon}' {deny}"));
    assert_journal(MTLS, "event=denied method=Connect cn=- role=read-only");
    let store = cert_store("client");
    succeed(&format!("nix store info --json --store '{store}'"));
    succeed(&format!(
        "nix build --store '{store}' --impure -f /etc/hello.nix --no-link --print-out-paths"
    ));
}

#[test]
fn missing_client_cert_yields_a_readable_error() {
    wait_for_unit("nix-grpc-daemon-strict.service");
    wait_for_open_port(50053);
    let p = hello_path();
    let d = cert_dir();
    let strict = format!("grpc://localhost:50053?ca-cert={d}/ca.pem");

    let err = fail(&format!("nix path-info --store '{strict}' '{p}'"));
    assert!(err.contains("requires a TLS client certificate"), "{err}");
    assert!(err.contains("no client certificate was presented"), "{err}");

    let err = fail(&format!(
        "nix path-info --store 'grpc://localhost:50053' '{p}'"
    ));
    assert!(err.contains("server certificate is not trusted"), "{err}");
    assert!(
        !err.contains("no client certificate was presented"),
        "{err}"
    );

    let err = fail(&format!(
        "nix path-info --store '{strict}&client-cert={d}/expired.pem&client-key={d}/expired.key' '{p}'"
    ));
    assert!(err.contains("has expired"), "{err}");

    succeed(&format!(
        "nix path-info --store '{strict}&client-cert={d}/client.pem&client-key={d}/client.key' '{p}'"
    ));
}

#[test]
fn acl_read_only_role() {
    wait_for_mtls();
    let p = hello_path();
    let dir = workdir("acl-ro");
    let ro = cert_store("ro");
    succeed(&format!("nix path-info --store '{ro}' '{p}'"));
    succeed(&format!(
        "nix copy --no-check-sigs --from '{ro}' --to {dir}/dst '{p}'"
    ));
    let deny = deny_file(&dir);
    fail(&format!("nix store add --store '{ro}' {deny}"));
    fail(&format!(
        "nix build --store '{ro}' --impure -f /etc/hello.nix --no-link"
    ));
    assert_journal(
        MTLS,
        "event=denied method=Connect cn=ro-client role=read-only",
    );
}

#[test]
fn acl_write_role_enforces_signatures() {
    wait_for_mtls();
    let dir = workdir("acl-sig");
    let rw = cert_store("rw");
    // Input-addressed output: a CA path (nix store add) would pass CheckSigs
    write_file(
        &format!("{dir}/acl.nix"),
        &drv_expr("acl-blob", "echo acl-payload > $out"),
    );
    let up = succeed(&format!(
        "nix build --store {dir}/src --impure -f {dir}/acl.nix --no-link --print-out-paths"
    ));
    let up = up.trim();
    let copy = format!("nix copy --no-check-sigs --from {dir}/src --to '{rw}' '{up}'");
    fail(&copy);
    let key = env("NGS_SIGNING_KEY");
    succeed(&format!(
        "install -m 0600 /dev/null {dir}/cache-key && echo '{key}' > {dir}/cache-key"
    ));
    succeed(&format!(
        "nix store sign -k {dir}/cache-key --store {dir}/src '{up}'"
    ));
    succeed(&copy);
    succeed(&format!("test -e '{up}'"));
    let deny = deny_file(&dir);
    fail(&format!("nix store add --store '{rw}' {deny}"));
}

#[test]
fn acl_write_role_builds_via_schedule_and_build_derivation() {
    wait_for_mtls();
    let dir = workdir("acl-build");
    let rw = cert_store("rw");
    write_file(
        &format!("{dir}/bp.nix"),
        &drv_expr("bp-blob", "echo bp-payload > $out"),
    );
    let out = succeed(&format!(
        "nix build --store '{rw}' --eval-store auto --impure -f {dir}/bp.nix --no-link --print-out-paths"
    ));
    let built = succeed(&format!("cat '{}'", out.trim()));
    assert!(built.contains("bp-payload"), "{built}");
    assert_journal(MTLS, "event=rpc method=BuildDerivation cn=rw-client");
    write_file(
        &format!("{dir}/bp-fail.nix"),
        &drv_expr("bp-fail", "exit 1"),
    );
    fail(&format!(
        "nix build --store '{rw}' --eval-store auto --impure -f {dir}/bp-fail.nix --no-link"
    ));
    fail(&format!(
        "nix build --store '{rw}' --eval-store auto --impure -f {dir}/bp.nix --no-link --repair"
    ));
    assert_journal(
        MTLS,
        "event=denied method=BuildDerivation(repair) cn=rw-client",
    );
}

#[test]
fn acl_unmatched_cn_is_denied() {
    wait_for_mtls();
    fail(&format!(
        "nix store info --store '{}'",
        cert_store("stranger")
    ));
    assert_journal(MTLS, "event=denied method=.* cn=stranger role=none");
}

fn token_store(dir: &str, name: &str, query: &str) -> String {
    let aud = env("NGS_OIDC_AUDIENCE");
    succeed(&format!(
        "curl -sfG 'http://127.0.0.1:8081/issue' --data-urlencode 'aud={aud}' {query} > {dir}/{name}.jwt \
             && test -s {dir}/{name}.jwt"
    ));
    format!(
        "grpc://localhost:50052?ca-cert={}/ca.pem&token-file={dir}/{name}.jwt",
        cert_dir()
    )
}

#[test]
fn oidc_bearer_tokens() {
    wait_for_mtls();
    wait_for_unit("mock-oidc.service");
    wait_for_open_port(8081);
    let p = hello_path();
    let dir = workdir("oidc");
    let d = cert_dir();

    let writer = token_store(
        &dir,
        "writer",
        "--data-urlencode 'sub=repo:myorg/x:ref:refs/heads/dev'",
    );
    let admin = token_store(
        &dir,
        "admin",
        "--data-urlencode sub=someone --data-urlencode repository_owner=admins",
    );
    let reader = token_store(
        &dir,
        "reader",
        r#"--data-urlencode sub=someone --data-urlencode 'claims={"groups":["readers"]}'"#,
    );
    let nobody = token_store(&dir, "nobody", "--data-urlencode 'sub=repo:other/x:y'");
    succeed(&format!(
        "curl -sfG 'http://127.0.0.1:8081/issue' --data-urlencode aud=elsewhere \
             --data-urlencode 'sub=repo:myorg/x:y' > {dir}/wrongaud.jwt"
    ));
    let wrongaud =
        format!("grpc://localhost:50052?ca-cert={d}/ca.pem&token-file={dir}/wrongaud.jwt");

    succeed(&format!("nix path-info --store '{reader}' '{p}'"));
    fail(&format!(
        "nix build --store '{reader}' --impure -f /etc/hello.nix --no-link"
    ));
    write_file(
        &format!("{dir}/oidc.nix"),
        &drv_expr("oidc-blob", "echo oidc > $out"),
    );
    succeed(&format!(
        "nix build --store '{writer}' --eval-store auto --impure -f {dir}/oidc.nix --no-link"
    ));
    let deny = deny_file(&dir);
    fail(&format!("nix store add --store '{writer}' {deny}"));
    succeed(&format!("nix store add --store '{admin}' /etc/hello.nix"));
    let err = fail(&format!("nix path-info --store '{nobody}' '{p}'"));
    assert!(
        err.contains("no access rule matches 'oidc:mock:repo:other/x:y'"),
        "{err}"
    );
    let err = fail(&format!("nix path-info --store '{wrongaud}' '{p}'"));
    assert!(err.contains("bearer token rejected"), "{err}");
    assert_journal(MTLS, "event=oidc_rejected.*audience");
    succeed(&format!("echo garbage > {dir}/garbage.jwt"));
    fail(&format!(
        "nix path-info --store 'grpc://localhost:50052?ca-cert={d}/ca.pem&token-file={dir}/garbage.jwt' '{p}'"
    ));
    let with_cert = format!("{}&token-file={dir}/reader.jwt", cert_store("client"));
    succeed(&format!(
        "nix store add --store '{with_cert}' /etc/hello.nix"
    ));
    assert_journal(MTLS, "method=BuildDerivation cn=oidc:mock:repo:myorg/x");
}

#[test]
fn access_log_attributes_clients_by_certificate_cn() {
    wait_for_mtls();
    let p = hello_path();
    succeed(&format!(
        "nix store info --json --store '{}'",
        cert_store("client")
    ));
    succeed(&format!("nix store info --store '{STORE}'"));
    succeed(&format!("nix path-info --store '{STORE}' '{p}'"));
    let connect = "event=rpc method=Connect cn=localhost .*bytes_out=";
    wait_journal_above(MTLS, connect, 0, 20);
    assert_journal("nix-grpc-daemon.service", "event=rpc method=Connect cn=- ");
    assert_journal(
        "nix-grpc-daemon.service",
        "level=debug event=rpc_start method=QueryPathInfos",
    );
    assert_no_journal(MTLS, "level=debug");
}

#[test]
fn prometheus_metrics_are_labelled_by_certificate_cn() {
    wait_for_mtls();
    succeed(&format!(
        "nix store info --json --store '{}'",
        cert_store("client")
    ));
    let lines = metrics(MACHINE, 9464);
    assert!(
        lines.iter().any(|l| l.starts_with("nix_grpc_rpcs_total{")
            && l.contains(r#"cn="localhost""#)
            && l.contains(r#"method="Connect""#)),
        "{lines:#?}"
    );
}

#[test]
fn evaluation_with_ca_derivations_needs_no_tunnel() {
    wait_for_mtls();
    wait_for_unit("mock-oidc.service");
    wait_for_open_port(8081);
    let dir = workdir("eval-store");
    let writer = token_store(
        &dir,
        "writer",
        "--data-urlencode 'sub=repo:myorg/evalstore:ref:refs/heads/dev'",
    );
    write_file(
        &format!("{dir}/ifd.nix"),
        r#"let
  gen = derivation { name = "ifd-gen"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo ifd-payload > $out" ]; };
in { value = builtins.readFile gen; drv = gen; }"#,
    );
    let out = succeed(&format!(
        "NIX_REMOTE='{writer}' nix eval --impure --raw --eval-store daemon -f {dir}/ifd.nix value"
    ));
    assert!(out.contains("ifd-payload"), "{out}");
    assert_no_journal(
        MTLS,
        "event=denied method=Connect cn=oidc:mock:repo:myorg/evalstore",
    );
}
