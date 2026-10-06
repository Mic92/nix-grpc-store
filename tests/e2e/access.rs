use crate::fixtures::*;
use crate::harness::{MACHINE, metrics};

const MTLS: &str = "nix-grpc-daemon-mtls.service";

fn wait_for_mtls() {
    wait_for_unit(MTLS);
    wait_for_open_port(50052);
}

#[test]
fn gc_is_refused() {
    let p = succeed(&["nix", "store", "add", "--store", STORE, "/etc/hello.nix"]);
    let p = p.trim();
    for cmd in [vec!["store", "gc"], vec!["store", "delete", p]] {
        let mut argv = vec!["nix"];
        argv.extend(cmd);
        argv.extend(["--store", STORE]);
        let err = fail(&argv);
        assert!(err.contains("workers collect their own stores"), "{err}");
    }
    succeed(&["nix", "path-info", "--store", STORE, p]);
}

#[test]
fn mtls() {
    wait_for_mtls();
    let p = hello_path();
    let dir = workdir("mtls");
    let anon = anon_store();
    succeed(&["nix", "path-info", "--store", &anon, p]);
    let deny = deny_file(&dir);
    fail(&["nix", "store", "add", "--store", &anon, &deny]);
    assert_journal(MTLS, "event=denied method=Connect cn=- role=read-only");
    let store = cert_store("client");
    succeed(&["nix", "store", "info", "--json", "--store", &store]);
    succeed(&[
        "nix",
        "build",
        "--store",
        &store,
        "--impure",
        "-f",
        "/etc/hello.nix",
        "--no-link",
        "--print-out-paths",
    ]);
}

#[test]
fn missing_client_cert_yields_a_readable_error() {
    wait_for_unit("nix-grpc-daemon-strict.service");
    wait_for_open_port(50053);
    let p = hello_path();
    let d = cert_dir();
    let strict = format!("grpc://localhost:50053?ca-cert={d}/ca.pem");

    let err = fail(&["nix", "path-info", "--store", &strict, p]);
    assert!(err.contains("requires a TLS client certificate"), "{err}");
    assert!(err.contains("no client certificate was presented"), "{err}");

    let err = fail(&["nix", "path-info", "--store", "grpc://localhost:50053", p]);
    assert!(err.contains("server certificate is not trusted"), "{err}");
    assert!(
        !err.contains("no client certificate was presented"),
        "{err}"
    );

    let expired = format!("{strict}&client-cert={d}/expired.pem&client-key={d}/expired.key");
    let err = fail(&["nix", "path-info", "--store", &expired, p]);
    assert!(err.contains("has expired"), "{err}");

    let valid = format!("{strict}&client-cert={d}/client.pem&client-key={d}/client.key");
    succeed(&["nix", "path-info", "--store", &valid, p]);
}

#[test]
fn acl_read_only_role() {
    wait_for_mtls();
    let p = hello_path();
    let dir = workdir("acl-ro");
    let ro = cert_store("ro");
    succeed(&["nix", "path-info", "--store", &ro, p]);
    succeed(&[
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        &ro,
        "--to",
        &format!("{dir}/dst"),
        p,
    ]);
    let deny = deny_file(&dir);
    fail(&["nix", "store", "add", "--store", &ro, &deny]);
    fail(&[
        "nix",
        "build",
        "--store",
        &ro,
        "--impure",
        "-f",
        "/etc/hello.nix",
        "--no-link",
    ]);
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
    let src = format!("{dir}/src");
    let up = succeed(&[
        "nix",
        "build",
        "--store",
        &src,
        "--impure",
        "-f",
        &format!("{dir}/acl.nix"),
        "--no-link",
        "--print-out-paths",
    ]);
    let up = up.trim();
    let copy = [
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        &src,
        "--to",
        &rw,
        up,
    ];
    fail(&copy);
    let key_file = format!("{dir}/cache-key");
    succeed(&[
        "sh",
        "-c",
        r#"install -m 0600 /dev/null "$1" && printf '%s\n' "$2" > "$1""#,
        "sh",
        &key_file,
        &env("NGS_SIGNING_KEY"),
    ]);
    succeed(&["nix", "store", "sign", "-k", &key_file, "--store", &src, up]);
    succeed(&copy);
    succeed(&["test", "-e", up]);
    let deny = deny_file(&dir);
    fail(&["nix", "store", "add", "--store", &rw, &deny]);
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
    let bp = format!("{dir}/bp.nix");
    let out = succeed(&[
        "nix",
        "build",
        "--store",
        &rw,
        "--eval-store",
        "auto",
        "--impure",
        "-f",
        &bp,
        "--no-link",
        "--print-out-paths",
    ]);
    let built = succeed(&["cat", out.trim()]);
    assert!(built.contains("bp-payload"), "{built}");
    assert_journal(MTLS, "event=rpc method=BuildDerivation cn=rw-client");
    write_file(
        &format!("{dir}/bp-fail.nix"),
        &drv_expr("bp-fail", "exit 1"),
    );
    fail(&[
        "nix",
        "build",
        "--store",
        &rw,
        "--eval-store",
        "auto",
        "--impure",
        "-f",
        &format!("{dir}/bp-fail.nix"),
        "--no-link",
    ]);
    fail(&[
        "nix",
        "build",
        "--store",
        &rw,
        "--eval-store",
        "auto",
        "--impure",
        "-f",
        &bp,
        "--no-link",
        "--repair",
    ]);
    assert_journal(
        MTLS,
        "event=denied method=BuildDerivation(repair) cn=rw-client",
    );
}

#[test]
fn acl_unmatched_cn_is_denied() {
    wait_for_mtls();
    fail(&["nix", "store", "info", "--store", &cert_store("stranger")]);
    assert_journal(MTLS, "event=denied method=.* cn=stranger role=none");
}

/// Asks the mock issuer for a token with the given claims and returns a store
/// URI that presents it.
fn token_store(dir: &str, name: &str, claims: &[&str]) -> String {
    let aud = format!("aud={}", env("NGS_OIDC_AUDIENCE"));
    let mut cmd = vec![
        "sh",
        "-c",
        r#"out=$1; shift; curl -sfG "$@" > "$out" && test -s "$out""#,
        "sh",
    ];
    let out = format!("{dir}/{name}.jwt");
    cmd.extend([
        out.as_str(),
        "http://127.0.0.1:8081/issue",
        "--data-urlencode",
        &aud,
    ]);
    for c in claims {
        cmd.extend(["--data-urlencode", c]);
    }
    succeed(&cmd);
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

    let writer = token_store(&dir, "writer", &["sub=repo:myorg/x:ref:refs/heads/dev"]);
    let admin = token_store(&dir, "admin", &["sub=someone", "repository_owner=admins"]);
    let reader = token_store(
        &dir,
        "reader",
        &["sub=someone", r#"claims={"groups":["readers"]}"#],
    );
    let nobody = token_store(&dir, "nobody", &["sub=repo:other/x:y"]);
    succeed(&[
        "sh",
        "-c",
        r#"curl -sfG "$1" --data-urlencode aud=elsewhere --data-urlencode "$2" > "$3""#,
        "sh",
        "http://127.0.0.1:8081/issue",
        "sub=repo:myorg/x:y",
        &format!("{dir}/wrongaud.jwt"),
    ]);
    let wrongaud =
        format!("grpc://localhost:50052?ca-cert={d}/ca.pem&token-file={dir}/wrongaud.jwt");

    succeed(&["nix", "path-info", "--store", &reader, p]);
    fail(&[
        "nix",
        "build",
        "--store",
        &reader,
        "--impure",
        "-f",
        "/etc/hello.nix",
        "--no-link",
    ]);
    write_file(
        &format!("{dir}/oidc.nix"),
        &drv_expr("oidc-blob", "echo oidc > $out"),
    );
    succeed(&[
        "nix",
        "build",
        "--store",
        &writer,
        "--eval-store",
        "auto",
        "--impure",
        "-f",
        &format!("{dir}/oidc.nix"),
        "--no-link",
    ]);
    let deny = deny_file(&dir);
    fail(&["nix", "store", "add", "--store", &writer, &deny]);
    succeed(&["nix", "store", "add", "--store", &admin, "/etc/hello.nix"]);
    let err = fail(&["nix", "path-info", "--store", &nobody, p]);
    assert!(
        err.contains("no access rule matches 'oidc:mock:repo:other/x:y'"),
        "{err}"
    );
    let err = fail(&["nix", "path-info", "--store", &wrongaud, p]);
    assert!(err.contains("bearer token rejected"), "{err}");
    assert_journal(MTLS, "event=oidc_rejected.*audience");
    write_file(&format!("{dir}/garbage.jwt"), "garbage");
    let garbage = format!("grpc://localhost:50052?ca-cert={d}/ca.pem&token-file={dir}/garbage.jwt");
    fail(&["nix", "path-info", "--store", &garbage, p]);
    let with_cert = format!("{}&token-file={dir}/reader.jwt", cert_store("client"));
    succeed(&[
        "nix",
        "store",
        "add",
        "--store",
        &with_cert,
        "/etc/hello.nix",
    ]);
    assert_journal(MTLS, "method=BuildDerivation cn=oidc:mock:repo:myorg/x");
}

#[test]
fn access_log_attributes_clients_by_certificate_cn() {
    wait_for_mtls();
    let p = hello_path();
    succeed(&[
        "nix",
        "store",
        "info",
        "--json",
        "--store",
        &cert_store("client"),
    ]);
    succeed(&["nix", "store", "info", "--store", STORE]);
    succeed(&["nix", "path-info", "--store", STORE, p]);
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
    succeed(&[
        "nix",
        "store",
        "info",
        "--json",
        "--store",
        &cert_store("client"),
    ]);
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
        &["sub=repo:myorg/evalstore:ref:refs/heads/dev"],
    );
    write_file(
        &format!("{dir}/ifd.nix"),
        r#"let
  gen = derivation { name = "ifd-gen"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "echo ifd-payload > $out" ]; };
in { value = builtins.readFile gen; drv = gen; }"#,
    );
    let out = succeed(&[
        "env",
        &format!("NIX_REMOTE={writer}"),
        "nix",
        "eval",
        "--impure",
        "--raw",
        "--eval-store",
        "daemon",
        "-f",
        &format!("{dir}/ifd.nix"),
        "value",
    ]);
    assert!(out.contains("ifd-payload"), "{out}");
    assert_no_journal(
        MTLS,
        "event=denied method=Connect cn=oidc:mock:repo:myorg/evalstore",
    );
}
