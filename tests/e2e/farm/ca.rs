//! Content-addressed derivations through the build hook, which the client
//! builds into a store of its own with the experimental features they need:
//! in NIX_CONFIG, so that the hook gets them too.

use super::helpers::*;
use crate::harness::*;

const EXPR: &str = "/root/ca.nix";
const STORE: &str = "/root/ca-client";

/// `nix build` of ca.nix's `attr` for `tag` through the build hook. The
/// hook's machine supports `ca-derivations`, which nix requires of a
/// content-addressed derivation; an unplaceable build fails at once.
fn build_ca(attr: &str, tag: &str) -> Out {
    write_file(CLIENT, EXPR, include_str!("../ca.nix"));
    let sys = system();
    let builders = format!(
        "{}&system={sys}&restart-grace=5 {sys} - 4 1 ca-derivations",
        envoy()
    );
    run_t(
        CLIENT,
        &[
            "env",
            "NIX_CONFIG=extra-experimental-features = ca-derivations dynamic-derivations impure-derivations",
            "nix",
            "build",
            "-L",
            "--print-out-paths",
            "--no-link",
            "--store",
            STORE,
            "--max-jobs",
            "0",
            "--builders",
            &builders,
            "-f",
            EXPR,
            attr,
            "--argstr",
            "tag",
            tag,
        ],
        300,
    )
}

/// nix requires ca-derivations of an impure derivation too, which the
/// hook's machine supports. Builders would turn it down, so the hook
/// refuses it.
#[test]
fn the_hook_refuses_an_impure_derivation() {
    farm_ready();
    let o = build_ca("impure", "impure");
    let out = o.combined();
    assert_ne!(o.code, 0, "{out}");
    assert!(
        out.contains("the farm does not build impure derivations"),
        "{out}"
    );
}
