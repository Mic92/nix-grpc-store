// Signs tokens with fresh keys, serves the JWKS from a directory given on the
// command line (python http.server in the wrapper) and checks the verifier.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/types.h>

#include <nlohmann/json.hpp> // NOLINT(misc-include-cleaner): json_fwd.hpp only declares.
#include <nlohmann/json_fwd.hpp>

#include <nix/util/base-n.hh>
#include <nix/util/file-system.hh>

#include "acl.hh"
#include "oidc.hh"

// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic): OpenSSL byte buffers.
namespace {

using Json = nlohmann::json;
using nixgrpc::Role;
using nixgrpc::oidc::loadConfig;
using nixgrpc::oidc::Verifier;

constexpr size_t p256Bytes = 32;
constexpr size_t ed25519Bytes = 32;
constexpr unsigned rsaBits = 2048;
constexpr int64_t validFor = 300;
constexpr int64_t longAgo = 3600;

auto ossl(char * ptr) -> unsigned char *
{
    return reinterpret_cast<unsigned char *>(ptr);
}

auto ossl(const char * ptr) -> const unsigned char *
{
    return reinterpret_cast<const unsigned char *>(ptr);
}

auto b64url(std::string_view bytes) -> std::string
{
    auto out = nix::base64::encode(std::as_bytes(std::span(bytes.data(), bytes.size())));
    for (auto & chr : out) {
        if (chr == '+') {
            chr = '-';
        } else if (chr == '/') {
            chr = '_';
        }
    }
    while (!out.empty() && out.back() == '=') {
        out.pop_back();
    }
    return out;
}

auto bnParam(EVP_PKEY * key, const char * name, size_t padTo = 0) -> std::string
{
    BIGNUM * num = nullptr;
    EVP_PKEY_get_bn_param(key, name, &num);
    auto width = padTo != 0 ? padTo : static_cast<size_t>(BN_num_bytes(num));
    std::string out(width, '\0');
    BN_bn2binpad(num, ossl(out.data()), static_cast<int>(width));
    BN_free(num);
    return out;
}

enum class Kind : std::uint8_t { ec, rsa, pss, ed };

struct Key
{
    std::shared_ptr<EVP_PKEY> pkey;
    Kind kind;
    std::string alg;
    std::string kid;

    static auto generate(Kind kind, const std::string & kid) -> Key
    {
        EVP_PKEY * raw = nullptr;
        std::string alg;
        switch (kind) {
        case Kind::ec:
            raw = EVP_EC_gen("P-256"); // NOLINT(cppcoreguidelines-pro-type-vararg)
            alg = "ES256";
            break;
        case Kind::ed:
            raw = EVP_PKEY_Q_keygen(nullptr, nullptr, "ED25519"); // NOLINT(cppcoreguidelines-pro-type-vararg)
            alg = "EdDSA";
            break;
        case Kind::rsa:
        case Kind::pss:
            raw = EVP_RSA_gen(rsaBits); // NOLINT(cppcoreguidelines-pro-type-vararg)
            alg = kind == Kind::pss ? "PS256" : "RS256";
            break;
        }
        if (raw == nullptr) {
            std::cerr << "keygen failed\n";
            std::exit(1); // NOLINT(concurrency-mt-unsafe)
        }
        return {.pkey = {raw, EVP_PKEY_free}, .kind = kind, .alg = alg, .kid = kid};
    }

    [[nodiscard]] auto jwk() const -> Json
    {
        Json jwk{{"kid", kid}, {"use", "sig"}, {"alg", alg}};
        switch (kind) {
        case Kind::ec:
            jwk.emplace("kty", "EC");
            jwk.emplace("crv", "P-256");
            jwk.emplace("x", b64url(bnParam(pkey.get(), OSSL_PKEY_PARAM_EC_PUB_X, p256Bytes)));
            jwk.emplace("y", b64url(bnParam(pkey.get(), OSSL_PKEY_PARAM_EC_PUB_Y, p256Bytes)));
            break;
        case Kind::ed: {
            std::string pub(ed25519Bytes, '\0');
            size_t len = pub.size();
            EVP_PKEY_get_raw_public_key(pkey.get(), ossl(pub.data()), &len);
            jwk.emplace("kty", "OKP");
            jwk.emplace("crv", "Ed25519");
            jwk.emplace("x", b64url(pub));
            break;
        }
        case Kind::rsa:
        case Kind::pss:
            jwk.emplace("kty", "RSA");
            jwk.emplace("n", b64url(bnParam(pkey.get(), OSSL_PKEY_PARAM_RSA_N)));
            jwk.emplace("e", b64url(bnParam(pkey.get(), OSSL_PKEY_PARAM_RSA_E)));
            break;
        }
        return jwk;
    }

    // JWS wants ECDSA as fixed-width r||s.
    static auto derToJose(const std::string & der) -> std::string
    {
        const auto * ptr = ossl(der.data());
        std::unique_ptr<ECDSA_SIG, decltype(&ECDSA_SIG_free)> const parsed(
            d2i_ECDSA_SIG(nullptr, &ptr, static_cast<long>(der.size())), ECDSA_SIG_free);
        std::string raw(2 * p256Bytes, '\0');
        BN_bn2binpad(ECDSA_SIG_get0_r(parsed.get()), ossl(raw.data()), p256Bytes);
        BN_bn2binpad(ECDSA_SIG_get0_s(parsed.get()), ossl(raw.data()) + p256Bytes, p256Bytes);
        return raw;
    }

    [[nodiscard]] auto sign(const Json & claims, const std::string & headerAlg = "") const -> std::string
    {
        Json const header{{"alg", headerAlg.empty() ? alg : headerAlg}, {"typ", "JWT"}, {"kid", kid}};
        auto input = b64url(header.dump()) + "." + b64url(claims.dump());
        std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> const ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
        EVP_PKEY_CTX * pctx = nullptr;
        EVP_DigestSignInit_ex(
            ctx.get(), &pctx, kind == Kind::ed ? nullptr : "SHA256", nullptr, nullptr, pkey.get(), nullptr);
        if (kind == Kind::pss) {
            EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_PSS_PADDING);
            EVP_PKEY_CTX_set_rsa_pss_saltlen(pctx, RSA_PSS_SALTLEN_DIGEST);
        }
        size_t len = 0;
        EVP_DigestSign(ctx.get(), nullptr, &len, ossl(input.data()), input.size());
        std::string sig(len, '\0');
        EVP_DigestSign(ctx.get(), ossl(sig.data()), &len, ossl(input.data()), input.size());
        sig.resize(len);
        if (kind == Kind::ec) {
            sig = derToJose(sig);
        }
        return input + "." + b64url(sig);
    }
};

class Checks
{
    int failures = 0;

public:
    void operator()(bool cond, const char * what)
    {
        if (!cond) {
            std::cerr << "FAIL: " << what << "\n";
            ++failures;
        }
    }

    [[nodiscard]] auto result() const -> int
    {
        std::cerr << (failures == 0 ? "ok" : "failed") << "\n";
        return failures == 0 ? 0 : 1;
    }
};

auto now() -> int64_t
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

auto writeConfig(const std::string & dir, const std::string & base) -> void
{
    nix::writeFile(
        dir + "/oidc.json",
        Json{
            {"allow_insecure", true},
            {"providers",
             {{"ci",
               {{"issuer", base},
                {"audience", "grpc://cache"},
                {"rules",
                 {{{"bound_subject", {"repo:myorg/*"}}, {"scopes", {"write"}}},
                  {{"bound_claims", {{"repository_owner", {"myorg"}}, {"ref", {"refs/heads/main"}}}},
                   {"scopes", {"admin"}}},
                  {{"bound_claims", {{"groups", {"readers"}}}}, {"scopes", {"read"}}}}}}},
              {"k8s",
               {{"audience", "grpc://cache"},
                {"bearer_token_file", dir + "/sa-token"},
                {"jwks_url", base + "/jwks.json"},
                {"bound_claims", {{"kubernetes.io.namespace", {"builders"}}}}}}}}}
            .dump());
}

auto run(const std::string & dir, const std::string & base) -> int
{
    Checks check;
    auto ecKey = Key::generate(Kind::ec, "ec-1");
    auto rsaKey = Key::generate(Kind::rsa, "rsa-1");
    auto pssKey = Key::generate(Kind::pss, "pss-1");
    auto edKey = Key::generate(Kind::ed, "ed-1");
    auto stranger = Key::generate(Kind::ec, "ec-1"); // same kid, different key

    // k8s style: issuer taken from the SA token, JWKS behind explicit URL.
    nix::writeFile(dir + "/sa-token", ecKey.sign({{"iss", "https://kubernetes.default.svc"}, {"sub", "x"}}));
    writeConfig(dir, base);

    // Issuer serves nothing yet: config loads, verifier constructs, tokens are refused.
    auto cfg = loadConfig(dir + "/oidc.json");
    check(cfg.providers.size() == 2, "two providers");
    Verifier verifier(cfg, std::chrono::seconds(0));

    auto claims = [&](const Json & extra) -> Json {
        Json res{{"iss", base}, {"aud", "grpc://cache"}, {"exp", now() + validFor}, {"iat", now()}};
        res.update(extra);
        return res;
    };
    auto good = claims({{"sub", "repo:myorg/a:b"}});

    {
        auto res = verifier.verify(ecKey.sign(good));
        check(!res.identity && res.error.contains("no signing keys"), "issuer down -> refused, no crash");
    }
    nix::writeFile(dir + "/jwks.json", "garbage");
    nix::writeFile(dir + "/.well-known/openid-configuration", Json{{"jwks_uri", base + "/jwks.json"}}.dump());
    check(!verifier.verify(ecKey.sign(good)).identity, "garbage JWKS -> refused");
    nix::writeFile(
        dir + "/jwks.json",
        Json{{"keys",
              {"junk", Json{{"kty", "RSA"}, {"n", "!!"}}, ecKey.jwk(), rsaKey.jwk(), pssKey.jwk(), edKey.jwk()}}}
            .dump());

    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/thing:ref:refs/heads/dev"}})));
        check(res.identity && res.identity->role == Role::write, "ES256 subject rule -> write");
        check(
            res.identity && res.identity->subject == "oidc:ci:repo:myorg/thing:ref:refs/heads/dev", "subject naming");
    }
    {
        auto res = verifier.verify(rsaKey.sign(claims(
            {{"sub", "repo:myorg/x:ref:refs/heads/main"}, {"repository_owner", "myorg"}, {"ref", "refs/heads/main"}})));
        check(res.identity && res.identity->role == Role::trusted, "RS256 union of rules -> trusted");
    }
    {
        auto res = verifier.verify(pssKey.sign(claims({{"sub", "someone"}, {"groups", {"other", "readers"}}})));
        check(res.identity && res.identity->role == Role::readOnly, "PS256 array claim -> read-only");
    }
    {
        auto res = verifier.verify(edKey.sign(good));
        check(res.identity && res.identity->role == Role::write, "EdDSA");
    }
    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:other/x:y"}})));
        check(res.identity && !res.identity->role, "verified but no rule -> no role");
    }
    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"aud", "someone-else"}})));
        check(!res.identity && res.error.contains("audience"), "wrong audience");
    }
    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"aud", {"x", "grpc://cache"}}})));
        check(res.identity && res.identity->role, "audience array");
    }
    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"exp", now() - longAgo}})));
        check(!res.identity && res.error.contains("expired"), "expired");
        check(!verifier.verify(ecKey.sign({{"iss", base}, {"aud", "grpc://cache"}, {"sub", "s"}})).identity, "no exp");
        auto future = claims({{"sub", "repo:myorg/a:b"}, {"nbf", now() + longAgo}});
        check(!verifier.verify(ecKey.sign(future)).identity, "not yet valid");
    }
    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"iss", "https://elsewhere"}})));
        check(!res.identity && res.error.contains("issuer"), "unknown issuer");
    }
    {
        auto res = verifier.verify(stranger.sign(good));
        check(!res.identity && res.error.contains("signature"), "foreign key with known kid");
    }
    {
        auto res = verifier.verify(rsaKey.sign(good, "HS256"));
        check(!res.identity && res.error.contains("alg"), "HS256 refused");
        check(!verifier.verify(rsaKey.sign(good, "none")).identity, "alg none refused");
        // RSA key material presented as ES256 must not verify.
        check(!verifier.verify(rsaKey.sign(good, "ES256")).identity, "alg/key type confusion");
    }
    {
        auto token = ecKey.sign(good);
        constexpr size_t intoSignature = 10;
        auto & chr = token.at(token.size() - intoSignature);
        chr = chr == 'A' ? 'B' : 'A';
        check(!verifier.verify(token).identity, "tampered signature");
        check(!verifier.verify("not.a.jwt").identity, "garbage");
        check(!verifier.verify("").identity, "empty");
        check(!verifier.verify("a.b").identity, "two parts");
        check(!verifier.verify("a.b.c.d").identity, "four parts");
        constexpr size_t huge = 100000;
        check(!verifier.verify(std::string(huge, 'a')).identity, "oversized");
        check(!verifier.verify(ecKey.sign(claims({{"sub", 42}}))).identity, "non-string sub"); // NOLINT(*-magic-numbers)
    }
    {
        constexpr int64_t minute = 60;
        auto res = verifier.verify(ecKey.sign(
            {{"iss", "https://kubernetes.default.svc"},
             {"aud", {"grpc://cache"}},
             {"exp", now() + minute},
             {"sub", "system:serviceaccount:builders:ci"},
             {"kubernetes.io", {{"namespace", "builders"}, {"pod", {{"name", "p"}}}}}}));
        check(res.identity && res.identity->role == Role::write, "k8s: issuer from SA token, nested claim, default write");
        check(res.identity && res.identity->subject == "oidc:k8s:system:serviceaccount:builders:ci", "k8s subject");
    }
    return check.result();
}

} // namespace
// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)

auto main(int argc, char ** argv) -> int
{
    try {
        if (argc != 3) {
            std::cerr << "usage: oidc-test JWKS-DIR BASE-URL\n";
            return 2;
        }
        std::vector<std::string> const args(argv, argv + argc); // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return run(args.at(1), args.at(2));
    } catch (std::exception & err) {
        std::cerr << "error: " << err.what() << "\n";
        return 1;
    }
}
