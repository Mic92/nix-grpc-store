#pragma once
// OIDC bearer tokens as an alternative to mTLS client certificates. Config
// schema and rule semantics follow niks3 so one file can serve both.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <fnmatch.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/evp.h>
#include <openssl/obj_mac.h>
#include <openssl/param_build.h>
#include <openssl/rsa.h>
#include <openssl/types.h>

#include <grpcpp/server_context.h>
#include <nlohmann/json.hpp>

#include <nix/util/base-n.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/util.hh>

#include "acl.hh"
#include "farm.hh"
#include "logfmt.hh"

namespace nixgrpc::oidc {

using Json = nlohmann::json;

struct Rule
{
    std::map<std::string, std::vector<std::string>> boundClaims; // bound_subject lands under "sub"
    Role role = Role::readOnly;
};

struct Provider
{
    std::string name;
    std::string issuer;
    std::string jwksUrl;
    std::string audience;
    std::string caFile;
    std::string bearerTokenFile;
    std::vector<Rule> rules;
};

struct Config
{
    std::vector<Provider> providers;
    bool allowInsecure = false;
};

struct Identity
{
    std::string subject;      // "oidc:<provider>:<sub>"
    std::optional<Role> role; // nullopt: verified but no rule matched
};

inline auto base64UrlDecode(std::string_view input) -> std::string
{
    std::string plain(input);
    std::ranges::replace(plain, '-', '+');
    std::ranges::replace(plain, '_', '/');
    while (plain.size() % 4 != 0) {
        plain += '=';
    }
    return nix::base64::decode(plain); // throws on junk
}

struct ParsedToken
{
    Json header;
    Json claims;
    std::string_view signingInput;
    std::string signature;
};

constexpr size_t maxTokenBytes = 16UL * 1024;

inline auto parseToken(std::string_view token) -> std::optional<ParsedToken>
{
    if (token.empty() || token.size() > maxTokenBytes) {
        return std::nullopt;
    }
    auto first = token.find('.');
    auto second = first == std::string_view::npos ? first : token.find('.', first + 1);
    if (second == std::string_view::npos || token.find('.', second + 1) != std::string_view::npos) {
        return std::nullopt;
    }
    try {
        ParsedToken res{
            .header = Json::parse(base64UrlDecode(token.substr(0, first))),
            .claims = Json::parse(base64UrlDecode(token.substr(first + 1, second - first - 1))),
            .signingInput = token.substr(0, second),
            .signature = base64UrlDecode(token.substr(second + 1)),
        };
        if (!res.header.is_object() || !res.claims.is_object()) {
            return std::nullopt;
        }
        return res;
    } catch (...) {
        return std::nullopt;
    }
}

// niks3 scopes. The highest one a rule grants becomes the role.
inline auto scopeRole(std::string_view scope) -> Role
{
    if (scope == "read") {
        return Role::readOnly;
    }
    if (scope == "write") {
        return Role::write;
    }
    if (scope == "admin") {
        return Role::trusted;
    }
    throw nix::Error("unknown OIDC scope '%s' (want read, write or admin)", std::string(scope));
}

// Without scopes a rule grants write, as in niks3.
inline auto ruleFrom(const Json & json) -> Rule
{
    Rule rule{.boundClaims = json.value("bound_claims", decltype(Rule::boundClaims){})};
    if (auto sub = json.value("bound_subject", std::vector<std::string>{}); !sub.empty()) {
        rule.boundClaims["sub"] = std::move(sub);
    }
    auto scopes = json.value("scopes", std::vector<std::string>{"write"});
    if (scopes.empty()) {
        throw nix::Error("OIDC rule: scopes must not be empty");
    }
    rule.role = Role::readOnly;
    for (const auto & scope : scopes) {
        rule.role = std::max(rule.role, scopeRole(scope));
    }
    return rule;
}

inline auto issuerFromTokenFile(const std::string & path) -> std::string
{
    auto parsed = parseToken(nix::chomp(nix::readFile(path)));
    if (!parsed) {
        throw nix::Error("%s: not a JWT", path);
    }
    return parsed->claims.at("iss").get<std::string>();
}

inline auto providerFrom(const std::string & name, const Json & prov, bool allowInsecure) -> Provider
{
    Provider provider;
    provider.name = name;
    provider.issuer = prov.value("issuer", "");
    provider.jwksUrl = prov.value("jwks_url", "");
    provider.audience = prov.at("audience").get<std::string>();
    provider.caFile = prov.value("ca_file", "");
    provider.bearerTokenFile = prov.value("bearer_token_file", "");
    if (provider.issuer.empty() && !provider.bearerTokenFile.empty()) {
        provider.issuer = issuerFromTokenFile(provider.bearerTokenFile);
    }
    if (provider.issuer.empty() || provider.audience.empty()) {
        throw nix::Error("OIDC provider '%s': issuer and audience are required", name);
    }
    for (const auto & url : {provider.issuer, provider.jwksUrl}) {
        if (!url.empty() && !url.starts_with("https://") && !allowInsecure) {
            throw nix::Error("OIDC provider '%s': '%s' must be https", name, url);
        }
    }
    if (!prov.contains("rules")) {
        provider.rules.push_back(ruleFrom(prov));
        return provider;
    }
    if (prov.contains("bound_claims") || prov.contains("bound_subject") || prov.contains("scopes")) {
        throw nix::Error("OIDC provider '%s': rules exclude top-level bound_*/scopes", name);
    }
    for (const auto & rule : prov.at("rules")) {
        if (!rule.contains("scopes")) {
            throw nix::Error("OIDC provider '%s': every rule needs scopes", name);
        }
        provider.rules.push_back(ruleFrom(rule));
    }
    return provider;
}

inline auto loadConfig(const std::string & path) -> Config
{
    try {
        auto json = Json::parse(nix::readFile(path));
        Config cfg;
        cfg.allowInsecure = json.value("allow_insecure", false);
        for (const auto & [name, prov] : json.at("providers").items()) {
            cfg.providers.push_back(providerFrom(name, prov, cfg.allowInsecure));
        }
        if (cfg.providers.empty()) {
            throw nix::Error("no providers");
        }
        return cfg;
    } catch (nix::Error &) {
        throw;
    } catch (std::exception & err) { // nlohmann type/parse errors
        throw nix::Error("%s: %s", path, err.what());
    }
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters): patterns, then values.
inline auto globAny(const std::vector<std::string> & patterns, const std::vector<std::string> & values) -> bool
{
    return std::ranges::any_of(values, [&](const std::string & val) -> bool {
        return !val.contains('\0') && std::ranges::any_of(patterns, [&](const std::string & pat) -> bool {
            return fnmatch(pat.c_str(), val.c_str(), 0) == 0;
        });
    });
}

// Dotted names reach into nested objects. At each level the longest literal
// key wins, so "kubernetes.io.namespace" finds {"kubernetes.io": {"namespace": …}}.
inline auto lookupClaim(const Json & claims, std::string_view name) -> const Json *
{
    const Json * cur = &claims;
    while (cur->is_object() && !name.empty()) {
        // Longest prefix of `name` that is a key of *cur.
        auto dot = name.size();
        auto found = cur->end();
        while ((found = cur->find(std::string(name.substr(0, dot)))) == cur->end()) {
            dot = dot == 0 ? std::string_view::npos : name.rfind('.', dot - 1);
            if (dot == std::string_view::npos) {
                return nullptr;
            }
        }
        cur = &*found;
        if (dot == name.size()) {
            return cur;
        }
        name.remove_prefix(dot + 1);
    }
    return nullptr;
}

inline auto claimStrings(const Json & value) -> std::vector<std::string>
{
    if (value.is_string()) {
        return {value.get<std::string>()};
    }
    std::vector<std::string> res;
    if (value.is_array()) {
        for (const auto & item : value) {
            if (item.is_string()) {
                res.push_back(item.get<std::string>());
            }
        }
        return res;
    }
    return {value.dump()};
}

inline auto ruleMatches(const Rule & rule, const Json & claims) -> bool
{
    return std::ranges::all_of(rule.boundClaims, [&](const auto & bound) -> bool {
        const auto * value = lookupClaim(claims, bound.first);
        return value != nullptr && globAny(bound.second, claimStrings(*value));
    });
}

// Union over matching rules, as in niks3. Roles are nested so that is the max.
inline auto roleFor(const Provider & provider, const Json & claims) -> std::optional<Role>
{
    std::optional<Role> role;
    for (const auto & rule : provider.rules) {
        if (ruleMatches(rule, claims)) {
            role = role ? std::max(*role, rule.role) : rule.role;
        }
    }
    return role;
}

inline auto audienceMatches(const Json & claims, const std::string & audience) -> bool
{
    auto found = claims.find("aud");
    return found != claims.end() && std::ranges::contains(claimStrings(*found), audience);
}

struct Alg
{
    std::string_view name;
    std::string_view kty;
    std::string_view crv; // JWK "crv", empty for RSA
    const char * digest;  // nullptr for EdDSA
    bool pss = false;
};

constexpr std::array<Alg, 10> algs{{
    {.name = "RS256", .kty = "RSA", .crv = "", .digest = "SHA256"},
    {.name = "RS384", .kty = "RSA", .crv = "", .digest = "SHA384"},
    {.name = "RS512", .kty = "RSA", .crv = "", .digest = "SHA512"},
    {.name = "PS256", .kty = "RSA", .crv = "", .digest = "SHA256", .pss = true},
    {.name = "PS384", .kty = "RSA", .crv = "", .digest = "SHA384", .pss = true},
    {.name = "PS512", .kty = "RSA", .crv = "", .digest = "SHA512", .pss = true},
    {.name = "ES256", .kty = "EC", .crv = "P-256", .digest = "SHA256"},
    {.name = "ES384", .kty = "EC", .crv = "P-384", .digest = "SHA384"},
    {.name = "ES512", .kty = "EC", .crv = "P-521", .digest = "SHA512"},
    {.name = "EdDSA", .kty = "OKP", .crv = "Ed25519", .digest = nullptr},
}};

inline auto findAlg(std::string_view name) -> const Alg *
{
    const auto * found = std::ranges::find(algs, name, &Alg::name);
    return found == algs.end() ? nullptr : found;
}

struct Jwk
{
    std::string kid;
    std::string kty;
    std::string crv;
    std::shared_ptr<EVP_PKEY> key;

    [[nodiscard]] auto usableFor(const Alg & alg, const std::string & wantKid) const -> bool
    {
        return kty == alg.kty && crv == alg.crv && (wantKid.empty() || kid == wantKid);
    }
};

namespace detail {

constexpr size_t maxKeyBytes = 2048; // 16k-bit RSA

// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast): OpenSSL takes unsigned char.
inline auto uchars(const std::string & bytes) -> const unsigned char *
{
    return reinterpret_cast<const unsigned char *>(bytes.data());
}

inline auto uchars(std::string & bytes) -> unsigned char *
{
    return reinterpret_cast<unsigned char *>(bytes.data());
}
// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)

using BnPtr = std::unique_ptr<BIGNUM, decltype(&BN_free)>;

inline auto bnFrom(const std::string & bytes) -> BnPtr
{
    if (bytes.empty() || bytes.size() > maxKeyBytes) {
        return {nullptr, BN_free};
    }
    return {BN_bin2bn(uchars(bytes), static_cast<int>(bytes.size()), nullptr), BN_free};
}

inline auto pkeyFromParams(const char * type, OSSL_PARAM_BLD * bld) -> std::shared_ptr<EVP_PKEY>
{
    std::unique_ptr<OSSL_PARAM, decltype(&OSSL_PARAM_free)> const params(OSSL_PARAM_BLD_to_param(bld), OSSL_PARAM_free);
    std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> const ctx(
        EVP_PKEY_CTX_new_from_name(nullptr, type, nullptr), EVP_PKEY_CTX_free);
    EVP_PKEY * raw = nullptr;
    if (!params || !ctx || EVP_PKEY_fromdata_init(ctx.get()) <= 0
        || EVP_PKEY_fromdata(ctx.get(), &raw, EVP_PKEY_PUBLIC_KEY, params.get()) <= 0) {
        return nullptr;
    }
    return {raw, EVP_PKEY_free};
}

constexpr std::array<std::pair<std::string_view, const char *>, 3> curves{{
    {"P-256", SN_X9_62_prime256v1}, {"P-384", SN_secp384r1}, {"P-521", SN_secp521r1}}};

inline auto makeKey(const Json & jwk, const std::string & kty, const std::string & crv) -> std::shared_ptr<EVP_PKEY>
{
    auto field = [&](const char * name) -> std::string { return base64UrlDecode(jwk.at(name).get<std::string>()); };
    std::unique_ptr<OSSL_PARAM_BLD, decltype(&OSSL_PARAM_BLD_free)> const bld(OSSL_PARAM_BLD_new(), OSSL_PARAM_BLD_free);
    if (!bld) {
        return nullptr;
    }
    if (kty == "RSA") {
        auto mod = bnFrom(field("n"));
        auto exp = bnFrom(field("e"));
        if (!mod || !exp || OSSL_PARAM_BLD_push_BN(bld.get(), OSSL_PKEY_PARAM_RSA_N, mod.get()) != 1
            || OSSL_PARAM_BLD_push_BN(bld.get(), OSSL_PKEY_PARAM_RSA_E, exp.get()) != 1) {
            return nullptr;
        }
        return pkeyFromParams("RSA", bld.get());
    }
    if (kty == "EC") {
        const auto * curve = std::ranges::find(curves, crv, &decltype(curves)::value_type::first);
        auto pub = '\x04' + field("x") + field("y");
        if (curve == curves.end() || pub.size() > maxKeyBytes
            || OSSL_PARAM_BLD_push_utf8_string(bld.get(), OSSL_PKEY_PARAM_GROUP_NAME, curve->second, 0) != 1
            || OSSL_PARAM_BLD_push_octet_string(bld.get(), OSSL_PKEY_PARAM_PUB_KEY, pub.data(), pub.size()) != 1) {
            return nullptr;
        }
        return pkeyFromParams("EC", bld.get());
    }
    if (kty == "OKP" && crv == "Ed25519") {
        auto pub = field("x");
        return {EVP_PKEY_new_raw_public_key(EVP_PKEY_ED25519, nullptr, uchars(pub), pub.size()), EVP_PKEY_free};
    }
    return nullptr;
}

} // namespace detail

// nullopt for anything unusable. JWKS content is remote input.
inline auto parseJwk(const Json & jwk) -> std::optional<Jwk>
{
    try {
        if (!jwk.is_object() || jwk.value("use", "sig") != "sig") {
            return std::nullopt;
        }
        Jwk res{.kid = jwk.value("kid", ""), .kty = jwk.value("kty", ""), .crv = jwk.value("crv", ""), .key = nullptr};
        res.key = detail::makeKey(jwk, res.kty, res.crv);
        return res.key ? std::optional(std::move(res)) : std::nullopt;
    } catch (...) {
        return std::nullopt;
    }
}

// JWS carries ECDSA as fixed-width r||s, OpenSSL wants DER.
inline auto ecdsaToDer(const std::string & raw) -> std::string
{
    if (raw.empty() || raw.size() % 2 != 0) {
        return {};
    }
    auto half = raw.size() / 2;
    std::unique_ptr<ECDSA_SIG, decltype(&ECDSA_SIG_free)> const sig(ECDSA_SIG_new(), ECDSA_SIG_free);
    auto sigR = detail::bnFrom(raw.substr(0, half));
    auto sigS = detail::bnFrom(raw.substr(half));
    if (!sig || !sigR || !sigS || ECDSA_SIG_set0(sig.get(), sigR.get(), sigS.get()) != 1) {
        return {};
    }
    std::ignore = sigR.release(); // owned by sig now
    std::ignore = sigS.release();
    int const len = i2d_ECDSA_SIG(sig.get(), nullptr);
    if (len <= 0) {
        return {};
    }
    std::string der(static_cast<size_t>(len), '\0');
    auto * out = detail::uchars(der);
    i2d_ECDSA_SIG(sig.get(), &out);
    return der;
}

inline auto verifySignature(const Alg & alg, EVP_PKEY & key, std::string_view signingInput, std::string sig) -> bool
{
    if (alg.kty == "EC") {
        sig = ecdsaToDer(sig);
    }
    if (sig.empty()) {
        return false;
    }
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> const ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    EVP_PKEY_CTX * pctx = nullptr;
    if (!ctx || EVP_DigestVerifyInit_ex(ctx.get(), &pctx, alg.digest, nullptr, nullptr, &key, nullptr) <= 0) {
        return false;
    }
    if (alg.pss
        && (EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_PSS_PADDING) <= 0
            || EVP_PKEY_CTX_set_rsa_pss_saltlen(pctx, RSA_PSS_SALTLEN_AUTO) <= 0)) {
        return false;
    }
    std::string const input(signingInput);
    return EVP_DigestVerify(ctx.get(), detail::uchars(sig), sig.size(), detail::uchars(input), input.size()) == 1;
}

constexpr double clockSkewSecs = 60;

// exp is mandatory. Compared as double so absurd values cannot overflow.
inline auto timeValid(const Json & claims, std::chrono::system_clock::time_point now) -> bool
{
    auto const secs = std::chrono::duration<double>(now.time_since_epoch()).count();
    auto const skew = clockSkewSecs;
    auto exp = claims.find("exp");
    if (exp == claims.end() || !exp->is_number() || exp->get<double>() + skew < secs) {
        return false;
    }
    auto nbf = claims.find("nbf");
    return nbf == claims.end() || !nbf->is_number() || nbf->get<double>() - skew <= secs;
}

class Verifier
{
public:
    struct Result
    {
        std::optional<Identity> identity;
        std::string error; // for logs, not for the client
    };

    static constexpr std::chrono::seconds defaultRefetchOnMiss{30};

    explicit Verifier(Config cfg, std::chrono::seconds refetchOnMiss = defaultRefetchOnMiss)
        : config(std::move(cfg))
        , refetchOnMiss(refetchOnMiss)
        , keys(config.providers.size())
    {
    }

    // Total: unknown/bad input is a Result with no identity.
    auto verify(std::string_view token, std::chrono::system_clock::time_point now = std::chrono::system_clock::now()) -> Result
    {
        try {
            return doVerify(token, now);
        } catch (std::exception & err) {
            return fail(std::string("malformed token: ") + err.what());
        } catch (...) {
            return fail("malformed token");
        }
    }

private:
    // Signing keys per provider. Fetched on first use, not at startup, so the
    // daemon comes up while the issuer is unreachable and recovers later.
    struct KeySet
    {
        std::mutex lock;
        std::vector<Jwk> keys;
        std::chrono::steady_clock::time_point fetched; // epoch: never
    };

    static constexpr std::chrono::minutes refetchAfter{60};
    static constexpr long httpOk = 200;

    Config config;
    std::chrono::seconds refetchOnMiss;
    std::vector<KeySet> keys;

    static auto fail(std::string why) -> Result
    {
        return {.identity = std::nullopt, .error = std::move(why)};
    }

    [[nodiscard]] auto get(const Provider & provider, const std::string & url) const -> Json
    {
        if (!url.starts_with("https://") && !config.allowInsecure) {
            throw nix::Error("refusing non-https OIDC URL '%s'", url);
        }
        auto bearer = provider.bearerTokenFile.empty() ? "" : nix::chomp(nix::readFile(provider.bearerTokenFile));
        http::Call call(url, bearer, std::nullopt);
        // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg): curl_easy_setopt.
        call.opt(CURLOPT_FOLLOWLOCATION, 1L);
        if (!provider.caFile.empty()) {
            call.opt(CURLOPT_CAINFO, provider.caFile.c_str());
        }
        // NOLINTEND(cppcoreguidelines-pro-type-vararg)
        auto status = call.perform();
        if (status != httpOk) {
            throw nix::Error("GET %s: HTTP %d", url, status);
        }
        return Json::parse(call.body());
    }

    [[nodiscard]] auto fetchKeys(const Provider & provider) const -> std::vector<Jwk>
    {
        auto url = provider.jwksUrl;
        if (url.empty()) {
            auto issuer = provider.issuer;
            while (issuer.ends_with('/')) {
                issuer.pop_back();
            }
            url = get(provider, issuer + "/.well-known/openid-configuration").at("jwks_uri").get<std::string>();
        }
        std::vector<Jwk> fresh;
        for (const auto & jwk : get(provider, url).at("keys")) {
            if (auto key = parseJwk(jwk)) {
                fresh.push_back(std::move(*key));
            }
        }
        if (fresh.empty()) {
            throw nix::Error("%s: no usable keys", url);
        }
        logLine(LogLevel::info, {{"event", "oidc_jwks"}, {"provider", provider.name}, {"keys", std::to_string(fresh.size())}});
        return fresh;
    }

    // Keys of `provider` usable for `alg`, narrowed to `kid` if given.
    auto candidates(const Provider & provider, KeySet & set, const Alg & alg, const std::string & kid)
        -> std::vector<std::shared_ptr<EVP_PKEY>>
    {
        std::scoped_lock const guard(set.lock);
        auto pick = [&]() -> std::vector<std::shared_ptr<EVP_PKEY>> {
            std::vector<std::shared_ptr<EVP_PKEY>> res;
            for (const auto & key : set.keys) {
                if (key.usableFor(alg, kid)) {
                    res.push_back(key.key);
                }
            }
            return res;
        };
        auto res = pick();
        auto age = std::chrono::steady_clock::now() - set.fetched;
        if (age > refetchAfter || (res.empty() && age > refetchOnMiss)) {
            set.fetched = std::chrono::steady_clock::now();
            try {
                set.keys = fetchKeys(provider);
            } catch (std::exception & err) {
                logLine(LogLevel::info, {{"event", "oidc_jwks_failed"}, {"provider", provider.name}, {"error", err.what()}});
                set.fetched -= refetchAfter - refetchOnMiss; // retry soon, keep old keys
            }
            res = pick();
        }
        return res;
    }

    auto doVerify(std::string_view token, std::chrono::system_clock::time_point now) -> Result
    {
        auto parsed = parseToken(token);
        if (!parsed) {
            return fail("malformed token");
        }
        const auto * alg = findAlg(parsed->header.value("alg", ""));
        if (alg == nullptr) {
            return fail("unsupported alg");
        }
        auto iss = parsed->claims.value("iss", "");
        for (size_t idx = 0; idx < config.providers.size(); ++idx) {
            const auto & provider = config.providers.at(idx);
            if (provider.issuer != iss) {
                continue;
            }
            if (!audienceMatches(parsed->claims, provider.audience)) {
                return fail("audience mismatch for provider " + provider.name);
            }
            if (!timeValid(parsed->claims, now)) {
                return fail("token expired or not yet valid");
            }
            auto pkeys = candidates(provider, keys.at(idx), *alg, parsed->header.value("kid", ""));
            if (pkeys.empty()) {
                return fail("no signing keys for provider " + provider.name + " (issuer unreachable?)");
            }
            if (!std::ranges::any_of(pkeys, [&](const auto & key) -> bool {
                    return verifySignature(*alg, *key, parsed->signingInput, parsed->signature);
                })) {
                return fail("bad signature for provider " + provider.name);
            }
            auto sub = parsed->claims.value("sub", "");
            if (sub.empty() || sub.contains('\0')) {
                return fail("token lacks sub");
            }
            auto role = roleFor(provider, parsed->claims);
            return {
                .identity = Identity{.subject = "oidc:" + provider.name + ":" + sub, .role = role},
                .error = role ? "" : "no rule matched"};
        }
        return fail("unknown issuer '" + iss + "'");
    }
};

inline auto bearerToken(const grpc::ServerContext & context) -> std::optional<std::string>
{
    auto const & metadata = context.client_metadata();
    auto found = metadata.find("authorization");
    if (found == metadata.end()) {
        return std::nullopt;
    }
    std::string_view value(found->second.data(), found->second.size());
    constexpr std::string_view prefix = "Bearer ";
    if (!value.starts_with(prefix) || value.size() == prefix.size()) {
        return std::nullopt;
    }
    value.remove_prefix(prefix.size());
    return std::string(value);
}

} // namespace nixgrpc::oidc
