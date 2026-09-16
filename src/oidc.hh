#pragma once
// OIDC bearer tokens as an alternative to mTLS client certificates. Config
// schema and rule semantics follow niks3 so one file can serve both.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <fnmatch.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <openssl/evp.h>
#include <openssl/pem.h>

#include <grpcpp/server_context.h>
#include <jwt-cpp/traits/nlohmann-json/defaults.h>
#include <nlohmann/json.hpp>

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

constexpr size_t maxTokenBytes = 16UL * 1024;
using Decoded = jwt::decoded_jwt<jwt::traits::nlohmann_json>;

inline auto parseToken(const std::string & token) -> std::optional<Decoded>
{
    if (token.empty() || token.size() > maxTokenBytes) {
        return std::nullopt;
    }
    try {
        return jwt::decode(token);
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
    if (!parsed || !parsed->has_issuer()) {
        throw nix::Error("%s: not a JWT with an issuer", path);
    }
    return parsed->get_issuer();
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

// A JWKS entry turned into a PEM public key once, at fetch time.
struct Jwk
{
    std::string kid;
    std::string kty;
    std::string crv;
    std::string pem;
};

// jwt-cpp has helpers for RSA and EC components but not for OKP.
inline auto ed25519Pem(const std::string & xB64) -> std::string
{
    auto raw = jwt::base::decode<jwt::alphabet::base64url>(jwt::base::pad<jwt::alphabet::base64url>(xB64));
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> const key(
        EVP_PKEY_new_raw_public_key(
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): OpenSSL takes unsigned char.
            EVP_PKEY_ED25519, nullptr, reinterpret_cast<const unsigned char *>(raw.data()), raw.size()),
        EVP_PKEY_free);
    std::unique_ptr<BIO, decltype(&BIO_free_all)> const bio(BIO_new(BIO_s_mem()), BIO_free_all);
    if (!key || !bio || PEM_write_bio_PUBKEY(bio.get(), key.get()) != 1) {
        return {};
    }
    char * data = nullptr;
    auto len = BIO_get_mem_data(bio.get(), &data); // NOLINT(cppcoreguidelines-pro-type-cstyle-cast): macro
    return {data, static_cast<size_t>(len)};
}

// nullopt for anything unusable. JWKS content is remote input.
inline auto parseJwk(const Json & json) -> std::optional<Jwk>
{
    try {
        if (!json.is_object() || json.value("use", "sig") != "sig") {
            return std::nullopt;
        }
        auto str = [&](const char * name) -> std::string { return json.at(name).get<std::string>(); };
        Jwk jwk{.kid = json.value("kid", ""), .kty = json.value("kty", ""), .crv = json.value("crv", ""), .pem = {}};
        if (jwk.kty == "RSA") {
            jwk.pem = jwt::helper::create_public_key_from_rsa_components(str("n"), str("e"));
        } else if (jwk.kty == "EC") {
            jwk.pem = jwt::helper::create_public_key_from_ec_components(jwk.crv, str("x"), str("y"));
        } else if (jwk.kty == "OKP" && jwk.crv == "Ed25519") {
            jwk.pem = ed25519Pem(str("x"));
        }
        return jwk.pem.empty() ? std::nullopt : std::optional(std::move(jwk));
    } catch (...) {
        return std::nullopt;
    }
}

// Tests pin the time.
struct FixedClock
{
    std::chrono::system_clock::time_point at;
    [[nodiscard]] auto now() const -> std::chrono::system_clock::time_point
    {
        return at;
    }
};
using VerifierBuilder = jwt::verifier<FixedClock, jwt::traits::nlohmann_json>;

// Adds `key` as the algorithm `alg` names, if the key type fits. False if not.
inline auto allowKey(VerifierBuilder & verifier, const std::string & alg, const Jwk & key) -> bool
{
    namespace algo = jwt::algorithm;
    auto rsa = key.kty == "RSA";
    auto ecc = [&](std::string_view crv) -> bool { return key.kty == "EC" && key.crv == crv; };
    if (alg == "RS256" && rsa) {
        verifier.allow_algorithm(algo::rs256(key.pem));
    } else if (alg == "RS384" && rsa) {
        verifier.allow_algorithm(algo::rs384(key.pem));
    } else if (alg == "RS512" && rsa) {
        verifier.allow_algorithm(algo::rs512(key.pem));
    } else if (alg == "PS256" && rsa) {
        verifier.allow_algorithm(algo::ps256(key.pem));
    } else if (alg == "PS384" && rsa) {
        verifier.allow_algorithm(algo::ps384(key.pem));
    } else if (alg == "PS512" && rsa) {
        verifier.allow_algorithm(algo::ps512(key.pem));
    } else if (alg == "ES256" && ecc("P-256")) {
        verifier.allow_algorithm(algo::es256(key.pem));
    } else if (alg == "ES384" && ecc("P-384")) {
        verifier.allow_algorithm(algo::es384(key.pem));
    } else if (alg == "ES512" && ecc("P-521")) {
        verifier.allow_algorithm(algo::es512(key.pem));
    } else if (alg == "EdDSA" && key.kty == "OKP") {
        verifier.allow_algorithm(algo::ed25519(key.pem));
    } else {
        return false;
    }
    return true;
}

constexpr size_t clockSkewSecs = 60;

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
    auto verify(const std::string & token, std::chrono::system_clock::time_point now = std::chrono::system_clock::now()) -> Result
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
        // steady_clock starts at boot on Linux, so its epoch is not "long ago".
        std::optional<std::chrono::steady_clock::time_point> fetched;
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

public:
    static auto fetchDue(
        std::optional<std::chrono::steady_clock::time_point> fetched,
        std::chrono::steady_clock::time_point now,
        bool haveKey,
        std::chrono::seconds refetchOnMiss) -> bool
    {
        if (!fetched) {
            return true;
        }
        auto age = now - *fetched;
        return age > refetchAfter || (!haveKey && age > refetchOnMiss);
    }

private:
    // Keys of `provider` that the token's alg can use, narrowed to its kid if given.
    auto candidates(const Provider & provider, KeySet & set, const Decoded & token,
                    std::chrono::system_clock::time_point now) -> VerifierBuilder
    {
        auto alg = token.has_algorithm() ? token.get_algorithm() : "";
        auto kid = token.has_key_id() ? token.get_key_id() : "";
        std::scoped_lock const guard(set.lock);
        auto pick = [&]() -> std::optional<VerifierBuilder> {
            auto verifier = jwt::verify<FixedClock, jwt::traits::nlohmann_json>(FixedClock{now})
                                .with_issuer(provider.issuer)
                                .with_audience(provider.audience)
                                .leeway(clockSkewSecs);
            bool any = false;
            for (const auto & key : set.keys) {
                if (kid.empty() || key.kid == kid) {
                    any = allowKey(verifier, alg, key) || any;
                }
            }
            return any ? std::optional(std::move(verifier)) : std::nullopt;
        };
        auto res = pick();
        auto mono = std::chrono::steady_clock::now();
        if (fetchDue(set.fetched, mono, res.has_value(), refetchOnMiss)) {
            set.fetched = mono;
            try {
                set.keys = fetchKeys(provider);
            } catch (std::exception & err) {
                logLine(LogLevel::info, {{"event", "oidc_jwks_failed"}, {"provider", provider.name}, {"error", err.what()}});
                *set.fetched -= refetchAfter - refetchOnMiss; // retry soon, keep old keys
            }
            res = pick();
        }
        if (!res) {
            throw nix::Error("no signing keys for provider %s and alg %s (issuer unreachable?)", provider.name, alg);
        }
        return std::move(*res);
    }

    auto doVerify(const std::string & token, std::chrono::system_clock::time_point now) -> Result
    {
        auto parsed = parseToken(token);
        if (!parsed) {
            return fail("malformed token");
        }
        auto iss = parsed->has_issuer() ? parsed->get_issuer() : "";
        for (size_t idx = 0; idx < config.providers.size(); ++idx) {
            const auto & provider = config.providers.at(idx);
            if (provider.issuer != iss) {
                continue;
            }
            if (!parsed->has_expires_at()) {
                return fail("token has no exp");
            }
            std::error_code err;
            candidates(provider, keys.at(idx), *parsed, now).verify(*parsed, err);
            if (err) {
                return fail(provider.name + ": " + err.message());
            }
            auto claims = Json::parse(parsed->get_payload());
            auto sub = claims.value("sub", "");
            if (sub.empty() || sub.contains('\0')) {
                return fail("token lacks sub");
            }
            auto role = roleFor(provider, claims);
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
