// Signs tokens with fresh keys, serves the JWKS from a directory given on the
// command line (python http.server in the wrapper) and checks the verifier.

#include <atomic>
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
#include <thread>
#include <vector>

#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/core_names.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/types.h>

#include <nlohmann/json.hpp> // NOLINT(misc-include-cleaner): json_fwd.hpp only declares.
#include <nlohmann/json_fwd.hpp>

#include <nix/util/base-n.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>

#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>

#include "acl.hh"
#include "coordinator.hh"
#include "dispatcher.hh"
#include "metrics.hh"
#include "reverse-gateway.hh"
#include "reverse-relay.hh"
#include "reverse-request.hh"
#include "oidc.hh"
#include "reverse-protocol.hh"
#include "reverse-session.hh"
#include "worker-identity.hh"

// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic): OpenSSL
// byte buffers.
namespace
{

using Json = nlohmann::json;
using nixgrpc::Role;
using nixgrpc::oidc::loadConfig;
using nixgrpc::oidc::Verifier;

constexpr size_t p256Bytes = 32;
constexpr size_t ed25519Bytes = 32;
constexpr unsigned rsaBits = 2048;
constexpr int64_t validFor = 300;
constexpr int64_t longAgo = 3600;

auto ossl(char *ptr) -> unsigned char * { return reinterpret_cast<unsigned char *>(ptr); }

auto ossl(const char *ptr) -> const unsigned char * { return reinterpret_cast<const unsigned char *>(ptr); }

auto b64url(std::string_view bytes) -> std::string
{
    auto out = nix::base64::encode(std::as_bytes(std::span(bytes.data(), bytes.size())));
    for (auto &chr : out)
    {
        if (chr == '+')
        {
            chr = '-';
        }
        else if (chr == '/')
        {
            chr = '_';
        }
    }
    while (!out.empty() && out.back() == '=')
    {
        out.pop_back();
    }
    return out;
}

auto bnParam(EVP_PKEY const *key, const char *name, size_t padTo = 0) -> std::string
{
    BIGNUM *num = nullptr;
    EVP_PKEY_get_bn_param(key, name, &num);
    auto const width = padTo != 0 ? padTo : static_cast<size_t>(BN_num_bytes(num));
    std::string out(width, '\0');
    BN_bn2binpad(num, ossl(out.data()), static_cast<int>(width));
    BN_free(num);
    return out;
}

enum class Kind : std::uint8_t
{
    ec,
    rsa,
    pss,
    ed
};

struct Key
{
    std::shared_ptr<EVP_PKEY> pkey;
    Kind kind;
    std::string alg;
    std::string kid;

    static auto generate(Kind kind, const std::string &kid) -> Key
    {
        EVP_PKEY *raw = nullptr;
        std::string alg;
        switch (kind)
        {
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
        if (raw == nullptr)
        {
            std::cerr << "keygen failed\n";
            std::exit(1); // NOLINT(concurrency-mt-unsafe)
        }
        return {.pkey = {raw, EVP_PKEY_free}, .kind = kind, .alg = alg, .kid = kid};
    }

    [[nodiscard]] auto jwk() const -> Json
    {
        Json jwk{{"kid", kid}, {"use", "sig"}, {"alg", alg}};
        switch (kind)
        {
        case Kind::ec:
            jwk.emplace("kty", "EC");
            jwk.emplace("crv", "P-256");
            jwk.emplace("x", b64url(bnParam(pkey.get(), OSSL_PKEY_PARAM_EC_PUB_X, p256Bytes)));
            jwk.emplace("y", b64url(bnParam(pkey.get(), OSSL_PKEY_PARAM_EC_PUB_Y, p256Bytes)));
            break;
        case Kind::ed:
        {
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
    static auto derToJose(const std::string &der) -> std::string
    {
        const auto *ptr = ossl(der.data());
        std::unique_ptr<ECDSA_SIG, decltype(&ECDSA_SIG_free)> const parsed(
            d2i_ECDSA_SIG(nullptr, &ptr, static_cast<long>(der.size())), ECDSA_SIG_free);
        std::string raw(2 * p256Bytes, '\0');
        BN_bn2binpad(ECDSA_SIG_get0_r(parsed.get()), ossl(raw.data()), p256Bytes);
        BN_bn2binpad(ECDSA_SIG_get0_s(parsed.get()), ossl(raw.data()) + p256Bytes, p256Bytes);
        return raw;
    }

    [[nodiscard]] auto sign(const Json &claims, const std::string &headerAlg = "") const -> std::string
    {
        return signWith(claims, Json{{"alg", headerAlg.empty() ? alg : headerAlg}, {"typ", "JWT"}, {"kid", kid}});
    }

    [[nodiscard]] auto signWithoutKid(const Json & claims) const -> std::string
    {
        return signWith(claims, Json{{"alg", alg}, {"typ", "JWT"}});
    }

    [[nodiscard]] auto signWith(const Json & claims, const Json & header) const -> std::string
    {
        auto input = b64url(header.dump()) + "." + b64url(claims.dump());
        std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> const ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
        EVP_PKEY_CTX *pctx = nullptr;
        EVP_DigestSignInit_ex(ctx.get(), &pctx, kind == Kind::ed ? nullptr : "SHA256", nullptr, nullptr, pkey.get(),
                              nullptr);
        if (kind == Kind::pss)
        {
            EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_PSS_PADDING);
            EVP_PKEY_CTX_set_rsa_pss_saltlen(pctx, RSA_PSS_SALTLEN_DIGEST);
        }
        size_t len = 0;
        EVP_DigestSign(ctx.get(), nullptr, &len, ossl(input.data()), input.size());
        std::string sig(len, '\0');
        EVP_DigestSign(ctx.get(), ossl(sig.data()), &len, ossl(input.data()), input.size());
        sig.resize(len);
        if (kind == Kind::ec)
        {
            sig = derToJose(sig);
        }
        return input + "." + b64url(sig);
    }
};

class Checks
{
    int failures = 0;

  public:
    void operator()(bool cond, const char *what)
    {
        if (!cond)
        {
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

class BuildRouteService final : public nix::remote::NixRemote::Service
{
    nixgrpc::ReverseGateway & gateway;
    nixgrpc::Dispatcher & dispatcher;
    nixgrpc::ReverseRelay relay;

public:
    std::string drvPath;
    uint64_t assignId = 0;
    std::string selectedPod;
    bool relayEnabled = false;
    bool buildRelayEnabled = false;

    BuildRouteService(nixgrpc::ReverseGateway & gateway, nixgrpc::ReverseRegistry & registry,
                      nixgrpc::Dispatcher & dispatcher)
        : gateway(gateway), dispatcher(dispatcher), relay(gateway, registry) { }

    auto StoreInfo(grpc::ServerContext * context, const nix::remote::StoreInfoRequest * request,
                   nix::remote::StoreInfoReply * reply) -> grpc::Status override
    {
        if (relayEnabled) {
            return relay.call(*context, [&](uint64_t id) {
                return gateway.prepareBuild(*context,
                    [this](std::string_view drv, uint64_t assign) { return dispatcher.assignmentPod(drv, assign); },
                    id, drvPath, assignId, 0);
            }, *request, [&](std::string_view data) { return reply->ParseFromArray(data.data(), static_cast<int>(data.size())); });
        }
        auto result = gateway.prepareBuild(
            *context, [this](std::string_view drv, uint64_t id) { return dispatcher.assignmentPod(drv, id); },
            1, drvPath, assignId, 0);
        selectedPod = result.prepared ? result.prepared->session->workerId : std::string{};
        return result.status;
    }

    auto BuildDerivation(grpc::ServerContext * context, const nix::remote::BuildDerivationRequest * request,
                         grpc::ServerWriter<nix::remote::BuildDerivationChunk> * writer) -> grpc::Status override
    {
        if (!buildRelayEnabled) return {grpc::StatusCode::UNIMPLEMENTED, "test build relay disabled"};
        bool done = false;
        auto status = relay.call(*context, [&](uint64_t id) {
            return gateway.prepareBuild(*context,
                [this](std::string_view drv, uint64_t assign) { return dispatcher.assignmentPod(drv, assign); },
                id, request->drv_path(), request->assign_id(), request->build_mode());
        }, *request, [&](std::string_view data) {
            nix::remote::BuildDerivationChunk chunk;
            if (!chunk.ParseFromArray(data.data(), static_cast<int>(data.size())) || done ||
                chunk.event_case() == nix::remote::BuildDerivationChunk::EVENT_NOT_SET) return false;
            done = chunk.has_done();
            return writer->Write(chunk);
        }, [this, drv = request->drv_path(), assign = request->assign_id()] {
            return dispatcher.podOwnsAssignment(dispatcher.assignmentPod(drv, assign).value_or(""), drv, assign);
        });
        return status.ok() && !done ? grpc::Status{grpc::StatusCode::DATA_LOSS, "missing build result"} : status;
    }
};

auto now() -> int64_t
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

auto writeConfig(const std::string &dir, const std::string &base) -> void
{
    nix::writeFile(dir + "/oidc.json",
                   Json{{"allow_insecure", true},
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

auto run(const std::string &dir, const std::string &base) -> int
{
    Checks check;
    auto const ecKey = Key::generate(Kind::ec, "ec-1");
    auto const rsaKey = Key::generate(Kind::rsa, "rsa-1");
    auto const pssKey = Key::generate(Kind::pss, "pss-1");
    auto const edKey = Key::generate(Kind::ed, "ed-1");
    auto const stranger = Key::generate(Kind::ec, "ec-1"); // same kid, different key
    auto const ecKey2 = Key::generate(Kind::ec, "ec-2");

    // k8s style: issuer taken from the SA token, JWKS behind explicit URL.
    nix::writeFile(dir + "/sa-token", ecKey.sign({{"iss", "https://kubernetes.default.svc"}, {"sub", "x"}}));
    writeConfig(dir, base);

    // Issuer serves nothing yet: config loads, verifier constructs, tokens are refused.
    auto const cfg = loadConfig(dir + "/oidc.json");
    check(cfg.providers.size() == 2, "two providers");
    {
        auto twice = Json::parse(nix::readFile(dir + "/oidc.json"));
        auto & providers = twice.at("providers");
        providers.emplace("ci2", providers.at("ci"));
        nix::writeFile(dir + "/twice.json", twice.dump());
        bool refused = false;
        try {
            loadConfig(dir + "/twice.json");
        } catch (nix::Error &) {
            refused = true;
        }
        check(refused, "two providers with one issuer are refused");
    }
    Verifier verifier(cfg, std::chrono::seconds(0));

    auto const claims = [&](const Json &extra) -> Json
    {
        Json res{{"iss", base}, {"aud", "grpc://cache"}, {"exp", now() + validFor}, {"iat", now()}};
        res.update(extra);
        return res;
    };
    auto const good = claims({{"sub", "repo:myorg/a:b"}});

    {
        using Clock = std::chrono::steady_clock;
        using std::chrono::seconds;
        seconds const onMiss{30};
        auto const justBooted = Clock::time_point{} + seconds{5};
        auto const later = justBooted + onMiss + seconds{1};
        check(Verifier::fetchDue({}, {}, justBooted, false, onMiss), "first token right after boot fetches JWKS");
        check(!Verifier::fetchDue(justBooted, justBooted, justBooted + seconds{1}, false, onMiss), "no refetch storm on unknown kid");
        check(Verifier::fetchDue(justBooted, justBooted, later, false, onMiss), "unknown kid refetches later");
        check(!Verifier::fetchDue(justBooted, justBooted, justBooted + onMiss * 2, true, onMiss), "known kid uses cached keys");
        check(!Verifier::fetchDue({}, justBooted, justBooted + seconds{1}, false, onMiss), "failed fetch holds off the next");
        check(Verifier::fetchDue({}, justBooted, later, false, onMiss), "failed fetch is retried later");
    }
    {
        auto const res = verifier.verify(ecKey.sign(good));
        check(!res.identity && res.error.contains("no signing keys"), "issuer down -> refused, no crash");
    }
    nix::writeFile(dir + "/jwks.json", "garbage");
    nix::writeFile(dir + "/.well-known/openid-configuration", Json{{"jwks_uri", base + "/jwks.json"}}.dump());
    check(!verifier.verify(ecKey.sign(good)).identity, "garbage JWKS -> refused");
    nix::writeFile(
        dir + "/jwks.json",
        Json{{"keys",
              {"junk", Json{{"kty", "RSA"}, {"n", "!!"}}, ecKey.jwk(), ecKey2.jwk(), rsaKey.jwk(), pssKey.jwk(), edKey.jwk()}}}
            .dump());

    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/thing:ref:refs/heads/dev"}})));
        check(res.identity && res.identity->role == Role::write, "ES256 subject rule -> write");
        check(res.identity && res.identity->subject == "oidc:ci:repo:myorg/thing:ref:refs/heads/dev", "subject naming");
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
    check(verifier.verify(ecKey.signWithoutKid(good)).identity.has_value(), "no kid: every ES256 key is tried");
    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:other/x:y"}})));
        check(res.identity && !res.identity->role, "verified but no rule -> no role");
    }
    {
        auto const res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"aud", "someone-else"}})));
        check(!res.identity && res.error.contains("audience"), "wrong audience");
    }
    {
        auto res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"aud", {"x", "grpc://cache"}}})));
        check(res.identity && res.identity->role, "audience array");
    }
    {
        auto const res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"exp", now() - longAgo}})));
        check(!res.identity && res.error.contains("expired"), "expired");
        check(!verifier.verify(ecKey.sign({{"iss", base}, {"aud", "grpc://cache"}, {"sub", "s"}})).identity, "no exp");
        auto const future = claims({{"sub", "repo:myorg/a:b"}, {"nbf", now() + longAgo}});
        check(!verifier.verify(ecKey.sign(future)).identity, "not yet valid");
    }
    {
        auto const res = verifier.verify(ecKey.sign(claims({{"sub", "repo:myorg/a:b"}, {"iss", "https://elsewhere"}})));
        check(!res.identity && res.error.contains("issuer"), "unknown issuer");
    }
    {
        auto const res = verifier.verify(stranger.sign(good));
        check(!res.identity && res.error.contains("signature"), "foreign key with known kid");
    }
    {
        auto const res = verifier.verify(rsaKey.sign(good, "HS256"));
        check(!res.identity && res.error.contains("alg"), "HS256 refused");
        check(!verifier.verify(rsaKey.sign(good, "none")).identity, "alg none refused");
        // RSA key material presented as ES256 must not verify.
        check(!verifier.verify(rsaKey.sign(good, "ES256")).identity, "alg/key type confusion");
    }
    {
        auto token = ecKey.sign(good);
        constexpr size_t intoSignature = 10;
        auto &chr = token.at(token.size() - intoSignature);
        chr = chr == 'A' ? 'B' : 'A';
        check(!verifier.verify(token).identity, "tampered signature");
        check(!verifier.verify("not.a.jwt").identity, "garbage");
        check(!verifier.verify("").identity, "empty");
        check(!verifier.verify("a.b").identity, "two parts");
        check(!verifier.verify("a.b.c.d").identity, "four parts");
        constexpr size_t huge = 100000;
        check(!verifier.verify(std::string(huge, 'a')).identity, "oversized");
        check(!verifier.verify(ecKey.sign(claims({{"sub", 42}}))).identity,
              "non-string sub"); // NOLINT(*-magic-numbers)
    }
    {
        constexpr int64_t minute = 60;
        auto res =
            verifier.verify(ecKey.sign({{"iss", "https://kubernetes.default.svc"},
                                        {"aud", {"grpc://cache"}},
                                        {"exp", now() + minute},
                                        {"sub", "system:serviceaccount:builders:ci"},
                                        {"kubernetes.io", {{"namespace", "builders"}, {"pod", {{"name", "p"}}}}}}));
        check(res.identity && res.identity->role == Role::write,
              "k8s: issuer from SA token, nested claim, default write");
        check(res.identity && res.identity->subject == "oidc:k8s:system:serviceaccount:builders:ci", "k8s subject");
    }
    {
        // A separate audience/provider is mandatory: the regular client
        // audience is not an authorization to establish a worker tunnel.
        auto tunnelConfig = cfg;
        tunnelConfig.providers.erase(tunnelConfig.providers.begin());
        tunnelConfig.providers.front().name = "worker-tunnel";
        tunnelConfig.providers.front().audience = "grpc://worker-tunnel";
        tunnelConfig.providers.front().rules = {
            nixgrpc::oidc::Rule{.boundClaims = {{"kubernetes.io.namespace", {"staging"}},
                                                {"kubernetes.io.serviceaccount.name", {"worker"}}},
                                .role = Role::trusted}};
        Verifier tunnelVerifier(tunnelConfig);
        nixgrpc::WorkerIdentity binding(tunnelVerifier, "worker-tunnel", "staging", "worker", "grpc://worker-tunnel");
        constexpr std::string_view uid = "12345678-1234-1234-1234-123456789abc";
        auto const workerId = "pod:staging:" + std::string(uid);
        auto tokenClaims = [&](std::string_view podUid) -> Json
        {
            return {{"iss", "https://kubernetes.default.svc"},
                    {"sub", "system:serviceaccount:staging:worker"},
                    {"aud", "grpc://worker-tunnel"},
                    {"exp", now() + validFor},
                    {"iat", now()},
                    {"kubernetes.io",
                     {{"namespace", "staging"},
                      {"pod", {{"name", "worker-pod-1"}, {"uid", podUid}}},
                      {"serviceaccount", {{"name", "worker"}}}}}};
        };
        auto const token = ecKey.sign(tokenClaims(uid));
        auto bound = binding.bind(token, workerId);
        check(bound && bound->workerId == workerId && bound->expiresAt > std::chrono::system_clock::now(),
              "pod-bound worker identity and expiry");
        auto controlBound = binding.verify(token);
        check(controlBound && controlBound->workerId == workerId,
              "control-session identity derives pod UID from verified token alone");
        auto projectedAudience = tokenClaims(uid);
        projectedAudience["aud"] = {"grpc://worker-tunnel"};
        auto projected = binding.bind(ecKey.sign(projectedAudience), workerId);
        check(projected && projected->workerId == workerId, "Kubernetes audience array");
        check(!binding.bind(token, "pod:staging:other"), "claimed ID cannot override signed pod UID");
        check(!binding.bind(token, "pod:staging:12345678-1234-1234-1234-123456789abd"), "cannot claim another pod");
        auto replacement = tokenClaims("12345678-1234-1234-1234-123456789abd");
        replacement["kubernetes.io"]["pod"]["name"] = "worker-pod-2";
        auto const replacementId = "pod:staging:12345678-1234-1234-1234-123456789abd";
        auto rotated = binding.bind(ecKey.sign(replacement), replacementId);
        check(rotated && rotated->workerId == replacementId, "new pod gets its own identity");
        check(!binding.bind(token, replacementId), "old token cannot claim replacement pod");
        auto otherAudience = tokenClaims(uid);
        otherAudience["aud"] = "grpc://cache";
        check(!binding.bind(ecKey.sign(otherAudience), workerId), "client audience cannot register worker");
        auto mixedAudience = tokenClaims(uid);
        mixedAudience["aud"] = {"grpc://worker-tunnel", "grpc://cache"};
        check(!binding.bind(ecKey.sign(mixedAudience), workerId), "multi-audience token cannot register worker");
        check(!binding.verify(ecKey.sign(mixedAudience)), "multi-audience token cannot bind control session");
        auto wrongNamespace = tokenClaims(uid);
        wrongNamespace["kubernetes.io"]["namespace"] = "production";
        check(!binding.bind(ecKey.sign(wrongNamespace), workerId), "namespace mismatch denied");
        auto wrongSa = tokenClaims(uid);
        wrongSa["kubernetes.io"]["serviceaccount"]["name"] = "client";
        check(!binding.bind(ecKey.sign(wrongSa), workerId), "service account mismatch denied");
        auto noUid = tokenClaims(uid);
        noUid["kubernetes.io"]["pod"].erase("uid");
        check(!binding.bind(ecKey.sign(noUid), workerId), "legacy unbound service account token denied");
        check(!binding.bind(ecKey.sign(tokenClaims("bad-uid")), workerId), "malformed pod UID denied");
        auto noPod = tokenClaims(uid);
        noPod["kubernetes.io"].erase("pod");
        check(!binding.bind(ecKey.sign(noPod), workerId), "missing pod identity denied");
        auto noSa = tokenClaims(uid);
        noSa["kubernetes.io"].erase("serviceaccount");
        check(!binding.bind(ecKey.sign(noSa), workerId), "missing service-account claim denied");
        auto expired = tokenClaims(uid);
        expired["exp"] = now() - longAgo;
        check(!binding.bind(ecKey.sign(expired), workerId), "expired worker token denied");
        auto noIat = tokenClaims(uid);
        noIat.erase("iat");
        check(!binding.bind(ecKey.sign(noIat), workerId), "worker token without iat denied");
        auto longLived = tokenClaims(uid);
        longLived["exp"] = now() + 3600;
        check(!binding.bind(ecKey.sign(longLived), workerId), "hour-long worker token exceeds revocation window");
        auto backdated = tokenClaims(uid);
        backdated["iat"] = now() - 3000;
        backdated["exp"] = now() + 3000;
        check(!binding.bind(ecKey.sign(backdated), workerId), "back-dated iat cannot hide a long lifetime");
        auto futureIat = tokenClaims(uid);
        futureIat["iat"] = now() + 1200;
        futureIat["exp"] = now() + 1500;
        check(!binding.bind(ecKey.sign(futureIat), workerId), "future-dated iat cannot extend the window");
        {
            nixgrpc::WorkerIdentity hourly(tunnelVerifier, "worker-tunnel", "staging", "worker",
                                           "grpc://worker-tunnel", std::chrono::hours(1));
            check(hourly.bind(ecKey.sign(longLived), workerId).has_value(), "configured lifetime accepts hour token");
        }
        auto later = tokenClaims(uid);
        later["exp"] = now() + validFor + 60;
        auto renewed = binding.renew(ecKey.sign(later), workerId);
        check(renewed && bound && *renewed > bound->expiresAt, "rotated token for same pod renews");
        check(!binding.renew(ecKey.sign(tokenClaims("12345678-1234-1234-1234-123456789abd")), workerId),
              "rotated token for another pod cannot renew");
        check(!binding.renew(ecKey.sign(expired), workerId), "expired rotated token cannot renew");
        check(!binding.bind(stranger.sign(tokenClaims(uid)), workerId), "unknown signer denied");
        check(!binding.bind("", workerId), "empty token denied");

        // Real TLS gRPC registration (not a mocked registry call). No worker
        // data or scheduling is enabled on this endpoint.
        nixgrpc::ReverseRegistry registry;
        nixgrpc::Auth registrationAuth;
        registrationAuth.acl.addRule("localhost=trusted");
        nixgrpc::ReverseSessionService service(binding, registry, registrationAuth);
        grpc::SslServerCredentialsOptions tls;
        tls.pem_root_certs = nix::readFile(dir + "/server.crt");
        tls.client_certificate_request = GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;
        tls.pem_key_cert_pairs.push_back({nix::readFile(dir + "/server.key"), nix::readFile(dir + "/server.crt")});
        grpc::ServerBuilder builder;
        int port = 0;
        int insecurePort = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::SslServerCredentials(tls), &port);
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &insecurePort);
        builder.RegisterService(&service);
        auto server = builder.BuildAndStart();
        check(server && port > 0, "TLS reverse registration server started");
        if (!server)
        {
            return check.result();
        }
        grpc::SslCredentialsOptions roots;
        roots.pem_root_certs = nix::readFile(dir + "/server.crt");
        grpc::SslCredentialsOptions workerTls = roots;
        workerTls.pem_private_key = nix::readFile(dir + "/server.key");
        workerTls.pem_cert_chain = nix::readFile(dir + "/server.crt");
        auto channel = grpc::CreateChannel("localhost:" + std::to_string(port), grpc::SslCredentials(workerTls));
        auto stub = nix::remote::ReverseSession::NewStub(channel);
        {
            auto noCertChannel = grpc::CreateChannel("localhost:" + std::to_string(port), grpc::SslCredentials(roots));
            auto noCertStub = nix::remote::ReverseSession::NewStub(noCertChannel);
            grpc::ClientContext noCertCtx;
            noCertCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            noCertCtx.AddMetadata("authorization", "Bearer " + token);
            auto noCertStream = noCertStub->Register(&noCertCtx);
            nix::remote::ReverseWorkerMsg registration;
            registration.set_worker_id(workerId);
            registration.set_protocol(nixgrpc::reverseProtocolVersion);
            noCertStream->Write(registration);
            nix::remote::ReverseFarmMsg ack;
            check(!noCertStream->Read(&ack) && noCertStream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED,
                  "pod token without TLS client certificate cannot register data session");
        }
        std::string lastAckKey;
        auto open = [&](grpc::ClientContext &ctx, const std::string &credential, const std::string &claim)
        {
            if (!credential.empty())
            {
                ctx.AddMetadata("authorization", "Bearer " + credential);
            }
            auto stream = stub->Register(&ctx);
            nix::remote::ReverseWorkerMsg hello;
            hello.set_worker_id(claim);
            hello.set_protocol(nixgrpc::reverseProtocolVersion);
            stream->Write(hello);
            nix::remote::ReverseFarmMsg ack;
            bool accepted = stream->Read(&ack);
            lastAckKey = accepted ? ack.session_key() : std::string{};
            return std::pair{std::move(stream), accepted ? ack.generation() : uint64_t{0}};
        };
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            auto [stream, gen] = open(ctx, token, workerId);
            check(gen == 0 && stream->Finish().error_code() == grpc::StatusCode::UNAVAILABLE,
                  "standby rejects registration");
        }
        registry.setActive(true);
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = stub->Register(&ctx);
            nix::remote::ReverseWorkerMsg hello;
            hello.set_worker_id(workerId);
            hello.set_stream_id(1);
            hello.mutable_data()->set_data("not a registration");
            stream->Write(hello);
            nix::remote::ReverseFarmMsg ack;
            check(!stream->Read(&ack) && stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT &&
                      !registry.find(workerId),
                  "first frame cannot smuggle data into registration");
        }
        {
            auto plaintext = nix::remote::ReverseSession::NewStub(
                grpc::CreateChannel("127.0.0.1:" + std::to_string(insecurePort), grpc::InsecureChannelCredentials()));
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = plaintext->Register(&ctx);
            nix::remote::ReverseWorkerMsg hello;
            hello.set_worker_id(workerId);
            hello.set_protocol(nixgrpc::reverseProtocolVersion);
            stream->Write(hello);
            nix::remote::ReverseFarmMsg ack;
            check(!stream->Read(&ack) && stream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED,
                  "plain HTTP/2 cannot register even with valid token");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            auto [stream, gen] = open(ctx, {}, workerId);
            check(gen == 0 && stream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED,
                  "missing bearer denied over TLS");
        }
        for (uint32_t const version : {0U, nixgrpc::reverseProtocolVersion + 1}) {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = stub->Register(&ctx);
            nix::remote::ReverseWorkerMsg hello;
            hello.set_worker_id(workerId);
            hello.set_protocol(version);
            stream->Write(hello);
            nix::remote::ReverseFarmMsg ack;
            auto const status = stream->Read(&ack) ? grpc::Status::OK : stream->Finish();
            check(status.error_code() == grpc::StatusCode::FAILED_PRECONDITION &&
                      status.error_message().contains("roll out scheduler and reverse workers together") &&
                      !registry.find(workerId),
                  "data registration with another reverse protocol is refused explicitly");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = stub->Register(&ctx);
            nix::remote::ReverseWorkerMsg hello;
            hello.set_worker_id(workerId);
            hello.set_protocol(nixgrpc::reverseProtocolVersion);
            stream->Write(hello);
            nix::remote::ReverseFarmMsg ack;
            check(stream->Read(&ack) && ack.protocol() == nixgrpc::reverseProtocolVersion,
                  "registration ack announces the scheduler's reverse protocol");
            ctx.TryCancel();
            stream->Finish();
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            auto [stream, gen] = open(ctx, token, replacementId);
            check(gen == 0 && stream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED,
                  "wrong pod cannot register over TLS");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = stub->Register(&ctx);
            nix::remote::ReverseWorkerMsg hello;
            hello.set_worker_id(workerId);
            hello.set_protocol(nixgrpc::reverseProtocolVersion);
            stream->Write(hello);
            nix::remote::ReverseFarmMsg ack;
            check(!stream->Read(&ack) && stream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED,
                  "ambiguous bearer metadata denied");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            auto [stream, gen] = open(ctx, token, workerId);
            check(gen != 0, "registration before invalid second message");
            nix::remote::ReverseWorkerMsg extra;
            extra.set_worker_id(workerId);
            stream->Write(extra);
            stream->WritesDone();
            check(stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT && !registry.find(workerId),
                   "unreserved data frames rejected and session removed");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            auto [stream, gen] = open(ctx, token, workerId);
            check(gen != 0, "registration before unknown stream response");
            nix::remote::ReverseWorkerMsg extra;
            extra.set_stream_id(99);
            extra.mutable_data()->set_data("unsolicited result");
            stream->Write(extra);
            stream->WritesDone();
            check(stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT && !registry.find(workerId),
                  "unreserved worker response closes live TLS session");
        }
        {
            // Real bidirectional TLS transport, but not a client-facing RPC:
            // the test explicitly reserves the stream as a gateway would.
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
            auto [stream, gen] = open(ctx, token, workerId);
            auto session = registry.find(workerId);
            check(gen != 0 && session && session->transport, "live reverse transport attached to registration");
            if (session && session->transport) {
                auto & transport = *session->transport;
                auto capability = nixgrpc::ReverseCapability::mint(
                    session->sessionKey, workerId, gen, 1, "StoreInfo",
                    nixgrpc::Caller{.name = "read-client", .role = Role::readOnly,
                                    .kind = nixgrpc::Caller::Kind::named});
                nix::remote::ReverseOpen start;
                start.set_method("StoreInfo");
                if (capability) start.set_capability(*capability);
                check(capability && transport.responses.open(1) && transport.outgoing.open(1, std::move(start)) &&
                          transport.outgoing.data(1, "request") && transport.outgoing.halfClose(1),
                      "gateway queues one scoped request");
                nix::remote::ReverseFarmMsg frame;
                check(stream->Read(&frame) && frame.stream_id() == 1 && frame.has_open(), "worker receives open");
                check(stream->Read(&frame) && frame.stream_id() == 1 && frame.data().data() == "request",
                      "worker receives bounded request bytes");
                check(stream->Read(&frame) && frame.stream_id() == 1 && frame.half_close(),
                      "worker receives request half-close");
                auto secondCapability = nixgrpc::ReverseCapability::mint(
                    session->sessionKey, workerId, gen, 2, "StoreInfo",
                    nixgrpc::Caller{.name = "other-client", .role = Role::readOnly,
                                    .kind = nixgrpc::Caller::Kind::named});
                nix::remote::ReverseOpen secondStart;
                secondStart.set_method("StoreInfo");
                if (secondCapability) secondStart.set_capability(*secondCapability);
                check(secondCapability && transport.responses.open(2) &&
                          transport.outgoing.open(2, std::move(secondStart)) && transport.outgoing.halfClose(2),
                      "second client reserves independent stream");
                check(stream->Read(&frame) && frame.stream_id() == 2 && frame.has_open(),
                      "worker receives second open");
                check(stream->Read(&frame) && frame.stream_id() == 2 && frame.half_close(),
                      "worker receives second half-close");
                nix::remote::ReverseWorkerMsg reply;
                reply.set_stream_id(1);
                reply.mutable_data()->set_data("response");
                check(stream->Write(reply), "worker sends data");
                reply.set_stream_id(2);
                reply.mutable_data()->set_data("other response");
                check(stream->Write(reply), "worker interleaves second client data");
                reply.mutable_finish()->set_code(0);
                check(stream->Write(reply), "worker finishes second client first");
                reply.set_stream_id(1);
                reply.mutable_finish()->set_code(0);
                check(stream->Write(reply), "worker sends final status");
                nix::remote::ReverseWorkerMsg result;
                check(transport.responses.waitTake(2, result, {}) && result.stream_id() == 2 &&
                          result.data().data() == "other response", "second client cannot take first response");
                check(transport.responses.waitTake(2, result, {}) && result.stream_id() == 2 && result.has_finish(),
                      "second client receives its own final status");
                check(transport.responses.waitTake(1, result, {}) && result.data().data() == "response",
                      "gateway receives exact stream data");
                check(transport.responses.waitTake(1, result, {}) && result.has_finish() && result.finish().code() == 0,
                      "gateway receives exact stream final status");
                check(transport.responses.finished(1) && transport.outgoing.finished(1) &&
                          transport.responses.finished(2) && transport.outgoing.finished(2),
                      "both completed streams released");
            }
            stream->WritesDone();
            check(stream->Finish().ok() && !registry.find(workerId), "transport closes on normal EOF");
        }
        // Finish and destroy client streams/contexts before server shutdown;
        // gRPC's Darwin teardown otherwise intermittently crashes at exit.
        {
            grpc::ClientContext firstCtx;
            firstCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            auto [firstStream, firstGen] = open(firstCtx, token, workerId);
            check(firstGen != 0 && registry.find(workerId) && registry.find(workerId)->generation == firstGen,
                  "registered exact worker");
            auto firstKey = registry.find(workerId)->sessionKey;
            check(firstKey.size() == nixgrpc::ReverseCapability::keyBytes && lastAckKey == firstKey,
                  "TLS registration ack delivers exactly the per-registration capability key");
            auto firstCapability = nixgrpc::ReverseCapability::mint(
                firstKey, workerId, firstGen, 1, "StoreInfo",
                nixgrpc::Caller{.name = "read-client", .role = Role::readOnly, .kind = nixgrpc::Caller::Kind::named});
            check(firstCapability.has_value(), "leader can mint scoped capability for registered worker");
            grpc::ClientContext secondCtx;
            secondCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            auto [secondStream, secondGen] = open(secondCtx, token, workerId);
            check(secondGen > firstGen && registry.find(workerId) && registry.find(workerId)->generation == secondGen,
                  "reconnect displaces old session");
            check(registry.find(workerId)->sessionKey.size() == nixgrpc::ReverseCapability::keyBytes &&
                      registry.find(workerId)->sessionKey != firstKey &&
                      lastAckKey == registry.find(workerId)->sessionKey,
                  "replacement rotates session capability key");
            check(firstCapability &&
                      !nixgrpc::ReverseCapability::verify(registry.find(workerId)->sessionKey, *firstCapability,
                                                          workerId, secondGen, 1, "StoreInfo"),
                  "reconnect cannot reuse old caller capability");
            grpc::ClientContext otherCtx;
            otherCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            auto [otherStream, otherGen] = open(otherCtx, ecKey.sign(replacement), replacementId);
            check(otherGen != 0 && registry.find(replacementId) && registry.find(workerId),
                  "different pod registers independently");
            firstStream->Finish();
            check(registry.find(workerId) && registry.find(workerId)->generation == secondGen,
                  "late close cannot evict replacement");
            registry.setActive(false);
            check(!registry.find(workerId) && !registry.find(replacementId),
                  "leadership loss removes every registered session");
            secondStream->Finish();
            otherStream->Finish();
            registry.setActive(true);
            {
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                auto soon = tokenClaims(uid);
                soon["exp"] = now() + 2;
                auto [stream, gen] = open(ctx, ecKey.sign(soon), workerId);
                check(gen != 0, "short-lived token registers");
                nix::remote::ReverseFarmMsg ack;
                check(!stream->Read(&ack), "registered stream closes at token expiry");
                check(!registry.find(workerId), "expired credential evicts worker");
                stream->Finish();
            }
            {
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
                auto soon = tokenClaims(uid);
                soon["exp"] = now() + 2;
                auto [stream, gen] = open(ctx, ecKey.sign(soon), workerId);
                nix::remote::ReverseWorkerMsg rotated;
                rotated.set_credential(ecKey.sign(tokenClaims(uid)));
                check(gen != 0 && stream->Write(rotated), "rotated token sent on data session");
                std::this_thread::sleep_for(std::chrono::seconds(3));
                auto const live = registry.find(workerId);
                check(live && live->generation == gen, "rotated token keeps data session past first expiry");
                nix::remote::ReverseWorkerMsg foreign;
                foreign.set_credential(ecKey.sign(tokenClaims("12345678-1234-1234-1234-123456789abd")));
                stream->Write(foreign);
                nix::remote::ReverseFarmMsg ack;
                check(!stream->Read(&ack) && stream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED &&
                          !registry.find(workerId),
                      "another pod's rotated token closes the data session");
            }
            {
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                auto [stream, gen] = open(ctx, token, workerId);
                nix::remote::ReverseWorkerMsg smuggled;
                smuggled.set_stream_id(1);
                smuggled.set_credential(token);
                stream->Write(smuggled);
                nix::remote::ReverseFarmMsg ack;
                check(gen != 0 && !stream->Read(&ack) &&
                          stream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED,
                      "credential cannot ride on a stream frame");
            }
        }
        stub.reset();
        channel.reset();
        server->Shutdown();
        server->Wait();

        // Exercise the production callback SchedulerService over real TLS.
        // The same pod JWT is verified independently on the control and data
        // streams; a Hello claim or a valid token without a client certificate
        // alone must never establish a reverse route.
        nixgrpc::Metrics metrics("");
        nixgrpc::Dispatcher dispatcher({.defaultSystem = "x"}, metrics);
        nixgrpc::Auth controlAuth;
        std::optional<Verifier> noClientOidc;
        controlAuth.oidc = &noClientOidc;
        controlAuth.acl.addRule("localhost=trusted");
        nixgrpc::ReverseRegistry controlRegistry;
        controlRegistry.setActive(true);
        nixgrpc::ReverseGateway gateway(
            controlAuth, controlRegistry,
            [&dispatcher](std::string_view pod, std::string_view drv, uint64_t id) {
                return dispatcher.podOwnsAssignment(pod, drv, id);
            });
        nixgrpc::IdleTracker idle;
        nixgrpc::SchedulerService control(dispatcher, controlAuth, idle);
        control.setReverseIdentity(binding);
        nixgrpc::ReverseSessionService dataControl(binding, controlRegistry, controlAuth);
        BuildRouteService buildRoute(gateway, controlRegistry, dispatcher);
        grpc::SslServerCredentialsOptions controlTls;
        controlTls.pem_root_certs = nix::readFile(dir + "/server.crt");
        controlTls.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
        controlTls.pem_key_cert_pairs.push_back({nix::readFile(dir + "/server.key"),
                                                 nix::readFile(dir + "/server.crt")});
        grpc::ServerBuilder controlBuilder;
        int controlPort = 0;
        controlBuilder.AddListeningPort("127.0.0.1:0", grpc::SslServerCredentials(controlTls), &controlPort);
        controlBuilder.RegisterService(&control);
        controlBuilder.RegisterService(&dataControl);
        controlBuilder.RegisterService(&buildRoute);
        auto controlServer = controlBuilder.BuildAndStart();
        check(controlServer && controlPort > 0, "production scheduler TLS control server started");
        if (!controlServer) return check.result();
        grpc::SslCredentialsOptions clientTls;
        clientTls.pem_root_certs = nix::readFile(dir + "/server.crt");
        clientTls.pem_private_key = nix::readFile(dir + "/server.key");
        clientTls.pem_cert_chain = nix::readFile(dir + "/server.crt");
        auto controlChannel = grpc::CreateChannel("localhost:" + std::to_string(controlPort), grpc::SslCredentials(clientTls));
        auto controlStub = nix::remote::Scheduler::NewStub(controlChannel);
        auto dataStub = nix::remote::ReverseSession::NewStub(controlChannel);
        auto routeStub = nix::remote::NixRemote::NewStub(controlChannel);
        auto noCertChannel = grpc::CreateChannel("localhost:" + std::to_string(controlPort), grpc::SslCredentials(roots));
        auto noCertStub = nix::remote::Scheduler::NewStub(noCertChannel);
        auto sendHello = [&](grpc::ClientContext &ctx, std::string_view claim, std::string_view addr) {
            auto stream = controlStub->WorkerSession(&ctx);
            nix::remote::WorkerMsgs messages;
            auto *hello = messages.add_msgs()->mutable_hello();
            hello->set_addr(std::string(addr));
            hello->add_systems("x");
            hello->set_max_jobs(1);
            hello->set_reverse_worker_id(std::string(claim));
            hello->set_reverse_protocol(nixgrpc::reverseProtocolVersion);
            stream->Write(messages);
            return stream;
        };
        constexpr std::string_view controlAddr = "127.0.0.1:50051";
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = noCertStub->WorkerSession(&ctx);
            nix::remote::WorkerMsgs messages;
            auto *hello = messages.add_msgs()->mutable_hello();
            hello->set_addr(std::string(controlAddr));
            hello->add_systems("x");
            hello->set_reverse_worker_id(workerId);
            hello->set_reverse_protocol(nixgrpc::reverseProtocolVersion);
            stream->Write(messages);
            nix::remote::SchedCmds commands;
            check(!stream->Read(&commands) && !stream->Finish().ok() && dispatcher.membership()->empty(),
                  "pod token without authorized TLS certificate cannot create a control route");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = sendHello(ctx, replacementId, controlAddr);
            nix::remote::SchedCmds commands;
            check(!stream->Read(&commands) && stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT,
                  "production control stream rejects claimed UID different from signed pod UID");
            check(dispatcher.membership()->empty(), "rejected control stream adds no worker");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            auto stream = sendHello(ctx, workerId, controlAddr);
            nix::remote::SchedCmds commands;
            check(!stream->Read(&commands) && stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT,
                  "certificate-only control stream cannot claim a reverse pod");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + ecKey.sign(otherAudience));
            auto stream = sendHello(ctx, workerId, controlAddr);
            nix::remote::SchedCmds commands;
            check(!stream->Read(&commands) && stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT,
                  "wrong audience cannot bind control pod");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = sendHello(ctx, workerId, controlAddr);
            nix::remote::SchedCmds commands;
            check(!stream->Read(&commands) && stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT,
                  "duplicate control bearer cannot bind a reverse pod");
        }
        {
            // Existing direct workers remain schedulable; no pod route can
            // result from their TLS certificate alone.
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            constexpr std::string_view directAddr = "127.0.0.2:50051";
            auto stream = sendHello(ctx, {}, directAddr);
            auto client = dispatcher.connectClient([](const nix::remote::SchedMsg &) { return true; }, [] {});
            nix::remote::ClientMsgs want;
            auto * request = want.add_msgs()->mutable_want();
            request->set_drv_path("/nix/store/test-direct-route.drv");
            request->set_system("x");
            dispatcher.clientMsgs(client, want);
            nix::remote::SchedCmds commands;
            check(stream->Read(&commands) && commands.msgs_size() == 1 && commands.msgs(0).has_expect(),
                  "direct worker still receives a scheduler Expect");
            if (commands.msgs_size() == 1 && commands.msgs(0).has_expect()) {
                auto const id = commands.msgs(0).expect().assign_id();
                check(dispatcher.assignmentWorker(request->drv_path(), id) == directAddr &&
                          !dispatcher.assignmentPod(request->drv_path(), id),
                      "direct worker assignment has no reverse pod route");
            }
            dispatcher.clientGone(client);
            stream->WritesDone();
            while (stream->Read(&commands)) { }
            check(stream->Finish().ok(), "direct TLS control stream ends normally");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = controlStub->WorkerSession(&ctx);
            nix::remote::WorkerMsgs messages;
            auto *hello = messages.add_msgs()->mutable_hello();
            hello->set_addr("127.0.0.6:50051");
            hello->add_systems("x");
            hello->set_reverse_worker_id(workerId);
            stream->Write(messages);
            nix::remote::SchedCmds commands;
            auto const status = stream->Read(&commands) ? grpc::Status::OK : stream->Finish();
            check(status.error_code() == grpc::StatusCode::FAILED_PRECONDITION &&
                      status.error_message().contains("roll out scheduler and reverse workers together") &&
                      dispatcher.membership()->empty(),
                  "control Hello without reverse protocol is refused explicitly");
        }
        auto reauth = [](std::string tokenValue) {
            nix::remote::WorkerMsgs messages;
            messages.add_msgs()->mutable_reauth()->set_token(std::move(tokenValue));
            return messages;
        };
        // Pod-bound workers are never EDS endpoints; count them directly.
        auto memberCount = [&] { return dispatcher.podConnected(workerId) ? size_t{1} : size_t{0}; };
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
            auto stream = sendHello(ctx, {}, "127.0.0.3:50051");
            stream->Write(reauth(token));
            nix::remote::SchedCmds commands;
            check(!stream->Read(&commands) && stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT,
                  "certificate-only control stream cannot send a tunnel credential");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
            auto soon = tokenClaims(uid);
            soon["exp"] = now() + 2;
            ctx.AddMetadata("authorization", "Bearer " + ecKey.sign(soon));
            auto stream = sendHello(ctx, workerId, "127.0.0.4:50051");
            check(stream->Write(reauth(ecKey.sign(tokenClaims(uid)))), "rotated token sent on control session");
            std::this_thread::sleep_for(std::chrono::seconds(3));
            dispatcher.expireCredentials();
            check(memberCount() == 1, "rotated token keeps pod-bound worker schedulable past first expiry");
            stream->Write(reauth(ecKey.sign(tokenClaims("12345678-1234-1234-1234-123456789abd"))));
            nix::remote::SchedCmds commands;
            while (stream->Read(&commands)) { }
            check(stream->Finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT && memberCount() == 0,
                  "another pod's rotated token closes the control session");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
            auto soon = tokenClaims(uid);
            soon["exp"] = now() + 2;
            ctx.AddMetadata("authorization", "Bearer " + ecKey.sign(soon));
            auto stream = sendHello(ctx, workerId, "127.0.0.5:50051");
            auto const joined = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (memberCount() == 0 && std::chrono::steady_clock::now() < joined) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            check(memberCount() == 1, "short-lived control session joins");
            std::this_thread::sleep_for(std::chrono::seconds(3));
            dispatcher.expireCredentials();
            nix::remote::SchedCmds commands;
            while (stream->Read(&commands)) { }
            check(memberCount() == 0 && !stream->Finish().ok(),
                  "expired control credential removes worker and cancels its stream");
        }
        {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            ctx.AddMetadata("authorization", "Bearer " + token);
            auto stream = sendHello(ctx, workerId, controlAddr);
            grpc::ClientContext dataCtx;
            dataCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            dataCtx.AddMetadata("authorization", "Bearer " + token);
            auto dataStream = dataStub->Register(&dataCtx);
            nix::remote::ReverseWorkerMsg registration;
            registration.set_worker_id(workerId);
            registration.set_protocol(nixgrpc::reverseProtocolVersion);
            check(dataStream->Write(registration), "same pod opens independent reverse data registration");
            nix::remote::ReverseFarmMsg ack;
            check(dataStream->Read(&ack) && ack.generation() != 0 && controlRegistry.find(workerId),
                  "same signed pod registers a live data session beside control");
            // The active dispatcher must derive the pod route only from this
            // authenticated control stream's live Expect, not from Hello alone.
            // Wait for Hello processing via an ordinary client Want, which
            // yields Expect only after the worker joins the scheduler.
            std::string selected;
            auto client = dispatcher.connectClient([&](const nix::remote::SchedMsg &msg) {
                if (msg.has_assigned()) {
                    selected = msg.assigned().worker_addr();
                    check(msg.assigned().reverse(), "pod-bound assignment selects scheduler reverse route");
                }
                return true;
            }, [] {});
            nix::remote::ClientMsgs want;
            auto * request = want.add_msgs()->mutable_want();
            request->set_drv_path("/nix/store/test-control-route.drv");
            request->set_system("x");
            nix::remote::SchedCmds commands;
            // Worker Hello can be processed after this Want; the dispatcher
            // queues it and assigns when the stream joins.
            dispatcher.clientMsgs(client, want);
            check(stream->Read(&commands) && commands.msgs_size() == 1 && commands.msgs(0).has_expect(),
                  "verified control stream receives a real Expect");
            if (commands.msgs_size() == 1 && commands.msgs(0).has_expect()) {
                auto const id = commands.msgs(0).expect().assign_id();
                auto const assignedPod = dispatcher.assignmentPod(request->drv_path(), id);
                check(selected == controlAddr && assignedPod == workerId &&
                          dispatcher.podOwnsAssignment(workerId, request->drv_path(), id),
                      "production TLS control stream binds live assignment to signed pod UID");
                check(controlRegistry.current(controlRegistry.find(workerId)) &&
                          dispatcher.assignmentPod(request->drv_path(), id) == controlRegistry.find(workerId)->workerId,
                      "live assignment and data registration resolve to the same verified pod");
                buildRoute.drvPath = request->drv_path();
                buildRoute.assignId = id;
                grpc::ClientContext routeCtx;
                routeCtx.AddMetadata("x-nix-worker", replacementId);
                nix::remote::StoreInfoRequest routeReq;
                nix::remote::StoreInfoReply routeReply;
                check(routeStub->StoreInfo(&routeCtx, routeReq, &routeReply).ok() && buildRoute.selectedPod == workerId,
                      "authenticated build route ignores forged client worker metadata");
                buildRoute.relayEnabled = true;
                std::jthread responder([&] {
                    nix::remote::ReverseFarmMsg requestFrame;
                    check(dataStream->Read(&requestFrame) && requestFrame.has_open() &&
                              requestFrame.open().method() == "BuildDerivation" && requestFrame.stream_id() != 0,
                          "client RPC opens scoped frame on selected TLS registration");
                    auto const streamId = requestFrame.stream_id();
                    auto selected = controlRegistry.find(workerId);
                    if (!selected) return;
                    nixgrpc::ReverseRequest incoming(workerId, selected->generation, selected->sessionKey);
                    check(incoming.accept(requestFrame), "worker validates scoped open on TLS registration");
                    while (dataStream->Read(&requestFrame) && requestFrame.stream_id() == streamId) {
                        check(incoming.accept(requestFrame), "worker accepts bounded reverse request frame");
                        if (requestFrame.half_close()) break;
                    }
                    auto ready = incoming.takeReady();
                    nix::remote::StoreInfoRequest parsedRequest;
                    check(ready && ready->delegated.drvPath == buildRoute.drvPath &&
                              ready->delegated.assignId == buildRoute.assignId &&
                              parsedRequest.ParseFromString(ready->payload),
                          "worker decodes complete request with signed build assignment");
                    nix::remote::StoreInfoReply response;
                    response.set_trusted(true);
                    auto const payload = response.SerializeAsString();
                    nix::remote::ReverseWorkerMsg answer;
                    answer.set_stream_id(streamId);
                    std::string encoded;
                    for (unsigned shift = 0; shift != 32; shift += 8) encoded.push_back(static_cast<char>((payload.size() >> shift) & 0xff));
                    encoded += payload;
                    answer.mutable_data()->set_data(encoded);
                    check(dataStream->Write(answer), "worker sends encoded reverse response");
                    answer.mutable_finish()->set_code(0);
                    check(dataStream->Write(answer), "worker sends reverse completion");
                    check(incoming.finished(streamId), "worker releases completed reverse stream");
                });
                grpc::ClientContext relayCtx;
                relayCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                relayCtx.AddMetadata("x-nix-worker", replacementId);
                check(routeStub->StoreInfo(&relayCtx, routeReq, &routeReply).ok() && routeReply.trusted(),
                      "authenticated client RPC receives reverse response despite forged worker metadata");
                responder.join();
                buildRoute.relayEnabled = false;
                buildRoute.buildRelayEnabled = true;
                std::jthread buildResponder([&] {
                    nix::remote::ReverseFarmMsg frame;
                    if (!dataStream->Read(&frame)) return;
                    auto selected = controlRegistry.find(workerId);
                    if (!selected) return;
                    nixgrpc::ReverseRequest incoming(workerId, selected->generation, selected->sessionKey);
                    check(frame.has_open() && incoming.accept(frame), "build RPC opens verified worker stream");
                    auto const streamId = frame.stream_id();
                    while (dataStream->Read(&frame) && frame.stream_id() == streamId) {
                        check(incoming.accept(frame), "worker accepts bounded build request");
                        if (frame.half_close()) break;
                    }
                    auto ready = incoming.takeReady();
                    nix::remote::BuildDerivationRequest parsed;
                    check(ready && parsed.ParseFromString(ready->payload) &&
                              nixgrpc::ReverseRequest::matchesBuild(*ready, parsed) &&
                              dispatcher.podOwnsAssignment(workerId, parsed.drv_path(), parsed.assign_id()),
                          "worker validates decoded derivation, mode, assignment and live pod ownership");
                    nix::remote::BuildDerivationChunk result;
                    result.mutable_done()->set_result("simulated-result");
                    auto const payload = result.SerializeAsString();
                    std::string encoded;
                    for (unsigned shift = 0; shift != 32; shift += 8) encoded.push_back(static_cast<char>((payload.size() >> shift) & 0xff));
                    encoded += payload;
                    nix::remote::ReverseWorkerMsg answer;
                    answer.set_stream_id(streamId);
                    answer.mutable_data()->set_data(encoded);
                    check(dataStream->Write(answer), "worker sends simulated build result");
                    answer.mutable_finish()->set_code(0);
                    check(dataStream->Write(answer), "worker finishes simulated build result");
                    check(incoming.finished(streamId), "worker releases completed build stream");
                });
                grpc::ClientContext buildCtx;
                buildCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                buildCtx.AddMetadata("x-nix-worker", replacementId);
                nix::remote::BuildDerivationRequest buildRequest;
                buildRequest.set_drv_path(buildRoute.drvPath);
                buildRequest.set_assign_id(buildRoute.assignId);
                auto buildStream = routeStub->BuildDerivation(&buildCtx, buildRequest);
                nix::remote::BuildDerivationChunk buildChunk;
                check(buildStream->Read(&buildChunk) && buildChunk.has_done() &&
                          buildChunk.done().result() == "simulated-result" && buildStream->Finish().ok(),
                      "authenticated build RPC streams response from exact signed pod despite forged header");
                buildResponder.join();
                buildRoute.buildRelayEnabled = false;
                check(!dispatcher.podOwnsAssignment(replacementId, request->drv_path(), id),
                      "another signed pod cannot own this assignment");
                // The data registration may be replaced while the control
                // assignment remains live. The old handle must fail closed.
                auto firstData = controlRegistry.find(workerId);
                grpc::ClientContext replacementCtx;
                replacementCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
                replacementCtx.AddMetadata("authorization", "Bearer " + token);
                auto replacementStream = dataStub->Register(&replacementCtx);
                check(replacementStream->Write(registration), "same pod reconnects data registration");
                nix::remote::ReverseFarmMsg replacementAck;
                check(replacementStream->Read(&replacementAck) && replacementAck.generation() > ack.generation() &&
                          !controlRegistry.current(firstData) && controlRegistry.current(controlRegistry.find(workerId)) &&
                          dispatcher.podOwnsAssignment(workerId, request->drv_path(), id),
                      "replacing data generation fences old handle without stealing control assignment");
                dataStream->Finish();
                replacementStream->WritesDone();
                check(replacementStream->Finish().ok() && !controlRegistry.find(workerId),
                      "last data registration closes without a fallback route");
                check(dispatcher.podOwnsAssignment(workerId, request->drv_path(), id),
                      "control assignment alone remains live after data disconnect");
                grpc::ClientContext missingData;
                check(routeStub->StoreInfo(&missingData, routeReq, &routeReply).error_code() ==
                          grpc::StatusCode::UNAVAILABLE && buildRoute.selectedPod.empty(),
                      "client-facing route fails shut after exact data registration closes");
            }
            dispatcher.clientGone(client);
            grpc::ClientContext cancelledBuild;
            nix::remote::StoreInfoRequest cancelledReq;
            nix::remote::StoreInfoReply cancelledReply;
            check(routeStub->StoreInfo(&cancelledBuild, cancelledReq, &cancelledReply).error_code() ==
                      grpc::StatusCode::FAILED_PRECONDITION,
                  "client-facing route fails shut after scheduler cancels assignment");
            if (controlRegistry.find(workerId)) {
                dataStream->WritesDone();
                check(dataStream->Finish().ok() && !controlRegistry.find(workerId),
                      "independent data stream closes without displacing live control assignment");
            }
            stream->WritesDone();
            nix::remote::SchedCmds remaining;
            while (stream->Read(&remaining)) { }
            auto status = stream->Finish();
            if (!status.ok()) std::cerr << "control Finish: " << status.error_message() << "\n";
            check(status.ok(), "verified TLS control stream ends normally");
        }
        controlStub.reset();
        dataStub.reset();
        routeStub.reset();
        controlChannel.reset();
        noCertStub.reset();
        noCertChannel.reset();
        controlServer->Shutdown();
        controlServer->Wait();
    }
    return check.result();
}

} // namespace
// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)

auto main(int argc, char **argv) -> int
{
    try
    {
        if (argc != 3)
        {
            std::cerr << "usage: oidc-test JWKS-DIR BASE-URL\n";
            return 2;
        }
        std::vector<std::string> const args(argv,
                                            argv + argc); // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        // Prevent OpenSSL atexit teardown racing with gRPC event-engine threads.
        if (OPENSSL_init_crypto(OPENSSL_INIT_NO_ATEXIT, nullptr) != 1) {
            return 1;
        }
        grpc_init();
        auto result = run(args.at(1), args.at(2));
        grpc_shutdown_blocking();
        return result;
    }
    catch (std::exception &err)
    {
        std::cerr << "error: " << err.what() << "\n";
        return 1;
    }
}
