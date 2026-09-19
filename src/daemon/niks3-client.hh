#pragma once
// niks3 connection settings and how this node authenticates there.

#include <curl/curl.h>
#include <memory>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include "http.hh"
#include "push.hh"
#include "token-file.hh"

namespace nixgrpc {

struct Niks3Config
{
    std::string url;
    std::string tokenFile;  // bearer, optional
    std::string clientCert; // mTLS, optional
    std::string clientKey;
    PushProcess::Argv pushArgv;

    [[nodiscard]] auto enabled() const -> bool
    {
        return !url.empty();
    }
};

// How this node authenticates to niks3: bearer, client cert, both or neither.
class Niks3Client
{
    std::string baseUrl;
    std::shared_ptr<TokenFile> bearer;
    std::string clientCert;
    std::string clientKey;

public:
    explicit Niks3Client(const Niks3Config & cfg)
        : baseUrl(cfg.url)
        , bearer(cfg.tokenFile.empty() ? nullptr : std::make_shared<TokenFile>(cfg.tokenFile))
        , clientCert(cfg.clientCert)
        , clientKey(cfg.clientKey)
    {
        while (baseUrl.ends_with('/')) {
            baseUrl.pop_back();
        }
    }

    // POST `body` to `path` with whatever credentials are configured.
    // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg): curl_easy_setopt
    [[nodiscard]] auto post(std::string_view path, const nlohmann::json & body) const -> std::unique_ptr<http::Call>
    {
        auto call = std::make_unique<http::Call>(baseUrl + std::string(path), bearer ? bearer->get() : "", body);
        if (!clientCert.empty()) {
            call->opt(CURLOPT_SSLCERT, clientCert.c_str());
            call->opt(CURLOPT_SSLKEY, clientKey.c_str());
        }
        return call;
    }
    // NOLINTEND(cppcoreguidelines-pro-type-vararg)
};

} // namespace nixgrpc
