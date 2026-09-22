#pragma once
// Blocking libcurl wrapper for the niks3 and OIDC JSON APIs.

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <nix/util/error.hh>
#include <nix/util/sync.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>
#include <nix/util/processes.hh>

#include "logfmt.hh"

namespace nixgrpc::http {

constexpr long conflict = 409;

inline auto ok(long status) -> bool
{
    constexpr long first = 200;
    constexpr long last = 299;
    return status >= first && status <= last;
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-vararg): curl_easy_* are vararg.
// Callbacks run inside C frames and must not throw.
class Call
{
    struct CurlDeleter
    {
        void operator()(CURL * handle) const noexcept
        {
            curl_easy_cleanup(handle);
        }
    };

    struct SlistDeleter
    {
        void operator()(curl_slist * list) const noexcept
        {
            curl_slist_free_all(list);
        }
    };

    std::unique_ptr<CURL, CurlDeleter> curl;
    std::unique_ptr<curl_slist, SlistDeleter> headers;
    std::string url;
    std::string payload;
    std::string body_;

    static auto collect(char * data, size_t size, size_t nmemb, void * userp) noexcept -> size_t
    {
        try {
            static_cast<std::string *>(userp)->append(data, size * nmemb);
            return size * nmemb;
        } catch (...) {
            return 0;
        }
    }

    void header(const std::string & line)
    {
        auto * next = curl_slist_append(headers.get(), line.c_str());
        if (next == nullptr) {
            throw nix::Error("curl_slist_append failed");
        }
        std::ignore = headers.release(); // now owned via next
        headers.reset(next);
    }

public:
    template<typename T>
    void opt(CURLoption option, T value)
    {
        if (curl_easy_setopt(curl.get(), option, value) != CURLE_OK) {
            throw nix::Error("curl_easy_setopt %d failed", static_cast<int>(option));
        }
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    Call(std::string url_, const std::string & bearer, const std::optional<nlohmann::json> & body)
        : url(std::move(url_))
    {
        static std::once_flag once;
        std::call_once(once, [] -> void { curl_global_init(CURL_GLOBAL_DEFAULT); });
        curl.reset(curl_easy_init());
        if (!curl) {
            throw nix::Error("curl_easy_init failed");
        }
        constexpr long timeoutSecs = 30;
        if (!bearer.empty()) {
            header("Authorization: Bearer " + bearer);
        }
        if (body) {
            payload = body->dump();
            header("Content-Type: application/json");
            opt(CURLOPT_POSTFIELDSIZE, static_cast<long>(payload.size()));
            opt(CURLOPT_POSTFIELDS, payload.c_str());
        }
        opt(CURLOPT_URL, url.c_str());
        opt(CURLOPT_HTTPHEADER, headers.get());
        opt(CURLOPT_NOSIGNAL, 1L);
        opt(CURLOPT_TIMEOUT, timeoutSecs);
        opt(CURLOPT_WRITEFUNCTION, &Call::collect);
        opt(CURLOPT_WRITEDATA, &body_);
    }

    // HTTP status, throws on transport errors.
    auto perform() -> long
    {
        auto const code = curl_easy_perform(curl.get());
        if (code != CURLE_OK && code != CURLE_WRITE_ERROR && code != CURLE_ABORTED_BY_CALLBACK) {
            throw nix::Error("%s: %s", url, curl_easy_strerror(code));
        }
        long status = 0;
        curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
        return status;
    }

    [[nodiscard]] auto body() const -> const std::string &
    {
        return body_;
    }

    [[nodiscard]] auto retryAfter() const -> std::chrono::seconds
    {
        curl_off_t wait = 0;
        curl_easy_getinfo(curl.get(), CURLINFO_RETRY_AFTER, &wait);
        return std::chrono::seconds(std::max<curl_off_t>(wait, 0));
    }
};
// NOLINTEND(cppcoreguidelines-pro-type-vararg)

} // namespace nixgrpc::http
