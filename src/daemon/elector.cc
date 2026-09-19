#include "elector.hh"

#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <curl/curl.h>
#include <curl/system.h>
#include <nlohmann/json_fwd.hpp>
#include <nlohmann/json.hpp>

#include "http.hh"
#include "logfmt.hh"
#include "niks3-client.hh"

namespace nixgrpc {

namespace {
constexpr std::chrono::seconds retryEvery{1};
// niks3 sends a line every 5 s.
constexpr long silentSecs = 15;

struct Reader
{
    std::stop_token stop;
    std::function<void(bool)> lead;
    std::string buf;
};

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters): curl's signature
auto onData(char * data, size_t size, size_t nmemb, void * userp) noexcept -> size_t
{
    auto & reader = *static_cast<Reader *>(userp);
    try {
        reader.buf.append(data, size * nmemb);
        for (size_t eol = 0; (eol = reader.buf.find('\n')) != std::string::npos;) {
            auto line = nlohmann::json::parse(std::string_view(reader.buf).substr(0, eol), nullptr, false);
            reader.buf.erase(0, eol + 1);
            if (auto lead = line.find("lead"); line.is_object() && lead != line.end() && lead->is_boolean()) {
                reader.lead(lead->get<bool>());
            }
        }
        return size * nmemb;
    } catch (...) {
        return 0;
    }
}

auto onProgress(void * userp, curl_off_t /*dlt*/, curl_off_t /*dln*/, curl_off_t /*ult*/, curl_off_t /*uln*/) noexcept
    -> int
{
    return static_cast<Reader *>(userp)->stop.stop_requested() ? 1 : 0;
}
} // namespace

Elector::Elector(const Niks3Client & niks3_, std::function<void(bool)> onChange_)
    : niks3(niks3_)
    , onChange(std::move(onChange_))
    , thread([this](const std::stop_token & stop) -> void { run(stop); })
{
}

Elector::~Elector()
{
    thread.request_stop();
    thread.join();
}

void Elector::set(bool lead)
{
    if (lead == active) {
        return;
    }
    active = lead;
    logLine(LogLevel::info, {{"event", lead ? "scheduler_take_over" : "scheduler_yield"}});
    onChange(lead);
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-vararg): curl_easy_setopt
void Elector::run(const std::stop_token & stop)
{
    while (!stop.stop_requested()) {
        try {
            auto call = niks3.post("/api/farm/lead", nlohmann::json::object());
            Reader reader{.stop = stop, .lead = [this](bool lead) -> void { set(lead); }, .buf = {}};
            call->opt(CURLOPT_TIMEOUT, 0L);
            call->opt(CURLOPT_LOW_SPEED_LIMIT, 1L);
            call->opt(CURLOPT_LOW_SPEED_TIME, silentSecs);
            call->opt(CURLOPT_WRITEDATA, &reader);
            call->opt(CURLOPT_WRITEFUNCTION, &onData);
            call->opt(CURLOPT_NOPROGRESS, 0L);
            call->opt(CURLOPT_XFERINFODATA, &reader);
            call->opt(CURLOPT_XFERINFOFUNCTION, &onProgress);
            auto status = call->perform();
            if (!stop.stop_requested()) {
                logLine(LogLevel::info, {{"event", "scheduler_lock_lost"}, {"status", std::to_string(status)}});
            }
        } catch (const std::exception & e) {
            logLine(LogLevel::info, {{"event", "scheduler_lock_lost"}, {"error", e.what()}});
        }
        set(false);
        std::this_thread::sleep_for(retryEvery);
    }
}

// NOLINTEND(cppcoreguidelines-pro-type-vararg)

} // namespace nixgrpc
