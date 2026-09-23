#include "eds.hh"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <absl/base/thread_annotations.h>
#include <absl/strings/numbers.h>
#include <absl/synchronization/mutex.h>
#include <google/protobuf/any.pb.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>

#include "acl.hh"
#include "auth.hh"
#include "dispatcher.hh"
#include "eds.pb.h"
#include "logfmt.hh"
#include "sched-reactor.hh"

namespace nixgrpc {

namespace pb = envoy::service::endpoint::v3;

class EdsReactor;

namespace {

constexpr std::string_view assignmentType = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";

// Peer input is bounded.
constexpr size_t maxNames = 32;
constexpr size_t maxNameLen = 64;
constexpr size_t maxAddrLen = 64;
constexpr size_t maxEndpoints = 4096;
constexpr size_t maxLogLen = 200;
constexpr size_t maxPortDigits = 5;
constexpr uint32_t maxPort = 65535;

auto clip(const std::string & text) -> std::string
{
    return text.substr(0, maxLogLen);
}

struct Endpoint
{
    std::string host;
    uint16_t port = 0;
};

// "1.2.3.4:50052" or "[::1]:50052". Envoy takes IP literals only.
auto parseAddr(std::string_view addr) -> std::expected<Endpoint, std::string_view>
{
    if (addr.empty() || addr.size() > maxAddrLen || addr.contains('\0')) {
        return std::unexpected("bad length or NUL");
    }
    auto const colon = addr.rfind(':');
    if (colon == std::string_view::npos) {
        return std::unexpected("no port");
    }
    auto host = addr.substr(0, colon);
    auto const digits = addr.substr(colon + 1);
    bool const bracketed = host.starts_with('[') && host.ends_with(']');
    if (bracketed) {
        host = host.substr(1, host.size() - 2);
    }
    uint32_t port = 0;
    bool const numeric = !digits.empty() && digits.size() <= maxPortDigits
        && std::ranges::all_of(digits, [](char chr) -> bool { return chr >= '0' && chr <= '9'; });
    if (!numeric || !absl::SimpleAtoi(digits, &port) || port == 0 || port > maxPort) {
        return std::unexpected("bad port");
    }
    const std::string text(host);
    in6_addr buf{};
    if (inet_pton(bracketed ? AF_INET6 : AF_INET, text.c_str(), &buf) != 1) {
        return std::unexpected("not an IP literal");
    }
    return Endpoint{.host = text, .port = static_cast<uint16_t>(port)};
}

auto assignment(const std::string & system, const std::vector<Dispatcher::Member> & members)
    -> std::expected<google::protobuf::Any, std::string_view>
{
    pb::ClusterLoadAssignment cla;
    cla.set_cluster_name(system);
    auto * locality = cla.add_endpoints();
    for (const auto & member : members) {
        if (std::cmp_greater_equal(locality->lb_endpoints_size(), maxEndpoints)) {
            break;
        }
        auto const parsed = parseAddr(member.addr);
        if (!parsed) {
            logLine(
                LogLevel::info,
                {{"event", "eds_bad_addr"}, {"addr", clip(member.addr)}, {"why", std::string(parsed.error())}});
            continue;
        }
        auto * lbe = locality->add_lb_endpoints();
        lbe->set_health_status(member.draining ? pb::DRAINING : pb::HEALTHY);
        auto * sock = lbe->mutable_endpoint()->mutable_address()->mutable_socket_address();
        sock->set_address(parsed->host);
        sock->set_port_value(parsed->port);
    }
    // PackFrom would use our package name, envoy expects its own.
    google::protobuf::Any res;
    res.set_type_url(std::string(assignmentType));
    if (!cla.SerializeToString(res.mutable_value())) {
        return std::unexpected("cannot serialize the endpoint list");
    }
    return res;
}

auto collectNames(const pb::DiscoveryRequest & req, std::set<std::string> & names) -> bool
{
    if (std::cmp_greater(req.resource_names_size(), maxNames)) {
        return false;
    }
    for (const auto & name : req.resource_names()) {
        if (name.empty() || name.size() > maxNameLen) {
            return false;
        }
        names.insert(name);
    }
    return true;
}

// A queued Dispatcher wake-up can outlive the reactor, so it holds this.
struct Stream
{
    absl::Mutex mutex;
    EdsReactor * reactor ABSL_GUARDED_BY(mutex) = nullptr;
};

enum class Phase : uint8_t { Open, Closed, ReadsDone, Finished };

} // namespace

// `writing` is claimed before send() and Finish() waits for it, so a reactor
// in send() is never deleted.
class EdsReactor final : public grpc::ServerBidiReactor<pb::DiscoveryRequest, pb::DiscoveryResponse>
{
public:
    EdsReactor(grpc::CallbackServerContext * ctx, Dispatcher & disp, EdsService & svc)
        : ctx(ctx)
        , disp(disp)
        , svc(svc)
        , stream(std::make_shared<Stream>())
    {
        {
            const absl::MutexLock lock(stream->mutex);
            stream->reactor = this;
        }
        watch = disp.watchMembership([stream = stream] -> void {
            EdsReactor * run = nullptr;
            {
                const absl::MutexLock lock(stream->mutex);
                if (stream->reactor != nullptr && stream->reactor->beginWake()) {
                    run = stream->reactor;
                }
            }
            if (run != nullptr) {
                run->send();
            }
        });
        StartRead(&in);
    }

    void OnReadDone(bool isOk) override
    {
        if (!isOk) {
            finish(grpc::Status::OK);
            return;
        }
        if (!in.type_url().empty() && in.type_url() != assignmentType) {
            finish({grpc::StatusCode::INVALID_ARGUMENT, "only ClusterLoadAssignment is served"});
            return;
        }
        if (in.error_detail().code() != 0) {
            logLine(
                LogLevel::info,
                {{"event", "eds_nack"}, {"version", in.version_info()}, {"error", clip(in.error_detail().message())}});
        }
        std::set<std::string> want;
        if (!collectNames(in, want)) {
            finish({grpc::StatusCode::INVALID_ARGUMENT, "bad resource_names"});
            return;
        }
        bool run = false;
        {
            const absl::MutexLock lock(mutex);
            if (want != names) {
                names = std::move(want);
                dirty = true;
                run = claimWriter();
            }
        }
        StartRead(&in);
        if (run) {
            send();
        }
    }

    void OnWriteDone(bool isOk) override
    {
        if (!isOk) {
            {
                const absl::MutexLock lock(mutex);
                close();
                writing = false;
            }
            ctx->TryCancel();
            maybeDone();
            return;
        }
        bool again = false;
        {
            const absl::MutexLock lock(mutex);
            writing = again = dirty && phase == Phase::Open;
        }
        if (again) {
            send();
        } else {
            maybeDone();
        }
    }

    void OnDone() override
    {
        disp.unwatchMembership(watch);
        {
            const absl::MutexLock lock(stream->mutex);
            stream->reactor = nullptr;
        }
        svc.release();
        delete this; // NOLINT(cppcoreguidelines-owning-memory): see EdsService::StreamEndpoints
    }

private:
    // Called with Stream::mutex held. True: the caller must run send().
    auto beginWake() -> bool
    {
        if (!svc.active()) {
            ctx->TryCancel();
            return false;
        }
        const absl::MutexLock lock(mutex);
        dirty = true;
        return claimWriter();
    }

    [[nodiscard("writing stays claimed until send() runs")]] ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) auto claimWriter()
        -> bool
    {
        if (writing || phase != Phase::Open || names.empty()) {
            return false;
        }
        return writing = true;
    }

    ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) void close()
    {
        phase = std::max(phase, Phase::Closed);
    }

    void send()
    {
        for (;;) {
            std::set<std::string> want;
            {
                const absl::MutexLock lock(mutex);
                if (phase != Phase::Open || !dirty || names.empty()) {
                    writing = false;
                    break;
                }
                dirty = false;
                want = names;
            }
            auto const shared = svc.render(disp.membership());
            out.Clear();
            std::string_view failure;
            for (const auto & system : want) {
                auto const iter = shared->find(system);
                if (iter != shared->end()) {
                    *out.add_resources() = iter->second;
                    continue;
                }
                auto res = assignment(system, {});
                if (!res) {
                    failure = res.error();
                    break;
                }
                *out.add_resources() = std::move(*res);
            }
            if (!failure.empty()) {
                logLine(LogLevel::info, {{"event", "eds_error"}, {"error", std::string(failure)}});
                {
                    const absl::MutexLock lock(mutex);
                    close();
                    writing = false;
                }
                ctx->TryCancel();
                break;
            }
            out.set_type_url(std::string(assignmentType));
            out.set_version_info(std::to_string(++version));
            out.set_nonce(out.version_info());
            StartWrite(&out);
            return;
        }
        maybeDone();
    }

    void finish(grpc::Status fin)
    {
        {
            const absl::MutexLock lock(mutex);
            if (phase < Phase::ReadsDone) {
                status = std::move(fin);
                phase = Phase::ReadsDone;
            }
        }
        maybeDone();
    }

    void maybeDone()
    {
        std::optional<grpc::Status> fin;
        {
            const absl::MutexLock lock(mutex);
            if (phase == Phase::ReadsDone && !writing) {
                phase = Phase::Finished;
                fin = status;
            }
        }
        if (fin) {
            Finish(*fin);
        }
    }

    grpc::CallbackServerContext * ctx;
    Dispatcher & disp;
    EdsService & svc;
    std::shared_ptr<Stream> stream;
    uint64_t watch = 0;
    pb::DiscoveryRequest in;
    pb::DiscoveryResponse out;
    uint64_t version = 0; // only the holder of `writing` touches it

    absl::Mutex mutex;
    std::set<std::string> names ABSL_GUARDED_BY(mutex);
    grpc::Status status ABSL_GUARDED_BY(mutex) = grpc::Status::OK;
    bool dirty ABSL_GUARDED_BY(mutex) = false;
    bool writing ABSL_GUARDED_BY(mutex) = false;
    Phase phase ABSL_GUARDED_BY(mutex) = Phase::Open;
};

EdsService::EdsService(Dispatcher & dispatcher, Auth & auth)
    : dispatcher(&dispatcher)
    , auth(&auth)
{
}

auto EdsService::acquire() -> bool
{
    auto open = open_.load();
    while (open < maxStreams) {
        if (open_.compare_exchange_weak(open, open + 1)) {
            return true;
        }
    }
    return false;
}

void EdsService::release()
{
    open_--;
}

auto EdsService::render(const std::shared_ptr<const Dispatcher::Membership> & members) -> std::shared_ptr<const Rendered>
{
    const absl::MutexLock lock(renderMutex);
    if (renderedFrom == members) {
        return rendered;
    }
    auto next = std::make_shared<Rendered>();
    for (const auto & [system, list] : *members) {
        auto res = assignment(system, list);
        if (res) {
            next->emplace(system, std::move(*res));
        } else {
            logLine(LogLevel::info, {{"event", "eds_error"}, {"system", clip(system)}, {"error", std::string(res.error())}});
        }
    }
    renderedFrom = members;
    rendered = std::move(next);
    return rendered;
}

auto EdsService::StreamEndpoints(grpc::CallbackServerContext * context)
    -> grpc::ServerBidiReactor<pb::DiscoveryRequest, pb::DiscoveryResponse> *
{
    using Reject = RejectReactor<pb::DiscoveryRequest, pb::DiscoveryResponse>;
    if (!active_) {
        return new Reject({grpc::StatusCode::UNAVAILABLE, "scheduler passive, another node serves"}); // NOLINT(cppcoreguidelines-owning-memory)
    }
    auto const caller = auth->identifyDirect(*context);
    if (auto const status = Auth::authorize(caller, "StreamEndpoints", Role::trusted); !status.ok()) {
        return new Reject(status); // NOLINT(cppcoreguidelines-owning-memory)
    }
    if (!acquire()) {
        return new Reject({grpc::StatusCode::RESOURCE_EXHAUSTED, "too many endpoint streams"}); // NOLINT(cppcoreguidelines-owning-memory)
    }
    logLine(LogLevel::info, {{"event", "eds_stream_open"}, {"cn", caller.name}, {"peer", context->peer()}});
    return new EdsReactor(context, *dispatcher, *this); // NOLINT(cppcoreguidelines-owning-memory)
}

} // namespace nixgrpc
