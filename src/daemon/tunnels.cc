#include "tunnels.hh"

#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <nix/util/error.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/finally.hh>
#include <nix/util/terminal.hh>

#include "client/retry-pause.hh"
#include "logfmt.hh"
#include "tunnel-protocol.hh"
#include "tunnel.hh"

// NOLINTBEGIN(misc-include-cleaner): which header declares a socket name differs per platform.
namespace nixgrpc::tunnel {

auto parseBalancer(std::string_view spec) -> Balancer
{
    const auto colon = spec.rfind(':');
    uint16_t port = 0;
    if (colon != std::string_view::npos) {
        const auto digits = spec.substr(colon + 1);
        const auto [end, err] = std::from_chars(digits.data(), digits.data() + digits.size(), port); // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (err != std::errc{} || end != digits.data() + digits.size()) { // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            port = 0;
        }
    }
    auto host = std::string(spec.substr(0, colon));
    bool valid = validName(host);
    if (host.starts_with('[') && host.ends_with(']')) {
        host = host.substr(1, host.size() - 2);
        in6_addr scratch{};
        valid = validAddress6(host) && inet_pton(AF_INET6, host.c_str(), &scratch) == 1;
    }
    if (port == 0 || !valid) {
        throw nix::Error("--tunnel expects host:port or [address]:port, not '%s'", spec);
    }
    return {.host = std::move(host), .port = port};
}

auto addresses(const std::string & host) -> std::vector<std::string>
{
    addrinfo hints{};
    hints.ai_socktype = SOCK_STREAM;
    addrinfo * raw = nullptr;
    if (const int err = getaddrinfo(host.c_str(), nullptr, &hints, &raw); err != 0) {
        throw nix::Error("resolving '%s': %s", host, gai_strerror(err));
    }
    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> list{raw, freeaddrinfo};
    std::set<std::string> found;
    for (const addrinfo * entry = raw; entry != nullptr; entry = entry->ai_next) {
        std::array<char, INET6_ADDRSTRLEN> text{};
        const void * addr = nullptr;
        // NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
        if (entry->ai_family == AF_INET) {
            addr = &reinterpret_cast<const sockaddr_in *>(entry->ai_addr)->sin_addr;
        } else if (entry->ai_family == AF_INET6) {
            addr = &reinterpret_cast<const sockaddr_in6 *>(entry->ai_addr)->sin6_addr;
        }
        // NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
        if (addr != nullptr && inet_ntop(entry->ai_family, addr, text.data(), text.size()) != nullptr) {
            found.emplace(text.data());
        }
    }
    return {found.begin(), found.end()};
}

Tunnels::Tunnels(Config config)
    : config(std::move(config))
    , supervisor([this] -> void { supervise(); })
{
}

Tunnels::~Tunnels()
{
    cutting = true;
    stop();
    // First, so that no slot starts after the last wake-up.
    supervisor = {};
    stirAll(false);
    const std::scoped_lock lock(mutex);
    groups.clear();
}

void Tunnels::stop()
{
    stopping = true;
    pause.set();
    stirAll(false);
}

void Tunnels::setTakesWork(bool takes)
{
    if (takesWork.exchange(takes) != takes) {
        stirAll(true);
    }
}

void Tunnels::stirAll(bool anyOnly)
{
    const std::scoped_lock lock(mutex);
    for (const auto & [key, group] : groups) {
        for (const auto & slot : group->slots) {
            if (slot->any || !anyOnly) {
                slot->stir.set();
            }
        }
    }
}

void Tunnels::supervise()
{
    while (!stopping) {
        for (size_t index = 0; index < config.balancers.size(); index++) {
            try {
                refresh(index, config.balancers.at(index));
            } catch (nix::Error & err) {
                // Keeps the addresses it had.
                logLine(LogLevel::info, {{"event", "tunnel_resolve_failed"}, {"error", nix::filterANSIEscapes(err.info().msg.str(), true)}});
            }
        }
        (void) pause.wait(config.resolveEvery);
    }
}

auto Tunnels::makeGroup(const Balancer & balancer, const std::string & address) const -> std::unique_ptr<Group>
{
    auto group = std::make_unique<Group>();
    for (const bool any : {false, true}) {
        for (unsigned count = 0; count < config.perSet; count++) {
            auto slot = std::make_unique<Slot>();
            slot->any = any;
            slot->target = {
                .host = balancer.host,
                .address = address,
                .port = balancer.port,
                .protocol = std::string(any ? anyProtocol : namedProtocol),
            };
            const auto own = nodeName(config.name, config.instance);
            slot->self = any ? Identity{.node = "any/" + own, .group = "any", .instance = config.instance, .name = config.name}
                             : Identity{.node = own, .group = own, .instance = config.instance, .name = config.name};
            // Not running yet.
            slot->done = true;
            group->slots.push_back(std::move(slot));
        }
    }
    return group;
}

void Tunnels::refresh(size_t index, const Balancer & balancer)
{
    const auto found = config.resolve(balancer.host);
    const std::set<std::string> now{found.begin(), found.end()};
    const std::scoped_lock lock(mutex);
    for (const auto & address : now) {
        if (!groups.contains({index, address})) {
            groups.emplace(std::pair{index, address}, makeGroup(balancer, address));
        }
    }
    for (auto it = groups.begin(); it != groups.end();) {
        auto & [key, group] = *it;
        if (key.first != index) {
            ++it;
            continue;
        }
        if (group->listed.exchange(now.contains(key.second)) && !group->listed) {
            // Lets the slots that wait to take work again end.
            for (const auto & slot : group->slots) {
                slot->stir.set();
            }
        }
        bool idle = true;
        for (const auto & slot : group->slots) {
            if (slot->done && group->listed && !stopping) {
                start(*slot, *group);
            }
            idle = idle && slot->done;
        }
        it = idle && !group->listed ? groups.erase(it) : std::next(it);
    }
}

void Tunnels::start(Slot & slot, Group & group)
{
    slot.thread = {};
    slot.done = false;
    try {
        slot.thread = std::jthread([this, &slot, &group] -> void {
            run(slot, group);
            slot.done = true;
        });
    } catch (std::system_error & err) {
        // Out of threads: the next refresh tries again.
        slot.done = true;
        logLine(LogLevel::info, {{"event", "tunnel_failed"}, {"address", slot.target.address}, {"error", err.what()}});
    }
}

auto Tunnels::leaves(const Slot & slot) const -> bool
{
    return cutting || (slot.any && !takesWork);
}

void Tunnels::run(Slot & slot, Group & group)
{
    unsigned failures = 0;
    while (!stopping) {
        // Cleared before the flags are read, so that no change is missed.
        slot.stir.clear();
        if (stopping) {
            break;
        }
        if (slot.any && !takesWork) {
            if (!group.listed) {
                break;
            }
            (void) slot.stir.wait(std::nullopt);
            continue;
        }
        try {
            const auto since = std::chrono::steady_clock::now();
            hold(slot, group);
            // A tunnel that lasted was not a failure to open one.
            if (std::chrono::steady_clock::now() - since >= maxPause) {
                failures = 0;
            }
        } catch (nix::Error & err) {
            if (failures == 0) {
                logLine(
                    LogLevel::info,
                    {{"event", "tunnel_failed"},
                     {"balancer", slot.target.host},
                     {"address", slot.target.address},
                     {"error", nix::filterANSIEscapes(err.info().msg.str(), true)}});
            }
        }
        if (!group.listed) {
            break;
        }
        (void) slot.stir.wait(retryPause(++failures));
    }
}

// One tunnel, from opening it until it ended.
void Tunnels::hold(Slot & slot, Group & group)
{
    // One line when a balancer has all its tunnels and one when it closes the
    // first, however many tunnels there are. The other set comes and goes
    // with the builder's load and is left out.
    const auto report = [&](std::string_view event) -> void {
        logLine(LogLevel::info, {{"event", std::string(event)}, {"balancer", slot.target.host}, {"address", slot.target.address}});
    };
    bool counted = false;
    const auto accepted = [&] -> void {
        counted = !slot.any;
        if (counted && ++group.open == config.perSet && !group.reported.exchange(true)) {
            report("tunnels_open");
        }
    };
    // Any end the builder did not choose is a loss, also a failed hand-over.
    bool left = false;
    // Finally is nix::Finally in newer versions of nix.
    using namespace nix; // NOLINT(google-build-using-namespace)
    const Finally ended([&] -> void {
        if (!counted) {
            return;
        }
        --group.open;
        if (!left && group.reported.exchange(false)) {
            report("tunnel_lost");
        }
    });

    auto sock = open(slot.target, config.tls, slot.self, slot.stir, accepted);
    if (!sock) {
        left = true;
        return;
    }
    // The second descriptor shows when the new owner's socket ended, and can end it.
    const nix::AutoCloseFD watch{dup(sock->get())};
    if (!watch) {
        throw nix::SysError("dup");
    }
    config.adopt(std::move(*sock));
    for (;;) {
        slot.stir.clear();
        if (leaves(slot)) {
            shutdown(watch.get(), SHUT_RDWR);
            left = true;
            return;
        }
        if (waitClosed(watch.get(), slot.stir) == End::closed) {
            return;
        }
    }
}

} // namespace nixgrpc::tunnel
// NOLINTEND(misc-include-cleaner)
