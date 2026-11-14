#pragma once

// Keeps a builder's tunnels to every balancer open.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <nix/util/file-descriptor.hh>

#include "tunnel.hh"

namespace nixgrpc::tunnel {

struct Balancer
{
    std::string host;
    uint16_t port = 0;
};

// "host:port" or "[IPv6 address]:port"
auto parseBalancer(std::string_view spec) -> Balancer;

// The IP addresses of `host`.
auto addresses(const std::string & host) -> std::vector<std::string>;

// Holds, to every address of every balancer, `perSet` tunnels for requests
// addressed to this builder and as many for requests any builder may answer.
// The second set is only open while the builder takes work.
class Tunnels
{
public:
    struct Config
    {
        std::vector<Balancer> balancers;
        Tls tls;
        std::string name; // the builder's, as in its certificate
        // A balancer keeps tunnels per thread, so this is its thread count.
        unsigned perSet = 4;
        // Takes a tunnel on which the balancer started a request.
        std::function<void(nix::AutoCloseFD)> adopt;
        std::function<std::vector<std::string>(const std::string &)> resolve = addresses;
        // A balancer behind a DNS name can be replaced at any time.
        std::chrono::seconds resolveEvery{5}; // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    };

    explicit Tunnels(Config config);
    // Cuts the tunnels that are still open.
    ~Tunnels();
    Tunnels(const Tunnels &) = delete;
    auto operator=(const Tunnels &) -> Tunnels & = delete;
    Tunnels(Tunnels &&) = delete;
    auto operator=(Tunnels &&) -> Tunnels & = delete;

    void setTakesWork(bool takes);
    // Opens no more tunnels. Those in use stay until their owner closes them.
    void stop();

private:
    struct Slot
    {
        Target target;
        Identity self;
        bool any = false;
        Wake stir;
        std::atomic<bool> done{false};
        std::jthread thread;
    };

    struct Group
    {
        // An address that left DNS keeps its tunnels until they fail.
        std::atomic<bool> listed{true};
        std::vector<std::unique_ptr<Slot>> slots;
    };

    void supervise();
    void refresh(size_t index, const Balancer & balancer);
    [[nodiscard]] auto makeGroup(const Balancer & balancer, const std::string & address) const -> std::unique_ptr<Group>;
    void start(Slot & slot, const Group & group);
    void run(Slot & slot, const Group & group);
    void hold(Slot & slot);
    [[nodiscard]] auto leaves(const Slot & slot) const -> bool;
    void stirAll(bool anyOnly);

    Config config;
    std::atomic<bool> takesWork{true};
    std::atomic<bool> stopping{false};
    std::atomic<bool> cutting{false};
    Wake pause;
    std::mutex mutex; // guards `groups`
    std::map<std::pair<size_t, std::string>, std::unique_ptr<Group>> groups;
    std::jthread supervisor;
};

} // namespace nixgrpc::tunnel
