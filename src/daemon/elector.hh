#pragma once
// Holds niks3's POST /api/farm/lead open. onChange(true) while it says
// {"lead":true}, onChange(false) otherwise. Starts passive.

#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "niks3-client.hh"

namespace nixgrpc {

class Elector
{
public:
    Elector(const Niks3Client & niks3, std::function<void(bool)> onChange);
    ~Elector();
    Elector(const Elector &) = delete;
    auto operator=(const Elector &) -> Elector & = delete;
    Elector(Elector &&) = delete;
    auto operator=(Elector &&) -> Elector & = delete;

private:
    const Niks3Client & niks3;
    std::function<void(bool)> onChange;
    bool active = false;
    std::jthread thread;
    void run(const std::stop_token & stop);
    void set(bool lead);
};

} // namespace nixgrpc
