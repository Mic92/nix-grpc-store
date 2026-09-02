#pragma once

#include <charconv>
#include <optional>
#include <string_view>
#include <system_error>

namespace nixgrpc {

// Whole-string decimal parse. Rejects sign on unsigned, whitespace and
// trailing junk, unlike std::sto*.
template<typename T>
inline auto parseInt(std::string_view str) -> std::optional<T>
{
    T value{};
    const char * first = str.data();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic): from_chars wants [first,last).
    const char * last = first + str.size();
    auto [end, errc] = std::from_chars(first, last, value);
    if (str.empty() || errc != std::errc() || end != last) {
        return std::nullopt;
    }
    return value;
}

} // namespace nixgrpc
