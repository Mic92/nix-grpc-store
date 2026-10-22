#pragma once
// Validation of a streaming build-input importer. An upload Open alone is
// insufficient: the worker also checks its live Expect before importing.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "nix_remote.pb.h"
#include "reverse-capability.hh"

namespace nixgrpc {

class ReverseUpload
{
public:
    static constexpr size_t maxChunkBytes = 1024 * 1024;

    [[nodiscard]] static auto first(const ReverseCapability::Verified & delegated,
                                    const nix::remote::UploadBuildInputsChunk & frame) -> bool
    {
        return frame.ByteSizeLong() <= maxChunkBytes && frame.has_chunk()
            && ReverseCapability::permitsBuildInputUpload(delegated, frame.drv_path(), frame.assign_id(), frame.chunk());
    }

    [[nodiscard]] static auto next(const nix::remote::UploadBuildInputsChunk & frame) -> bool
    {
        return frame.ByteSizeLong() <= maxChunkBytes && frame.has_chunk() && frame.drv_path().empty()
            && frame.assign_id() == 0 && !frame.chunk().repair() && !frame.chunk().check_sigs()
            && !frame.chunk().data().empty();
    }

    // Called by the per-upload pipe; never retain more than one framed
    // protobuf, irrespective of NAR size.
    class Decoder
    {
    public:
        using Consume = std::function<bool(nix::remote::UploadBuildInputsChunk &&)>;

        explicit Decoder(ReverseCapability::Verified delegated) : delegated(std::move(delegated)) { }

        [[nodiscard]] auto feed(std::string_view bytes, const Consume & consume) -> bool
        {
            if (failed || bytes.empty() || !consume) return fail();
            while (!bytes.empty()) {
                auto const target = expected ? static_cast<size_t>(*expected) + 4 : 4;
                auto const count = std::min(bytes.size(), target - buffer.size());
                buffer.append(bytes.data(), count);
                bytes.remove_prefix(count);
                if (!expected && buffer.size() == 4) {
                    uint32_t length = 0;
                    for (unsigned i = 0; i < 4; ++i)
                        length |= static_cast<uint32_t>(static_cast<unsigned char>(buffer[i])) << (8 * i);
                    if (length == 0 || length > maxChunkBytes) return fail();
                    expected = length;
                }
                if (expected && buffer.size() == *expected + 4) {
                    nix::remote::UploadBuildInputsChunk frame;
                    if (!frame.ParseFromArray(buffer.data() + 4, static_cast<int>(*expected))
                        || !(seenFirst ? next(frame) : first(delegated, frame)) || !consume(std::move(frame)))
                        return fail();
                    seenFirst = true;
                    expected.reset();
                    buffer.clear();
                }
            }
            return true;
        }

        [[nodiscard]] auto complete() const -> bool { return !failed && seenFirst && buffer.empty() && !expected; }

    private:
        [[nodiscard]] auto fail() -> bool { failed = true; buffer.clear(); expected.reset(); return false; }
        ReverseCapability::Verified delegated;
        std::string buffer;
        std::optional<uint32_t> expected;
        bool seenFirst = false;
        bool failed = false;
    };
};

} // namespace nixgrpc
