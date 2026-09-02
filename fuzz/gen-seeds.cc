// Writes well-formed seed inputs for each fuzz target into <dir>/<target>/,
// so mutation starts past store-path hashes, NAR hashes and protocol magics
// that libFuzzer would not synthesise. Encodes the wire formats by hand to
// stay independent of libnixstore struct churn.

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <string>
#include <string_view>

#include <nix/store/worker-protocol.hh>
#include <nix/util/archive.hh>
#include <nix/util/hash.hh>
#include <nix/util/logging.hh>
#include <nix/util/serialise.hh>

#include "nix_remote.pb.h"
#include "../src/pump.hh"
#include "support.hh"

namespace {

const std::string storeDir = "/nix/store/";
const std::string hashA = "7h7qgvs4kgzsn8a6rb273saxyqh4jxlz";
const std::string hashB = "g1w7hy3a4bpl6xkvx3wy3s6z7b0kzn0a";
const std::string pathA = storeDir + hashA + "-hello-2.12";
const std::string pathB = storeDir + hashB + "-glibc-2.40";
const std::string drvPath = storeDir + hashB + "-hello-2.12.drv";

void put(const std::filesystem::path & dir, std::string_view name, std::string_view data)
{
    std::filesystem::create_directories(dir);
    std::ofstream(dir / std::string(name), std::ios::binary) << data;
}

auto nar(std::string_view contents) -> std::string
{
    nix::StringSink sink;
    nix::dumpString(contents, sink);
    return std::move(sink.s);
}

// UnkeyedValidPathInfo, worker protocol 1.16.
void writeInfo(nix::Sink & sink, const std::string & narBytes, bool withRefs)
{
    auto narHash = nix::hashString(nix::HashAlgorithm::SHA256, narBytes);
    sink << drvPath                                              // deriver
         << narHash.to_string(nix::HashFormat::Base16, false); // narHash
    if (withRefs) {
        sink << uint64_t{2} << pathA << pathB;
    } else {
        sink << uint64_t{0};
    }
    sink << uint64_t{1700000000}          // registrationTime
         << uint64_t{narBytes.size()}     // narSize
         << uint64_t{1};                  // ultimate
    sink << uint64_t{1} << "cache.example.org-1:c2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlc2lnbmF0dXJlYWFhYQ==";
    sink << (withRefs ? "" : "fixed:r:sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s");
}

// BuildResult, worker protocol 1.37 without features.
void writeBuildResult(nix::Sink & sink, uint64_t status, std::string_view error)
{
    sink << status << error << uint64_t{1} << uint64_t{0} << uint64_t{1700000000} << uint64_t{1700000010};
    sink << uint8_t{1} << int64_t{123456} << uint8_t{0}; // cpuUser, cpuSystem
    if (error.empty()) {
        std::string const id =
            "sha256:15e3c560894cbb27085cf65b5a2ecb18488c999497f4531b6907a7581ce6d527!out";
        sink << uint64_t{1} << id << (R"({"dependentRealisations":{},"id":")" + id + R"(","outPath":")" + hashA + R"(-hello-2.12","signatures":[]})");
    } else {
        sink << uint64_t{0};
    }
}

void writeDrv(nix::Sink & sink)
{
    sink << uint64_t{2}                               // outputs
         << "out" << pathA << "" << ""                // input-addressed
         << "dev" << "" << "r:sha256" << ""           // floating CA
         << uint64_t{1} << pathB                      // inputSrcs
         << "x86_64-linux" << pathB + "/bin/sh"       // platform, builder
         << uint64_t{2} << "-c" << "echo hello > $out" // args
         << uint64_t{3} << "name" << "hello" << "out" << pathA << "outputs" << "out dev";
}

void writeError(nix::Sink & sink)
{
    sink << "Error" << uint64_t{0} << "Error" << "builder for '\x1b[35;1m" + drvPath + "\x1b[0m' failed"
         << uint64_t{0}                                   // havePos
         << uint64_t{2} << uint64_t{0} << "while building hello" << uint64_t{0} << "while evaluating";
}

void writeFields(nix::Sink & sink, std::initializer_list<std::pair<uint64_t, std::string>> fields)
{
    sink << uint64_t{fields.size()};
    for (const auto & [num, str] : fields) {
        if (str.empty()) {
            sink << uint64_t{0} << num;
        } else {
            sink << uint64_t{1} << str;
        }
    }
}

auto zstd(std::string_view plain) -> std::string
{
    nixgrpc::fuzz::CollectWriter<nix::remote::Chunk> out;
    nixgrpc::ZstdWriterSink<decltype(out), nix::remote::Chunk> sink(out);
    sink(plain.substr(0, plain.size() / 2));
    sink.flush();
    sink(plain.substr(plain.size() / 2));
    sink.finish();
    return out.out;
}

} // namespace

auto main(int argc, char ** argv) -> int
{
    if (argc != 2) {
        return 1;
    }
    std::filesystem::path const root = argv[1];
    auto const smallNar = nar("hello\n");

    {
        auto dir = root / "wire";
        put(dir, "storepath", std::string("\x00", 1) + hashA + "-hello-2.12");
        put(dir, "derivedpath-opaque", "\x01" + pathA);
        put(dir, "derivedpath-built", "\x01" + drvPath + "^out,dev");
        put(dir, "derivedpath-all", "\x01" + drvPath + "^*");
        {
            nix::StringSink s;
            s.s = "\x02" + hashB + "-hello-2.12.drv" + std::string("\0", 1);
            writeDrv(s);
            put(dir, "drv", s.s);
        }
        for (bool refs : {false, true}) {
            nix::StringSink info;
            writeInfo(info, smallNar, refs);
            nix::remote::PathInfo entry;
            entry.set_path(hashA + "-hello-2.12");
            entry.set_info(info.s);
            put(dir, refs ? "pathinfo-refs" : "pathinfo-ca", "\x03" + entry.SerializeAsString());
        }
        {
            nix::StringSink s;
            s.s = "\x04";
            writeBuildResult(s, 0, "");
            put(dir, "buildresult-ok", s.s);
        }
        {
            nix::StringSink s;
            s.s = "\x04";
            writeBuildResult(s, 10, "dependency failed");
            put(dir, "buildresult-fail", s.s);
        }
        {
            nix::StringSink s;
            s.s = "\x05";
            s << uint64_t{2} << pathA;
            writeBuildResult(s, 0, "");
            s << drvPath + "!out";
            writeBuildResult(s, 3, "build failed");
            put(dir, "keyed", s.s);
        }
    }

    {
        nix::StringSink s;
        s.s = std::string("\x07\x3f", 2); // msgSize, flushEvery
        auto bigNar = nar(std::string(20000, 'x'));
        s << uint64_t{2};
        s << pathA;
        writeInfo(s, smallNar, true);
        s.s += smallNar;
        s << pathB;
        writeInfo(s, bigNar, false);
        s.s += bigNar;
        put(root / "import-paths", "two-paths", s.s);
    }

    {
        nix::StringSink s;
        s << uint64_t{STDERR_NEXT} << "building '" + drvPath + "'...\n";
        s << uint64_t{STDERR_START_ACTIVITY} << uint64_t{1} << uint64_t{3} << uint64_t{nix::actBuild}
          << "building hello";
        writeFields(s, {{0, drvPath}, {0, "builder"}, {1, ""}, {1, ""}});
        s << uint64_t{0};
        s << uint64_t{STDERR_RESULT} << uint64_t{1} << uint64_t{nix::resBuildLogLine};
        writeFields(s, {{0, "hello world"}});
        s << uint64_t{STDERR_RESULT} << uint64_t{1} << uint64_t{nix::resProgress};
        writeFields(s, {{1, ""}, {2, ""}, {3, ""}, {4, ""}});
        s << uint64_t{STDERR_STOP_ACTIVITY} << uint64_t{1};
        s << uint64_t{STDERR_LAST};
        put(root / "build-log", "activity", s.s);

        nix::StringSink e;
        e << uint64_t{STDERR_NEXT} << "oops\n" << uint64_t{STDERR_ERROR};
        writeError(e);
        put(root / "build-log", "error", e.s);
    }

    {
        auto z = zstd(smallNar + std::string(5000, 'y'));
        put(root / "zstd-reader", "flushed", "\x20" + z);
        put(root / "tunnel-pump", "flushed", "\x01" + z);
    }

    {
        // [index][flags][len][payload], flags bit 2 = compress.
        std::string s;
        auto frame = [&](uint8_t idx, uint8_t flags, std::string_view payload) {
            s += static_cast<char>(idx);
            s += static_cast<char>(flags);
            s += static_cast<char>(payload.size());
            s += payload;
        };
        frame(0, 2, smallNar.substr(0, 20));
        frame(1, 2, smallNar);
        frame(0, 2 | 1, smallNar.substr(20));
        frame(1, 1, "");
        frame(2, 2 | 4 | 1, smallNar);
        put(root / "nar-frames", "interleaved", s);
    }

    put(root / "acl", "rules", std::string("ci-*=write\ndeploy-?=trusted\n[a-c]*=read-only") + '\0' + "ci-1");
    return 0;
}
