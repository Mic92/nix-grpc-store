// grpc:// Store: batched path-info queries and resumable NAR upload.

#include "store.hh"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <ranges>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/logging.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>

#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "path-info-wire.hh"
#include "pump.hh"

namespace nix {

auto GrpcStore::queryValidPaths(const StorePathSet & paths, SubstituteFlag maybeSubstitute)
    -> StorePathSet {
  grpc::ClientContext ctx;
  remote::QueryValidPathsRequest request;
  request.set_substitute(maybeSubstitute == Substitute);
  for (const auto &path : paths) {
    request.add_paths(std::string(path.to_string()));
  }
  remote::QueryValidPathsReply reply;
  checkStatus(stub->QueryValidPaths(&ctx, request, &reply), "QueryValidPaths");
  StorePathSet res;
  for (const auto &path : reply.paths()) {
    res.insert(StorePath(path));
  }
  return res;
}

auto GrpcStore::queryMissing(const std::vector<DerivedPath> & targets)
    -> MissingPaths {
  grpc::ClientContext ctx;
  remote::QueryMissingRequest request;
  for (const auto & target : targets) {
    request.add_targets(target.to_string(*this));
  }
  remote::QueryMissingReply reply;
  auto status = stub->QueryMissing(&ctx, request, &reply);
  if (status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
    // Deliberate: RemoteStore::queryMissing would tunnel, the generic
    // Store walk works with native path queries.
    // NOLINTNEXTLINE(bugprone-parent-virtual-call)
    return Store::queryMissing(targets);
  }
  checkStatus(status, "QueryMissing");
  MissingPaths res;
  for (const auto & path : reply.will_build()) {
    res.willBuild.insert(StorePath(path));
  }
  for (const auto & path : reply.will_substitute()) {
    res.willSubstitute.insert(StorePath(path));
  }
  for (const auto & path : reply.unknown()) {
    res.unknown.insert(StorePath(path));
  }
  res.downloadSize = reply.download_size();
  res.narSize = reply.nar_size();
  return res;
}

auto GrpcStore::queryPathInfosNative(const StorePathSet &paths) -> PathInfoMap {
  remote::QueryPathInfosRequest request;
  for (const auto &path : paths) {
    request.add_paths(std::string(path.to_string()));
  }
  remote::QueryPathInfosReply reply;
  grpc::ClientContext ctx;
  checkStatus(stub->QueryPathInfos(&ctx, request, &reply), "QueryPathInfos");

  PathInfoMap res;
  for (const auto &entry : reply.infos()) {
    res.insert(nixgrpc::decodePathInfo(*this, entry));
  }
  return res;
}

#if NIX_COMPAT_AT_LEAST(2, 35)
auto GrpcStore::topoSortPaths(const StorePathSet &paths) -> StorePaths {
  // copyPaths() hands the source store the full set of paths to copy
  // here, right before querying their info one by one; prefetch them in
  // a single RPC.
  std::map<StorePath, uint64_t> sizes;
  if (!paths.empty()) {
    auto infos = queryPathInfosNative(paths);
    for (const auto & [infoPath, info] : infos) {
      sizes.emplace(infoPath, info->narSize);
    }
    std::scoped_lock const lock(prefetchMutex);
    prefetchedInfos.merge(infos);
  }
  // copyPaths() consumes the sources in reverse topological order.
  // Record it so narFromPath() can pipeline its NAR requests.
  auto sorted = Store::topoSortPaths(paths);
  narFetcher.recordOrder({sorted.rbegin(), sorted.rend()}, std::move(sizes));
  return sorted;
}
#endif

void GrpcStore::runInfoBatches(const std::stop_token & stop)
{
    while (true) {
        std::vector<std::pair<StorePath, InfoCallback>> batch;
        {
            std::unique_lock lock(infoBatchMutex);
            if (!infoBatchWakeup.wait(lock, stop, [&] -> bool { return !infoBatch.empty(); })) {
                return;
            }
            batch.swap(infoBatch);
        }
        StorePathSet paths;
        for (const auto & [path, callback] : batch) {
            paths.insert(path);
        }
        PathInfoMap infos;
        try {
            infos = queryPathInfosNative(paths);
        } catch (...) {
            for (auto & [path, callback] : batch) {
                callback.rethrow();
            }
            continue;
        }
        // Each callback fires exactly once, even if one throws.
        for (auto & [path, callback] : batch) {
            auto found = infos.find(path);
            try {
                callback(found == infos.end() ? nullptr : found->second);
            } catch (...) {
                ignoreExceptionExceptInterrupt();
            }
        }
    }
}

void GrpcStore::queryPathInfoUncached(const StorePath & path, InfoCallback callback) noexcept {
    try {
        {
          std::scoped_lock const lock(prefetchMutex);
          if (auto found = prefetchedInfos.find(path);
              found != prefetchedInfos.end()) {
            auto info = std::move(found->second);
            prefetchedInfos.erase(found);
            callback(std::move(info));
            return;
          }
        }

        {
          std::scoped_lock const lock(infoBatchMutex);
          infoBatch.emplace_back(path, std::move(callback));
          if (!infoBatchWorker.joinable()) {
            infoBatchWorker = std::jthread([this](const std::stop_token & stop) -> void { runInfoBatches(stop); });
          }
        }
        infoBatchWakeup.notify_one();
    } catch (...) {
        callback.rethrow();
    }
}

void GrpcStore::addMultipleToStore(
    PathsSource && pathsToCopy_, Activity & act, RepairFlag repair, CheckSigsFlag checkSigs)
{
    auto pathsToCopy = std::move(pathsToCopy_);
    uint64_t bytesExpected = 0;
    for (auto &[pathInfo, pathSource] : pathsToCopy) {
      bytesExpected += pathInfo.narSize;
    }
    act.setExpected(actCopyPath, bytesExpected);

    grpc::ClientContext ctx;
    remote::AddMultipleReply reply;
    auto writer = stub->AddMultipleToStore(&ctx, &reply);

    try {
        // Flags travel on the first message; everything after is one zstd
        // stream in worker-protocol AddMultipleToStore framing.
        remote::AddMultipleChunk flags;
        flags.set_repair(repair == Repair);
        flags.set_check_sigs(checkSigs == CheckSigs);
        if (!writer->Write(flags)) {
          throw Error("gRPC stream closed by peer");
        }

        nixgrpc::ZstdWriterSink<grpc::ClientWriter<remote::AddMultipleChunk>, remote::AddMultipleChunk> sink(
            *writer);
        size_t const nrTotal = pathsToCopy.size();
        sink << nrTotal;
        // Reverse, so we can release memory at the original start.
        std::ranges::reverse(pathsToCopy);
        while (!pathsToCopy.empty()) {
            act.progress(
                nrTotal - pathsToCopy.size(), nrTotal, static_cast<size_t>(1), static_cast<size_t>(0));
            auto & [pathInfo, pathSource] = pathsToCopy.back();
            WorkerProto::Serialise<ValidPathInfo>::write(
                *this, WorkerProto::WriteConn{.to = sink, .version = nixcompat::infoProtocolVersion()}, pathInfo);
            pathSource->drainInto(sink);
            pathsToCopy.pop_back();
        }
        sink.finish();
        writer->WritesDone();
    } catch (...) {
        // The server-side status usually explains a broken stream better
        // than the local write failure. Finish() half-closes, so the
        // server stops waiting for the remaining paths.
        checkStatus(writer->Finish(), "AddMultipleToStore");
        throw;
    }

    checkStatus(writer->Finish(), "AddMultipleToStore");
}

} // namespace nix
