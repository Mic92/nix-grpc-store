// grpc:// Store: native BuildPaths/BuildDerivation.

#include "store.hh"

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>
#include <nix/util/serialise.hh>

#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "path-info-wire.hh"

namespace nix {

auto GrpcStore::alreadyValidResults(const std::vector<DerivedPath> & reqs)
    -> std::optional<std::vector<KeyedBuildResult>> {
  StorePathSet paths;
  for (const auto & req : reqs) {
    const auto * opaque = std::get_if<DerivedPath::Opaque>(&req.raw());
    if (opaque == nullptr) {
      return std::nullopt;
    }
    paths.insert(opaque->path);
  }
  if (queryValidPaths(paths, NoSubstitute).size() != paths.size()) {
    return std::nullopt;
  }
  std::vector<KeyedBuildResult> results;
  results.reserve(reqs.size());
  for (const auto & req : reqs) {
    KeyedBuildResult res{{}, req};
    nixcompat::setAlreadyValid(res);
    results.push_back(std::move(res));
  }
  return results;
}

auto GrpcStore::dispatchBuild(const std::vector<DerivedPath> & reqs, BuildMode buildMode,
                   const std::shared_ptr<Store> & evalStore)
    -> std::optional<std::vector<KeyedBuildResult>> {
  if (buildMode == bmNormal) {
    if (auto results = alreadyValidResults(reqs)) {
      return results;
    }
  }
  importDrvsFromEvalStore(reqs, evalStore);
  return buildPathsWithResultsNative(reqs, buildMode);
}

auto GrpcStore::buildDerivationNative(const StorePath & drvPath, const BasicDerivation & drv,
                                      BuildMode buildMode) -> BuildResult {
  remote::BuildDerivationRequest request;
  request.set_drv_path(std::string(drvPath.to_string()));
  request.set_build_mode(static_cast<uint32_t>(buildMode));
  request.set_protocol(nixcompat::kBuildProtocolWire);
  {
    StringSink sink;
    nixcompat::writeDrv(sink, *this, drv);
    *request.mutable_drv() = std::move(sink.s);
  }

  grpc::ClientContext ctx;
  auto reader = stub->BuildDerivation(&ctx, request);

  std::optional<BuildResult> res;
  PathInfoMap infos;
  remote::BuildDerivationChunk msg;
  while (reader->Read(&msg)) {
    if (msg.has_log_line()) {
      printError(msg.log_line());
    } else if (msg.has_done()) {
      StringSource source(msg.done().result());
      res = WorkerProto::Serialise<BuildResult>::read(
          *this, WorkerProto::ReadConn{.from = source,
                                       .version = nixcompat::buildProtocolVersion()});
      for (const auto & entry : msg.done().outputs()) {
        infos.insert(nixgrpc::decodePathInfo(*this, entry));
      }
    }
  }
  checkStatus(reader->Finish(), "BuildDerivation");
  if (!res) {
    throw Error("gRPC BuildDerivation stream ended without a result");
  }
  if (!infos.empty()) {
    std::scoped_lock const lock(prefetchMutex);
    prefetchedInfos.merge(infos);
  }
  return std::move(*res);
}

auto GrpcStore::buildPathsWithResultsNative(
    const std::vector<DerivedPath> & reqs, BuildMode buildMode)
    -> std::optional<std::vector<KeyedBuildResult>> {
  remote::BuildPathsRequest request;
  for (const auto & req : reqs) {
    request.add_targets(req.to_string(*this));
  }
  request.set_build_mode(static_cast<uint32_t>(buildMode));
  request.set_protocol(nixcompat::kBuildProtocolWire);

  grpc::ClientContext ctx;
  auto reader = stub->BuildPaths(&ctx, request);

  std::optional<std::vector<KeyedBuildResult>> results;
  PathInfoMap infos;
  remote::BuildPathsChunk msg;
  while (reader->Read(&msg)) {
    if (msg.has_log_line()) {
      printError(msg.log_line());
    } else if (msg.has_done()) {
      StringSource source(msg.done().results());
      results = WorkerProto::Serialise<std::vector<KeyedBuildResult>>::read(
          *this, WorkerProto::ReadConn{.from = source,
                                       .version = nixcompat::buildProtocolVersion()});
      for (const auto & entry : msg.done().outputs()) {
        infos.insert(nixgrpc::decodePathInfo(*this, entry));
      }
    }
  }
  auto status = reader->Finish();
  if (status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
    return std::nullopt;
  }
  checkStatus(status, "BuildPaths");
  if (!results) {
    throw Error("gRPC BuildPaths stream ended without a result");
  }
  if (!infos.empty()) {
    std::scoped_lock const lock(prefetchMutex);
    prefetchedInfos.merge(infos);
  }
  return results;
}

void GrpcStore::importDrvsFromEvalStore(const std::vector<DerivedPath> & paths,
                           const std::shared_ptr<Store> & evalStore) {
  if (evalStore && evalStore.get() != this) {
    StorePathSet drvPaths;
    for (const auto & req : paths) {
      if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
        drvPaths.insert(built->drvPath->getBaseStorePath());
      }
    }
    if (!drvPaths.empty()) {
      copyClosure(*evalStore, *this, drvPaths);
    }
  }
}

void GrpcStore::throwOnFailedBuilds(std::vector<KeyedBuildResult> & results) {
  for (auto & res : results) {
    if (auto errorMsg = nixcompat::buildFailureMsg(res)) {
      throw Error("build of '%s' failed: %s", res.path.to_string(*this),
                  *errorMsg);
    }
  }
}

} // namespace nix
