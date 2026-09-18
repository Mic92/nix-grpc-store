// grpc:// Store: native BuildPaths/BuildDerivation and the build-farm fan-out.

#include "store.hh"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/status.h>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/fmt.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/signals.hh>
#include <nix/util/strings.hh>

#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"

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
  if (isFarm()) {
    return farmBuildPaths(reqs, buildMode, evalStore.get());
  }
  if (buildMode == bmNormal) {
    if (auto results = alreadyValidResults(reqs)) {
      return results;
    }
  }
  importDrvsFromEvalStore(reqs, evalStore);
  return buildPathsWithResultsNative(reqs, buildMode);
}

auto GrpcStore::routingFor(const StorePath & drvPath, const BasicDerivation & drv) -> Metadata {
  return {{"x-nix-drv", std::string(drvPath.hashPart())},
          {"x-nix-system", drv.platform},
          {"x-nix-features", concatStringsSep(",", nixcompat::requiredSystemFeatures(*this, drv))}};
}

void GrpcStore::uploadDrvClosure(Store & evalStore, const StorePath & drvPath, const Metadata & headers) {
  StorePathSet closure;
  evalStore.computeFSClosure(drvPath, closure);
  auto present = queryValidPathsRouted(closure, NoSubstitute, headers);
  StorePathSet missing;
  for (const auto & path : closure) {
    if (!present.contains(path)) {
      missing.insert(path);
    }
  }
  if (missing.empty()) {
    return;
  }
  PathsSource sources;
  // addMultipleToStore wants references before referrers.
  Activity act(*logger, lvlInfo, actCopyPaths,
               fmt("copying %d paths to '%s'", missing.size(), config->authority.to_string()));
  for (const auto & path : evalStore.topoSortPaths(missing) | std::views::reverse) {
    auto info = evalStore.queryPathInfo(path);
    // Same per-path progress activity as Store::copyPaths.
    // NOLINTNEXTLINE(bugprone-exception-escape): libc++ coroutine frames trip this.
    sources.emplace_back(*info, sinkToSource([this, &evalStore, info, parent = act.id](Sink & sink) -> void {
      auto pathS = printStorePath(info->path);
      const std::vector<Logger::Field> fields{pathS, "local", config->authority.to_string()};
      const Activity pathAct(*logger, lvlInfo, actCopyPath,
                             fmt("copying path '%s' to '%s'", pathS, config->authority.to_string()), fields,
                             parent);
      uint64_t sent = 0;
      LambdaSink progress([&](std::string_view data) -> void {
        sent += data.size();
        pathAct.progress(sent, info->narSize);
      });
      TeeSink tee{sink, progress};
      evalStore.narFromPath(info->path, tee);
    }));
  }
  addMultipleToStoreRouted(std::move(sources), act, NoRepair, CheckSigs, headers);
}

auto GrpcStore::tryBuildDerivation(const remote::BuildDerivationRequest & request, const Metadata & headers,
                        std::optional<BuildResult> & res) -> grpc::Status {
  grpc::ClientContext ctx;
  addHeaders(ctx, headers);
  // ^C cancels the stream so the worker stops the build at once.
  auto const onInterrupt = createInterruptCallback([&ctx]() -> void { ctx.TryCancel(); });
  auto reader = stub->BuildDerivation(&ctx, request);

  // build-remote only forwards results of an actBuild activity to nix. Named
  // like nix's own so log UIs merge them, opened lazily so the NOT_FOUND probe is silent.
  auto drv = printStorePath(StorePath(request.drv_path()));
  BuildLogActivity act(fmt("building '%s' on %s", drv, config->authority.to_string()),
                       {drv, config->authority.to_string(), 1, 1});
  return readBuildStream(*reader, act, [&](const remote::BuildDerivationDone & done) -> void {
    StringSource source(done.result());
    res = WorkerProto::Serialise<BuildResult>::read(
        *this, WorkerProto::ReadConn{.from = source, .version = nixcompat::buildProtocolVersion()});
  });
}

auto GrpcStore::buildDerivationNative(const StorePath & drvPath,
                           const BasicDerivation & drv,
                           BuildMode buildMode,
                           Store * evalStore) -> BuildResult {
  remote::BuildDerivationRequest request;
  request.set_drv_path(std::string(drvPath.to_string()));
  request.set_build_mode(static_cast<uint32_t>(buildMode));
  request.set_protocol(nixcompat::kBuildProtocolWire);
  {
    StringSink sink;
    nixcompat::writeDrv(sink, *this, drv);
    *request.mutable_drv() = std::move(sink.s);
  }

  auto headers = routingFor(drvPath, drv);
  std::optional<BuildResult> res;
  grpc::Status status;
  std::shared_ptr<Store> local;
  for (unsigned attempt = 0;; attempt++) {
    status = tryBuildDerivation(request, headers, res);
    if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
      if (evalStore == nullptr) {
        local = localEvalStore({drvPath});
        evalStore = local.get();
      }
      uploadDrvClosure(*evalStore, drvPath, headers);
      status = tryBuildDerivation(request, headers, res);
    }
    // Salt the hash header so a consistent-hashing balancer picks another worker.
    if (status.error_code() != grpc::StatusCode::UNAVAILABLE
        || attempt >= config->unavailableRetries) {
      break;
    }
    printError("%s, retrying elsewhere", firstLine(status.error_message()));
    std::this_thread::sleep_for(std::chrono::seconds(attempt + 1));
    headers.front().second = std::string(drvPath.hashPart()) + "-" + std::to_string(attempt + 1);
  }
  checkStatus(status, "BuildDerivation");
  if (!res) {
    throw Error("gRPC BuildDerivation stream ended without a result");
  }
  return std::move(*res);
}

auto GrpcStore::runFarmJob(FarmJob & job, BuildMode buildMode, Store & evalStore) -> BuildResult {
  using nixcompat::FailureStatus;
  if (job.failedInput) {
    return nixcompat::failed(FailureStatus::DependencyFailed,
                             fmt("dependency '%s' failed", job.failedInput->to_string()));
  }
  if (isInterrupted()) {
    return nixcompat::failed(FailureStatus::MiscFailure, "interrupted");
  }
  try {
    return buildDerivationNative(job.drvPath, job.drv, buildMode, &evalStore);
  } catch (Interrupted &) {
    return nixcompat::failed(FailureStatus::MiscFailure, "interrupted");
  } catch (std::exception & err) {
    // nix reports only the top-level result, so a failed leaf would just read "dependency failed".
    printError("%s: %s", job.drvPath.to_string(), err.what());
    return nixcompat::failed(FailureStatus::MiscFailure, err.what());
  }
}

auto GrpcStore::localEvalStore(const StorePathSet & drvPaths) -> std::shared_ptr<Store> {
  std::shared_ptr<Store> local = nix::openStore();
  if (local.get() == this || !std::ranges::all_of(drvPaths, [&](const StorePath & drv) -> bool {
        return local->isValidPath(drv);
      })) {
    throw Error("'%s' is a build farm\nhint: pass --eval-store auto", config->authority.to_string());
  }
  return local;
}

auto GrpcStore::farmBuildPaths(const std::vector<DerivedPath> & reqs, BuildMode buildMode,
                    Store * evalStore) -> std::vector<KeyedBuildResult> {
  std::shared_ptr<Store> local;
  if (evalStore == nullptr || evalStore == this) {
    StorePathSet drvs;
    for (const auto & req : reqs) {
      if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
        drvs.insert(built->drvPath->getBaseStorePath());
      }
    }
    local = localEvalStore(drvs);
    evalStore = local.get();
  }
  std::map<StorePath, FarmJob> jobs;
  loadFarmJobs(reqs, *evalStore, jobs);

  FarmRun run{.remaining = jobs.size()};
  for (auto & [drvPath, job] : jobs) {
    if (job.waiting == 0) {
      run.ready.push_back(&job);
    }
  }
  auto worker = [&]() -> void {
    while (auto * job = run.next()) {
      run.finish(*job, runFarmJob(*job, buildMode, *evalStore));
    }
  };
  {
    std::vector<std::jthread> threads(std::min<size_t>(config->maxBuilds, jobs.size()));
    for (auto & thread : threads) {
      thread = std::jthread(worker);
    }
  }
  checkInterrupt();

  std::vector<KeyedBuildResult> results;
  results.reserve(reqs.size());
  for (const auto & req : reqs) {
    KeyedBuildResult res{{}, req};
    if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
      auto & job = jobs.at(built->drvPath->getBaseStorePath());
      if (job.result) {
        static_cast<BuildResult &>(res) = std::move(*job.result);
      }
    } else {
      nixcompat::setAlreadyValid(res);
    }
    results.push_back(std::move(res));
  }
  return results;
}

auto GrpcStore::basicForFarm(Store & evalStore, const StorePath & drvPath, const Derivation & full)
    -> BasicDerivation {
  auto basic = nixcompat::toBasicDrv(full, [&](const StorePath & dep, const std::string & output) -> StorePath {
    auto path = evalStore.queryStaticPartialDerivationOutputMap(dep)[output];
    if (!path) {
      throw Error("'%s': input '%s!%s' has no statically known path (floating content-addressed?)",
                  printStorePath(drvPath), printStorePath(dep), output);
    }
    return *path;
  });
  if (!basic || std::ranges::any_of(basic->outputs, [&](const auto & out) -> bool {
        return !out.second.path(*this, basic->name, out.first);
      })) {
    throw Error("'%s': the build farm needs statically known input and output paths "
                "(no floating content-addressed or dynamic derivations)",
                printStorePath(drvPath));
  }
  return std::move(*basic);
}

void GrpcStore::loadFarmJobs(const std::vector<DerivedPath> & reqs, Store & evalStore,
                  std::map<StorePath, FarmJob> & jobs) {
  std::function<FarmJob &(const StorePath &)> load = [&](const StorePath & drvPath) -> FarmJob & {
    if (auto found = jobs.find(drvPath); found != jobs.end()) {
      return found->second;
    }
    auto full = evalStore.readDerivation(drvPath);
    auto & job = jobs.emplace(drvPath, FarmJob{.drvPath = drvPath,
                                               .drv = basicForFarm(evalStore, drvPath, full)})
                     .first->second;
    nixcompat::forInputDrvs(full, [&](const StorePath & input) -> void {
      load(input).dependants.push_back(&job);
      job.waiting++;
    });
    return job;
  };
  for (const auto & req : reqs) {
    if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
      if (const auto * opaque = std::get_if<SingleDerivedPath::Opaque>(&built->drvPath->raw())) {
        load(opaque->path);
      } else {
        throw Error("'%s': dynamic derivations are not supported by the build farm", req.to_string(*this));
      }
    }
  }
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
  BuildLogActivity act(fmt("building %d paths on %s", reqs.size(), config->authority.to_string()), {});
  auto status = readBuildStream(*reader, act, [&](const remote::BuildPathsDone & done) -> void {
    StringSource source(done.results());
    results = WorkerProto::Serialise<std::vector<KeyedBuildResult>>::read(
        *this, WorkerProto::ReadConn{.from = source, .version = nixcompat::buildProtocolVersion()});
  });
  if (status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
    return std::nullopt;
  }
  checkStatus(status, "BuildPaths");
  if (!results) {
    throw Error("gRPC BuildPaths stream ended without a result");
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
