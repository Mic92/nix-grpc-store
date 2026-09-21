// grpc:// Store: builds through the Schedule stream and BuildDerivation.

#include "store.hh"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/build-result.hh>
#include <nix/store/derivations.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path-info.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/finally.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/fmt.hh>
#include <nix/util/logging.hh>
#include <nix/util/serialise.hh>
#include <nix/util/signals.hh>
#include <nix/util/strings.hh>

#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"

namespace nix {

auto GrpcStore::routingFor(const BasicDerivation & drv, const std::string & workerAddr) -> Metadata {
  Metadata meta{{"x-nix-system", drv.platform},
                {"x-nix-features", concatStringsSep(",", nixcompat::requiredSystemFeatures(*this, drv))}};
  if (!workerAddr.empty()) {
    meta.emplace_back("x-nix-worker", workerAddr);
  }
  return meta;
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

  // Named like nix's own actBuild so log UIs merge them, opened lazily so the NOT_FOUND probe is silent.
  auto drv = printStorePath(StorePath(request.drv_path()));
  BuildLogActivity act(fmt("building '%s' on %s", drv, config->authority.to_string()),
                       {drv, config->authority.to_string(), 1, 1});
  return readBuildStream(*reader, act, [&](const remote::BuildDerivationDone & done) -> void {
    StringSource source(done.result());
    res = WorkerProto::Serialise<BuildResult>::read(
        *this, WorkerProto::ReadConn{.from = source, .version = nixcompat::buildProtocolVersion()});
  });
}

auto GrpcStore::buildAssigned(Job & job, BuildMode buildMode, Store & evalStore) -> std::optional<BuildResult> {
  remote::BuildDerivationRequest request;
  request.set_drv_path(std::string(job.drvPath.to_string()));
  request.set_build_mode(static_cast<uint32_t>(buildMode));
  request.set_protocol(nixcompat::kBuildProtocolWire);
  request.set_assign_id(job.assignId);
  {
    StringSink sink;
    nixcompat::writeDrv(sink, *this, job.drv);
    *request.mutable_drv() = std::move(sink.s);
  }
  auto headers = routingFor(job.drv, job.workerAddr);
  std::optional<BuildResult> res;
  auto status = tryBuildDerivation(request, headers, res);
  if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
    uploadDrvClosure(evalStore, job.drvPath, headers);
    status = tryBuildDerivation(request, headers, res);
  }
  switch (status.error_code()) {
  case grpc::StatusCode::ABORTED: // superseded: the scheduler already sent where to go
    return std::nullopt;
  case grpc::StatusCode::FAILED_PRECONDITION: // not expected there (any more)
  case grpc::StatusCode::UNAVAILABLE:         // worker draining or gone
  case grpc::StatusCode::UNKNOWN:             // worker died mid-stream ("Stream removed"),
  case grpc::StatusCode::INTERNAL:            //   or RST_STREAM via the balancer
    printError("%s: %s, asking the scheduler again", job.drvPath.to_string(), firstLine(status.error_message()));
    return std::nullopt;
  default:
    checkStatus(status, "BuildDerivation");
  }
  if (!res) {
    throw Error("gRPC BuildDerivation stream ended without a result");
  }
  return res;
}

auto GrpcStore::localEvalStore(const StorePathSet & drvPaths) -> std::shared_ptr<Store> {
  std::shared_ptr<Store> local = nix::openStore();
  if (local.get() == this || !std::ranges::all_of(drvPaths, [&](const StorePath & drv) -> bool {
        return local->isValidPath(drv);
      })) {
    throw Error("'%s' builds from derivations in a local store\nhint: pass --eval-store auto",
                config->authority.to_string());
  }
  return local;
}

// Shared between the Schedule reader and the build threads.
struct GrpcStore::Run {
  GrpcStore & store;
  BuildMode mode;
  Store & evalStore;
  std::map<StorePath, Job> & jobs;

  std::mutex mutex;
  std::condition_variable cv;
  std::deque<Job *> ready;    // assigned, cached, lost or dep-failed: a thread can finish it
  std::deque<Job *> wantable; // inputs done, Want not yet sent
  size_t remaining;
  bool closed = false; // WritesDone sent
  grpc::ClientReaderWriter<remote::ClientMsgs, remote::SchedMsgs> * stream = nullptr;
  std::map<std::string, Job *, std::less<>> byName;

  auto next() -> Job * {
    std::unique_lock lock(mutex);
    cv.wait(lock, [&]() -> bool { return !ready.empty() || remaining == 0; });
    if (ready.empty()) {
      return nullptr;
    }
    auto * job = ready.front();
    ready.pop_front();
    return job;
  }

  void fillWant(remote::Want * want, const Job & job) const {
    want->set_drv_path(std::string(job.drvPath.to_string()));
    for (const auto & [name, out] : job.drv.outputs) {
      if (auto path = out.path(store, job.drv.name, name)) {
        want->add_out_keys(std::string(path->hashPart()) + ".narinfo");
      }
    }
    for (const auto & input : nixcompat::drvInputs(job.drv)) {
      want->add_inputs(store.printStorePath(input));
    }
    for (const auto & feat : nixcompat::requiredSystemFeatures(store, job.drv)) {
      want->add_required_features(feat);
    }
    want->set_cp_hint_ms(job.cpHintMs);
    want->set_build_mode(static_cast<uint32_t>(mode));
    want->set_system(job.drv.platform);
    debug("grpc: want %s", job.drvPath.to_string());
  }

  // Under `mutex`. Everything wantable goes out as one stream message. With
  // no stream it stays wantable until reconnect or give-up.
  void flushWants() {
    remote::ClientMsgs batch;
    std::deque<Job *> keep;
    for (auto * job : wantable) {
      if (job->failedInput) {
        ready.push_back(job);
      } else if (stream == nullptr) {
        keep.push_back(job);
      } else {
        fillWant(batch.add_msgs()->mutable_want(), *job);
        keep.push_back(job); // dropped below once the Write succeeded
      }
    }
    wantable.swap(keep);
    if (batch.msgs_size() > 0) {
      if (stream->Write(batch)) {
        wantable.clear();
      } else {
        stream = nullptr;
      }
    }
    if (remaining == 0 && stream != nullptr && !closed) {
      closed = true;
      stream->WritesDone();
    }
    cv.notify_all();
  }

  // Under `mutex`. Releases dependants: they become wantable (or dep-failed => ready).
  void finishLocked(Job & job, BuildResult res) {
    bool const succeeded = nixcompat::succeeded(res);
    job.result = std::move(res);
    assert(remaining > 0);
    remaining--;
    for (auto * dep : job.dependants) {
      if (!succeeded && !dep->failedInput) {
        dep->failedInput = job.drvPath;
      }
      assert(dep->waiting > 0);
      if (--dep->waiting == 0) {
        wantable.push_back(dep);
      }
    }
    flushWants();
  }

  // Under `mutex`. Bounced by the worker: hand back to the scheduler, or
  // follow a redirect it already sent.
  void requeueLocked(Job & job) {
    job.workerAddr = std::exchange(job.redirectAddr, {});
    job.assignId = std::exchange(job.redirectId, 0);
    if (!job.workerAddr.empty()) {
      ready.push_back(&job);
      cv.notify_all();
      return;
    }
    if (++job.bounced > store.config->rescheduleRetries) {
      finishLocked(job, nixcompat::failed(nixcompat::FailureStatus::MiscFailure,
                                          fmt("gave up after %d reschedules", job.bounced)));
    } else {
      wantable.push_back(&job);
      flushWants();
    }
  }

  auto resultFor(Job & job) -> std::optional<BuildResult> {
    using nixcompat::FailureStatus;
    if (job.failedInput) {
      return nixcompat::failed(FailureStatus::DependencyFailed,
                               fmt("dependency '%s' failed", job.failedInput->to_string()));
    }
    if (isInterrupted()) {
      return nixcompat::failed(FailureStatus::MiscFailure, "interrupted");
    }
    if (job.lost) {
      return nixcompat::failed(FailureStatus::MiscFailure, "scheduler connection lost");
    }
    if (job.result) { // Cached / Unplaceable
      return std::exchange(job.result, std::nullopt);
    }
    return store.buildAssigned(job, mode, evalStore);
  }

  // Build thread body.
  void work() {
    while (auto * job = next()) {
      std::optional<BuildResult> res;
      try {
        res = resultFor(*job);
      } catch (Interrupted &) {
        res = nixcompat::failed(nixcompat::FailureStatus::MiscFailure, "interrupted");
      } catch (std::exception & err) {
        // nix reports only the top-level result, so a failed leaf would just read "dependency failed".
        printError("%s: %s", job->drvPath.to_string(), err.what());
        res = nixcompat::failed(nixcompat::FailureStatus::MiscFailure, err.what());
      }
      std::scoped_lock const lock(mutex);
      if (res) {
        finishLocked(*job, std::move(*res));
      } else {
        requeueLocked(*job);
      }
    }
  }

  static auto drvOf(const remote::SchedMsg & msg) -> std::string {
    switch (msg.msg_case()) {
    case remote::SchedMsg::kAssigned:
      return msg.assigned().drv_path();
    case remote::SchedMsg::kCached:
      return msg.cached().drv_path();
    case remote::SchedMsg::kUnplaceable:
      return msg.unplaceable().drv_path();
    default:
      return {};
    }
  }

  // Input-addressed only (checked in buildPaths), so every output has a path.
  [[nodiscard]] auto cachedResult(const Job & job) const -> BuildResult {
    std::map<std::string, StorePath> outs;
    for (const auto & [outName, out] : job.drv.outputs) {
      if (auto path = out.path(store, job.drv.name, outName)) {
        outs.emplace(outName, *path);
      }
    }
    return nixcompat::alreadyValid(job.drvPath, std::move(outs));
  }

  // A scheduler decision. Ignores anything stale or unknown.
  void onMsgs(const remote::SchedMsgs & msgs) {
    std::scoped_lock const lock(mutex);
    for (const auto & msg : msgs.msgs()) {
      onMsgLocked(msg);
    }
    cv.notify_all();
  }

  void onMsgLocked(const remote::SchedMsg & msg) {
    auto name = drvOf(msg);
    auto found = byName.find(name);
    Job * job = found == byName.end() ? nullptr : found->second;
    if (job == nullptr || job->result) {
      return;
    }
    if (!job->workerAddr.empty()) {
      if (msg.has_assigned() && msg.assigned().worker_addr() != job->workerAddr) {
        job->redirectAddr = msg.assigned().worker_addr();
        job->redirectId = msg.assigned().assign_id();
      }
      return;
    }
    if (msg.has_assigned()) {
      debug("grpc: assigned %s -> %s", name, msg.assigned().worker_addr());
      if (msg.assigned().worker_addr().empty()) {
        return;
      }
      job->workerAddr = msg.assigned().worker_addr();
      job->assignId = msg.assigned().assign_id();
    } else if (msg.has_cached()) {
      job->result = cachedResult(*job);
    } else {
      if (!job->unplaceableSince) {
        warn("%s: %s, waiting up to %ds for a worker", job->drvPath.to_string(), msg.unplaceable().reason(),
             store.restartGrace().count());
        job->unplaceableSince = std::chrono::steady_clock::now();
      }
      job->unplaceableReason = msg.unplaceable().reason();
      cv.notify_all();
      return;
    }
    job->unplaceableSince.reset();
    ready.push_back(job);
  }

  // Thread body: fail jobs whose Unplaceable outlived the grace.
  void reapUnplaceable(const std::stop_token & stop) {
    std::unique_lock lock(mutex);
    while (!stop.stop_requested() && remaining > 0) {
      auto const now = std::chrono::steady_clock::now();
      auto wake = now + store.restartGrace();
      for (auto & [drvPath, job] : jobs) {
        if (!job.unplaceableSince || job.result || !job.workerAddr.empty()) {
          continue;
        }
        auto const due = *job.unplaceableSince + store.restartGrace();
        if (due <= now) {
          // nix reports only the top-level result. Say why a leaf failed.
          printError("%s: %s", job.drvPath.to_string(), job.unplaceableReason);
          job.unplaceableSince.reset();
          job.result = nixcompat::failed(nixcompat::FailureStatus::MiscFailure, job.unplaceableReason);
          ready.push_back(&job);
          cv.notify_all();
        } else {
          wake = std::min(wake, due);
        }
      }
      cv.wait_until(lock, wake);
    }
  }

  // A fresh stream: everything not yet placed is Wanted again.
  void attach(grpc::ClientReaderWriter<remote::ClientMsgs, remote::SchedMsgs> * fresh) {
    std::scoped_lock const lock(mutex);
    stream = fresh;
    closed = false;
    for (auto & [drvPath, job] : jobs) {
      bool const pending = !job.result && !job.failedInput && !job.lost && job.workerAddr.empty() && job.waiting == 0;
      if (pending && std::ranges::find(wantable, &job) == wantable.end()
          && std::ranges::find(ready, &job) == ready.end()) {
        wantable.push_back(&job);
      }
    }
    flushWants();
  }

  auto detach() -> size_t {
    std::scoped_lock const lock(mutex);
    stream = nullptr;
    return remaining;
  }

  // No scheduler any more: jobs a thread already holds finish, the rest fail.
  void abandon() {
    std::scoped_lock const lock(mutex);
    for (auto & [drvPath, job] : jobs) {
      if (job.result || job.failedInput || job.lost || !job.workerAddr.empty() || job.waiting > 0) {
        continue;
      }
      job.lost = true;
      std::erase(wantable, &job);
      if (std::ranges::find(ready, &job) == ready.end()) {
        ready.push_back(&job);
      }
    }
    flushWants();
  }
};

// A dead scheduler (or the balancer in front losing it) shows as UNAVAILABLE,
// or as UNKNOWN/INTERNAL "Stream removed" when it dies mid-stream. Config
// errors (auth, TLS, bad request) are not worth reconnecting for.
namespace {
auto schedulerGone(const grpc::Status & status) -> bool {
  switch (status.error_code()) {
  case grpc::StatusCode::UNAVAILABLE:
  case grpc::StatusCode::UNKNOWN:
  case grpc::StatusCode::INTERNAL:
    // "tcp handshaker shutdown" is a plain connect timeout, not TLS.
    return GrpcStore::transportError(status.error_message())
           || (!status.error_message().contains("andshake") && !status.error_message().contains("certificate"));
  default:
    return false;
  }
}

struct StreamEnd {
  bool answered = false;   // got at least one message
  bool restarting = false; // the scheduler announced it and we closed the stream
};

// Pump one Schedule stream into `run` until it closes.
template <typename Stream, typename Run>
auto readSchedule(Stream & stream, Run & run, grpc::ClientContext & ctx) -> StreamEnd {
  StreamEnd end;
  remote::SchedMsgs msgs;
  while (stream.Read(&msgs)) {
    end.answered = true;
    run.onMsgs(msgs);
    if (std::ranges::any_of(msgs.msgs(), &remote::SchedMsg::has_restarting)) {
      // Leave now. A scheduler that yields to another keeps the connection open.
      end.restarting = true;
      ctx.TryCancel();
    }
  }
  return end;
}

void logReconnect(const std::string & where, const grpc::Status & status, bool restarting) {
  if (restarting) {
    // Announced restart: not worth a red line.
    debug("grpc: scheduler at %s restarting, reconnecting", where);
  } else {
    printError("scheduler at %s: %s, reconnecting", where, GrpcStore::firstLine(status.error_message()));
  }
}
} // namespace

// The scheduler holds soft state only: on a broken stream reconnect and
// re-Want everything not yet placed. Jobs already Assigned carry on.
auto GrpcStore::scheduleUntilDone(Run & run, const Metadata & headers,
                                  const std::function<void(grpc::ClientContext *)> & setCtx) -> grpc::Status {
  grpc::Status status;
  auto giveUp = std::chrono::steady_clock::time_point::max();
  auto pause = reconnectPause;
  for (;;) {
    grpc::ClientContext ctx;
    addHeaders(ctx, headers);
    setCtx(&ctx);
    auto stream = sched->Schedule(&ctx);
    run.attach(stream.get());
    debug("grpc: Schedule stream open, %d jobs", run.jobs.size());
    auto const [answered, restarting] = readSchedule(*stream, run, ctx);
    status = stream->Finish();
    setCtx(nullptr);
    if (run.detach() == 0 || isInterrupted() || !(restarting || schedulerGone(status))) {
      return status;
    }
    auto const now = std::chrono::steady_clock::now();
    if (answered || giveUp == std::chrono::steady_clock::time_point::max()) {
      giveUp = now + restartGrace();
      pause = reconnectPause;
    }
    if (now + pause > giveUp) {
      return status;
    }
    logReconnect(config->authority.to_string(), status, restarting);
    std::this_thread::sleep_for(pause);
    pause = std::min(pause * 2, maxReconnectPause);
  }
}

void GrpcStore::runJobs(std::map<StorePath, Job> & jobs, BuildMode buildMode, Store & evalStore) {
  if (jobs.empty()) {
    return;
  }
  Run run{.store = *this, .mode = buildMode, .evalStore = evalStore, .jobs = jobs, .remaining = jobs.size()};
  for (auto & [drvPath, job] : jobs) {
    run.byName.emplace(std::string(drvPath.to_string()), &job);
    if (job.waiting == 0) {
      run.wantable.push_back(&job);
    }
  }
  auto headers = routingFor(jobs.begin()->second.drv);
  std::mutex ctxMutex; // not run.mutex: the interrupt callback must not wait on stream I/O
  grpc::ClientContext * liveCtx = nullptr;
  auto setCtx = [&](grpc::ClientContext * ctx) -> void {
    std::scoped_lock const lock(ctxMutex);
    liveCtx = ctx;
  };
  auto const onInterrupt = createInterruptCallback([&]() -> void {
    std::scoped_lock const lock(ctxMutex);
    if (liveCtx != nullptr) {
      liveCtx->TryCancel();
    }
  });

  std::vector<std::jthread> threads(std::min<size_t>(config->maxBuilds, jobs.size()));
  for (auto & thread : threads) {
    thread = std::jthread([&run]() -> void { run.work(); });
  }
  std::jthread reaper([&run](const std::stop_token & stop) -> void { run.reapUnplaceable(stop); });
  std::stop_callback const wakeReaper(reaper.get_stop_token(), [&run]() -> void { run.cv.notify_all(); });
  // Also on unwind: lets the threads drain so the join in ~jthread returns.
  Finally const abandon([&run]() -> void { run.abandon(); });

  auto status = scheduleUntilDone(run, headers, setCtx);
  run.abandon();
  reaper.request_stop();
  threads.clear(); // join
  checkInterrupt();
  if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED && run.remaining > 0) {
    throw statusError(status, "Schedule");
  }
}

auto GrpcStore::dispatchBuild(const std::vector<DerivedPath> & reqs, BuildMode buildMode,
                   const std::shared_ptr<Store> & evalStoreIn) -> std::vector<KeyedBuildResult> {
  Store * evalStore = evalStoreIn.get();
  std::shared_ptr<Store> local;
  if (evalStore == nullptr || evalStore == this) {
    StorePathSet drvs;
    for (const auto & req : reqs) {
      if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
        drvs.insert(built->drvPath->getBaseStorePath());
      }
    }
    if (!drvs.empty()) {
      local = localEvalStore(drvs);
      evalStore = local.get();
    }
  }
  std::map<StorePath, Job> jobs;
  if (evalStore != nullptr) {
    loadJobs(reqs, *evalStore, jobs);
    runJobs(jobs, buildMode, *evalStore);
  }

  // Opaque paths: "building" them means making them valid, i.e. substituting.
  StorePathSet opaque;
  for (const auto & req : reqs) {
    if (const auto * opq = std::get_if<DerivedPath::Opaque>(&req.raw())) {
      opaque.insert(opq->path);
    }
  }
  auto validOpaque = opaque.empty() ? StorePathSet{} : queryValidPaths(opaque, Substitute);

  std::vector<KeyedBuildResult> results;
  results.reserve(reqs.size());
  for (const auto & req : reqs) {
    KeyedBuildResult res{{}, req};
    if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
      auto & job = jobs.at(built->drvPath->getBaseStorePath());
      if (job.result) {
        static_cast<BuildResult &>(res) = *job.result;
      }
    } else if (validOpaque.contains(std::get<DerivedPath::Opaque>(req.raw()).path)) {
      nixcompat::setAlreadyValid(res);
    } else {
      static_cast<BuildResult &>(res) = nixcompat::failed(
          nixcompat::FailureStatus::MiscFailure,
          fmt("path '%s' does not exist and cannot be substituted", req.to_string(*this)));
    }
    results.push_back(std::move(res));
  }
  return results;
}

auto GrpcStore::buildOne(const StorePath & drvPath, const BasicDerivation & drv, BuildMode buildMode,
              Store * evalStore) -> BuildResult {
  std::shared_ptr<Store> local;
  if (evalStore == nullptr || evalStore == this) {
    local = localEvalStore({drvPath});
    evalStore = local.get();
  }
  std::map<StorePath, Job> jobs;
  jobs.emplace(drvPath, Job{.drvPath = drvPath, .drv = drv});
  runJobs(jobs, buildMode, *evalStore);
  auto & job = jobs.begin()->second;
  if (!job.result) {
    throw Error("'%s' was not built", printStorePath(drvPath));
  }
  return std::move(*job.result);
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
    throw Error("'%s': needs statically known input and output paths "
                "(no floating content-addressed or dynamic derivations)",
                printStorePath(drvPath));
  }
  return std::move(*basic);
}

// Inputs whose outputs the farm already has or can substitute are not
// jobs: the worker fetches them when it builds the dependant. One
// QueryValidPaths for the whole closure instead of a Want per drv.
// NoSubstitute: a farm node also reports what its substituters offer,
// without fetching it just to answer.
auto GrpcStore::validInputDrvs(const std::vector<DerivedPath> & reqs, Store & evalStore) -> StorePathSet {
  std::map<StorePath, Derivation> drvs;
  std::function<void(const StorePath &)> collect = [&](const StorePath & drvPath) -> void {
    if (drvs.contains(drvPath)) {
      return;
    }
    auto & full = drvs.emplace(drvPath, evalStore.readDerivation(drvPath)).first->second;
    nixcompat::forInputDrvs(full, collect);
  };
  StorePathSet tops;
  for (const auto & req : reqs) {
    if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
      tops.insert(built->drvPath->getBaseStorePath());
      collect(built->drvPath->getBaseStorePath());
    }
  }
  StorePathSet outputs;
  std::map<StorePath, std::vector<StorePath>> outputsOf;
  for (auto & [drvPath, full] : drvs) {
    if (tops.contains(drvPath)) {
      continue;
    }
    for (auto & [name, path] : evalStore.queryStaticPartialDerivationOutputMap(drvPath)) {
      if (path) {
        outputs.insert(*path);
        outputsOf[drvPath].push_back(*path);
      }
    }
  }
  auto present = outputs.empty() ? StorePathSet{} : queryValidPaths(outputs, NoSubstitute);
  StorePathSet valid;
  for (auto & [drvPath, outs] : outputsOf) {
    if (!outs.empty() && std::ranges::all_of(outs, [&](auto & out) -> bool { return present.contains(out); })) {
      valid.insert(drvPath);
    }
  }
  return valid;
}

void GrpcStore::loadJobs(const std::vector<DerivedPath> & reqs, Store & evalStore,
                  std::map<StorePath, Job> & jobs) {
  auto valid = validInputDrvs(reqs, evalStore);
  std::function<Job &(const StorePath &)> load = [&](const StorePath & drvPath) -> Job & {
    if (auto found = jobs.find(drvPath); found != jobs.end()) {
      return found->second;
    }
    auto full = evalStore.readDerivation(drvPath);
    auto & job = jobs.emplace(drvPath, Job{.drvPath = drvPath,
                                           .drv = basicForFarm(evalStore, drvPath, full)})
                     .first->second;
    nixcompat::forInputDrvs(full, [&](const StorePath & input) -> void {
      if (valid.contains(input)) {
        return;
      }
      auto & dep = load(input);
      dep.dependants.push_back(&job);
      job.inputs.push_back(&dep);
      job.waiting++;
    });
    return job;
  };
  for (const auto & req : reqs) {
    if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
      if (const auto * opaque = std::get_if<SingleDerivedPath::Opaque>(&built->drvPath->raw())) {
        load(opaque->path);
      } else {
        throw Error("'%s': dynamic derivations are not supported", req.to_string(*this));
      }
    }
  }
  // cp_hint: longest chain of dependants above each job, 1 s per drv.
  constexpr uint64_t perDrvMs = 1000;
  std::function<uint64_t(Job &)> height = [&](Job & job) -> uint64_t {
    if (job.cpHintMs != 0) {
      return job.cpHintMs;
    }
    uint64_t above = 0;
    for (auto * dep : job.dependants) {
      above = std::max(above, height(*dep));
    }
    return job.cpHintMs = above + perDrvMs;
  };
  for (auto & [path, job] : jobs) {
    height(job);
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
