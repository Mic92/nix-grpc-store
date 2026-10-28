// grpc:// Store: builds through the Schedule stream and BuildDerivation.

#include "store.hh"

#include <algorithm>
#include <cassert>
#ifdef NIX_GRPC_TEST_REVERSE_RETRY
#include <atomic>
std::atomic<unsigned> reverseProbeNotFound{0};
std::atomic<unsigned> reverseClosureUploaded{0};
#endif
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
#include <set>
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

#include "channel.hh"
#include "executor.hh"
#include "nix-compat.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "retry.hh"
#include "top-down.hh"

namespace nix {

// The balancer routes on the system. The worker's own beats the drv's, which
// may be `builtin` or an extra platform only some workers offer.
auto GrpcStore::routingFor(const BasicDerivation & drv, const std::string & workerAddr, const std::string & workerSystem)
    -> Metadata {
  Metadata meta{{"x-nix-system", workerSystem.empty() ? drv.platform : workerSystem},
                {"x-nix-features", concatStringsSep(",", nixcompat::requiredSystemFeatures(*this, drv))}};
  if (!workerAddr.empty()) {
    meta.emplace_back("x-nix-worker", workerAddr);
  }
    meta.emplace_back("x-nix-session", session);
  return meta;
}

auto GrpcStore::uploadDrvClosure(Store & evalStore, std::vector<StorePath> drvPaths, const Metadata & headers,
                                 std::optional<std::pair<StorePath, uint64_t>> reverseAssignment) -> grpc::Status {
  // The scheduler's store cannot answer what a reverse worker has locally.
  // Upload the entire closure to the pinned worker; duplicate paths are
  // handled by the worker importer. Never query/transfer through a direct
  // worker or the scheduler store for this assignment. The worker still
  // verifies signatures for write-role clients, as for direct uploads.
  StorePathSet missing;
  if (reverseAssignment) {
    for (auto const & drvPath : drvPaths) evalStore.computeFSClosure(drvPath, missing);
  } else {
    missing = nixgrpc::missingTopDown(
        std::move(drvPaths),
        [](const StorePath & path) -> std::vector<StorePath> { return {path}; },
        [&](const StorePath & path) -> StorePathSet { return evalStore.queryPathInfo(path)->references; },
        [&](const StorePathSet & paths) -> StorePathSet { return queryValidPathsRouted(paths, NoSubstitute, headers); });
  }
  if (missing.empty()) {
    return grpc::Status::OK;
  }
  PathsSource sources;
  // addMultipleToStore wants references before referrers.
  Activity act(*logger, lvlInfo, actCopyPaths,
               fmt("copying %d paths to '%s'", missing.size(), config->authority.to_string()));
  for (const auto & path : evalStore.topoSortPaths(missing) | std::views::reverse) {
    auto info = evalStore.queryPathInfo(path);
    // NOLINTNEXTLINE(bugprone-exception-escape): libc++ coroutine frames trip this.
    sources.emplace_back(*info, sinkToSource([this, &evalStore, info, parent = act.id](Sink & sink) -> void {
      auto const pathS = printStorePath(info->path);
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
  if (!reverseAssignment) {
    addMultipleToStoreRouted(std::move(sources), act, NoRepair, CheckSigs, headers);
    return grpc::Status::OK;
  }
  std::vector<std::pair<ValidPathInfo, ReplayableNar>> paths;
  paths.reserve(sources.size());
  auto const spool = std::make_shared<UploadSpool>();
  for (auto & [info, nar] : sources) paths.emplace_back(info, ReplayableNar(std::move(nar), spool, info.narSize));
  return addMultipleToStoreOnce(paths, act, NoRepair, CheckSigs, headers,
                                std::pair{std::string(reverseAssignment->first.to_string()), reverseAssignment->second});
}

auto GrpcStore::tryBuildDerivation(const remote::BuildDerivationRequest & request, const Metadata & headers,
                        std::optional<BuildResult> & res, std::string & missingInput) -> grpc::Status {
  // ^C cancels the stream so the worker stops the build at once.
  nixgrpc::Call call;
  addHeaders(call.ctx(), headers);
  auto reader = stub->BuildDerivation(&call.ctx(), request);

  // Named like nix's own actBuild so log UIs merge them, opened lazily so the NOT_FOUND probe is silent.
  auto const drv = printStorePath(StorePath(request.drv_path()));
  BuildLogActivity act(fmt("building '%s' on %s", drv, config->authority.to_string()),
                       {drv, config->authority.to_string(), 1, 1});
  auto status = readBuildStream(*reader, act, [&](const remote::BuildDerivationDone & done) -> void {
    StringSource source(done.result());
    res = WorkerProto::Serialise<BuildResult>::read(
        *this, WorkerProto::ReadConn{.from = source, .version = nixcompat::buildProtocolVersion()});
  });
  const auto & trailers = call.ctx().GetServerTrailingMetadata();
  if (auto const found = trailers.find("x-nix-missing-input"); found != trailers.end()) {
    missingInput.assign(found->second.data(), found->second.size());
  }
  return status;
}

auto GrpcStore::buildAssigned(const Job & job, const nixgrpc::exec::Placement & where, BuildMode buildMode,
                              Store & evalStore) -> Attempt {
  remote::BuildDerivationRequest request;
  request.set_drv_path(std::string(job.drvPath.to_string()));
  request.set_build_mode(static_cast<uint32_t>(buildMode));
  request.set_protocol(nixcompat::kBuildProtocolWire);
  request.set_assign_id(where.assignId);
  request.set_build_timeout(static_cast<uint64_t>(nixcompat::buildTimeout()));
  request.set_max_silent_time(static_cast<uint64_t>(nixcompat::maxSilentTime()));
  {
    StringSink sink;
    nixcompat::writeDrv(sink, *this, job.drv);
    *request.mutable_drv() = std::move(sink.s);
  }
  auto headers = routingFor(job.drv, where.reverse ? "" : where.addr, where.system);
  if (where.reverse) {
    headers.emplace_back("x-nix-reverse", "1"); // Envoy route only; server uses the live assignment.
  }
  std::optional<BuildResult> res;
  std::string missing;
  auto status = tryBuildDerivation(request, headers, res, missing);
  if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
#ifdef NIX_GRPC_TEST_REVERSE_RETRY
    if (where.reverse) ++reverseProbeNotFound;
#endif
    // Opt-in while reverse workers are new.
    if (where.reverse && !config->reverseUploads) {
      throw Error("reverse worker is missing build inputs; set the store setting reverse-uploads=true to send them");
    }
    auto upload = uploadDrvClosure(evalStore, {job.drvPath}, headers,
                                   where.reverse ? std::optional(std::pair{job.drvPath, where.assignId}) : std::nullopt);
    if (!upload.ok()) {
      checkInterrupt();
      checkStatus(upload, where.reverse ? "UploadBuildInputs" : "AddMultipleToStore");
    }
#ifdef NIX_GRPC_TEST_REVERSE_RETRY
    if (where.reverse) ++reverseClosureUploaded;
#endif
    status = tryBuildDerivation(request, headers, res, missing);
  }
  // Our own cancel ends the stream CANCELLED, and a dead worker looks the same.
  if (!status.ok()) {
    checkInterrupt();
  }
  switch (status.error_code()) {
  case grpc::StatusCode::FAILED_PRECONDITION: // not expected there (any more), or an input missing
    if (!missing.empty()) {
      return MissingInput{.path = parseStorePath(missing)};
    }
    [[fallthrough]];
  case grpc::StatusCode::ABORTED:             // superseded: the scheduler already sent where to go
  case grpc::StatusCode::UNAVAILABLE:         // worker gone or failing for a while
  case grpc::StatusCode::CANCELLED:           // worker shut down (ours was checked above)
  case grpc::StatusCode::UNKNOWN:             // worker died mid-stream ("Stream removed"),
  case grpc::StatusCode::INTERNAL:            //   or RST_STREAM via the balancer
    if (status.error_code() != grpc::StatusCode::ABORTED) {
      printError("%s: %s, asking the scheduler again", job.drvPath.to_string(), firstLine(status.error_message()));
    }
    return Bounced{};
  default:
    checkStatus(status, "BuildDerivation");
  }
  if (!res) {
    throw Error("gRPC BuildDerivation stream ended without a result");
  }
  return std::move(*res);
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

// Wires the executor to the Schedule stream and the build threads.
struct GrpcStore::Run {
  using JobId = nixgrpc::exec::JobId;

  GrpcStore & store;
  BuildMode mode;
  Store & evalStore;
  std::map<StorePath, Job> & jobs;

  std::mutex mutex;
  std::condition_variable cv;
  nixgrpc::exec::Executor exe;
  std::deque<Job *> byId; // deque: build threads keep references while jobs are added
  std::map<std::string, JobId, std::less<>> byName;
  std::set<StorePath> recovered; // missing inputs, each handled once
  bool closed = false;           // WritesDone sent
  grpc::ClientReaderWriter<remote::ClientMsgs, remote::SchedMsgs> * stream = nullptr;

  Run(GrpcStore & store, BuildMode mode, Store & evalStore, std::map<StorePath, Job> & jobs, size_t threads)
      : store(store), mode(mode), evalStore(evalStore), jobs(jobs),
        exe({.threads = threads,
             .rescheduleRetries = store.config->rescheduleRetries,
             .keepGoing = nixcompat::keepGoing()}) {
        std::function<JobId(Job &)> add = [&](Job & job) -> JobId {
      auto const name = std::string(job.drvPath.to_string());
      if (auto const found = byName.find(name); found != byName.end()) {
        return found->second;
      }
      std::vector<JobId> inputs;
      inputs.reserve(job.inputs.size());
      for (auto * input : job.inputs) {
        inputs.push_back(add(*input));
      }
      return addLocked(job, inputs);
    };
    for (auto & [drvPath, job] : jobs) {
      add(job);
    }
  }

  auto addLocked(Job & job, const std::vector<JobId> & inputs) -> JobId {
    auto const jid = exe.add(static_cast<double>(job.cpHintMs), inputs);
    byId.push_back(&job);
    byName.emplace(std::string(job.drvPath.to_string()), jid);
    return jid;
  }

  struct Work {
    JobId jid;
    Job * job;
    nixgrpc::exec::Placement where;
  };

  auto next() -> std::optional<Work> {
    std::unique_lock lock(mutex);
    for (;;) {
      cv.wait(lock, [&] -> bool { return exe.hasReady() || exe.remainingJobs() == 0; });
      if (auto job = exe.next()) {
        return Work{.jid = job->first, .job = byId.at(job->first), .where = std::move(job->second)};
      }
      if (exe.remainingJobs() == 0) {
        return std::nullopt;
      }
    }
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

  // Under `mutex`. Batches stay below the server's receive limit.
  void flushLocked() {
    static constexpr size_t maxBatchBytes = 4UL << 20U;
    auto const cancels = exe.takeCancels();
    auto const wants = exe.takeWants();
    remote::ClientMsgs batch;
    size_t bytes = 0;
    std::vector<JobId> sent;
    auto const write = [&] -> void {
      if (stream != nullptr && batch.msgs_size() > 0 && !stream->Write(batch)) {
        stream = nullptr;
        exe.disconnected();
      }
      if (stream == nullptr) {
        exe.unsent(sent);
      }
      batch.Clear();
      bytes = 0;
      sent.clear();
    };
    for (auto const jid : cancels) {
      batch.add_msgs()->mutable_cancel()->set_drv_path(std::string(byId.at(jid)->drvPath.to_string()));
    }
    for (auto const jid : wants) {
      auto * msg = batch.add_msgs();
      fillWant(msg->mutable_want(), *byId.at(jid));
      bytes += msg->ByteSizeLong();
      sent.push_back(jid);
      if (bytes >= maxBatchBytes) {
        write();
      }
    }
    write();
    if (exe.remainingJobs() == 0 && stream != nullptr && !closed) {
      closed = true;
      stream->WritesDone();
    }
    cv.notify_all();
  }

  // Build thread body.
  void work() {
    while (auto work = next()) {
      Attempt res;
      try {
        if (isInterrupted()) {
          throw Interrupted("interrupted");
        }
        res = store.buildAssigned(*work->job, work->where, mode, evalStore);
      } catch (Interrupted &) {
        res = nixcompat::failed(nixcompat::FailureStatus::MiscFailure, "interrupted");
      } catch (std::exception & err) {
        // nix reports only the top-level result, so a failed leaf would just read "dependency failed".
        printError("%s: %s", work->job->drvPath.to_string(), err.what());
        res = nixcompat::failed(nixcompat::FailureStatus::MiscFailure, err.what());
      }
      if (auto * missing = std::get_if<MissingInput>(&res)) {
        res = recover(*work, missing->path);
      }
      // A reverse data session reconnects asynchronously to WorkerSession.
      // Immediate re-Wants can exhaust the bounded retry budget before the
      // registration and the previous assignment's Done have been processed.
      // Do not change the existing direct-worker retry behavior.
      if (std::holds_alternative<Bounced>(res) && work->where.reverse) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
      }
      std::scoped_lock const lock(mutex);
      if (auto * result = std::get_if<BuildResult>(&res)) {
        bool const succeeded = nixcompat::succeeded(*result);
        work->job->result = std::move(*result);
        exe.finished(work->jid, succeeded);
      } else if (std::holds_alternative<Bounced>(res)) {
        exe.bounced(work->jid);
      }
            flushLocked();
    }
  }

  // A drv output becomes a job in this run, a source or .drv is uploaded again.
  auto recover(const Work & work, const StorePath & path) -> Attempt {
    auto const pathS = store.printStorePath(path);
    {
      std::scoped_lock const lock(mutex);
      if (!recovered.insert(path).second) {
        return nixcompat::failed(nixcompat::FailureStatus::MiscFailure,
                                 fmt("input '%s' is missing on the farm", pathS));
      }
    }
    auto const producer = work.job->farmInputs.find(path);
    if (producer == work.job->farmInputs.end()) {
      if (work.where.reverse && !store.config->reverseUploads) {
        throw Error("reverse worker is missing build inputs; set the store setting reverse-uploads=true to send them");
      }
      warn("%s: worker lacks '%s', uploading it again", work.job->drvPath.to_string(), pathS);
      auto headers = store.routingFor(work.job->drv, work.where.reverse ? "" : work.where.addr, work.where.system);
      if (work.where.reverse) {
        headers.emplace_back("x-nix-reverse", "1");
      }
      store.checkStatus(
          store.uploadDrvClosure(evalStore, {work.job->drvPath}, headers,
                                 work.where.reverse ? std::optional(std::pair{work.job->drvPath, work.where.assignId})
                                                    : std::nullopt),
          work.where.reverse ? "UploadBuildInputs" : "AddMultipleToStore");
      return Bounced{};
    }
    warn("%s: farm lacks '%s', building '%s' too", work.job->drvPath.to_string(), pathS,
         producer->second.to_string());
    auto const drvPath = producer->second;
    auto const full = evalStore.readDerivation(drvPath);
    std::scoped_lock const lock(mutex);
    auto const name = std::string(drvPath.to_string());
    if (!byName.contains(name)) {
      auto noJobs = [](const StorePath &) -> bool { return false; };
      auto & job = jobs.emplace(drvPath, store.farmJob(evalStore, drvPath, full, noJobs)).first->second;
      job.cpHintMs = work.job->cpHintMs + 1; // ahead of the build waiting for it
      addLocked(job, {});
    }
    exe.blockOn(work.jid, byName.at(name));
    return MissingInput{.path = path};
  }

  static auto drvOf(const remote::SchedMsg & msg) -> std::string {
    switch (msg.msg_case()) {
    case remote::SchedMsg::kAssigned:
      return msg.assigned().drv_path();
    case remote::SchedMsg::kCached:
      return msg.cached().drv_path();
    case remote::SchedMsg::kUnplaceable:
      return msg.unplaceable().drv_path();
    case remote::SchedMsg::kPlaceable:
      return msg.placeable().drv_path();
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
    flushLocked();
  }

  void onMsgLocked(const remote::SchedMsg & msg) {
    auto const name = drvOf(msg);
    auto const found = byName.find(name);
    if (found == byName.end()) {
      return;
    }
    auto const jid = found->second;
    switch (msg.msg_case()) {
    case remote::SchedMsg::kAssigned:
      debug("grpc: assigned %s -> %s", name, msg.assigned().worker_addr());
      exe.onAssigned(jid, {.addr = msg.assigned().worker_addr(),
                          .assignId = msg.assigned().assign_id(),
                          .system = msg.assigned().worker_system(),
                          .reverse = msg.assigned().reverse()});
      break;
    case remote::SchedMsg::kCached:
      exe.onCached(jid);
      break;
    case remote::SchedMsg::kUnplaceable:
      if (exe.onUnplaceable(jid, msg.unplaceable().reason(), std::chrono::steady_clock::now())) {
        warn("%s: %s, waiting up to %ds for a worker", name, msg.unplaceable().reason(), store.restartGrace().count());
      }
      break;
    case remote::SchedMsg::kPlaceable:
      exe.onPlaceable(jid);
      break;
    default:
      break;
    }
  }

  // Thread body: fail jobs whose Unplaceable outlived the grace.
  void reapUnplaceable(const std::stop_token & stop) {
    std::unique_lock lock(mutex);
    while (!stop.stop_requested() && exe.remainingJobs() > 0) {
      auto const wake = exe.reap(std::chrono::steady_clock::now(), store.restartGrace());
      flushLocked();
      cv.wait_until(lock, wake);
    }
  }

  void attach(grpc::ClientReaderWriter<remote::ClientMsgs, remote::SchedMsgs> * fresh) {
    std::scoped_lock const lock(mutex);
    stream = fresh;
    closed = false;
    exe.connected();
    flushLocked();
  }

  auto detach() -> size_t {
    std::scoped_lock const lock(mutex);
    stream = nullptr;
    exe.disconnected();
    return exe.remainingJobs();
  }

  auto progress() -> uint64_t {
    std::scoped_lock const lock(mutex);
    return exe.progress();
  }

    void abandon() {
    std::scoped_lock const lock(mutex);
    exe.abandon();
    flushLocked();
  }

    void collectResults() {
    using nixcompat::FailureStatus;
    using nixgrpc::exec::Outcome;
    for (JobId jid = 0; jid < byId.size(); jid++) {
      auto & job = *byId.at(jid);
      const auto * done = std::get_if<nixgrpc::exec::state::Done>(&exe.at(jid));
      if (job.result || done == nullptr) {
        continue;
      }
      switch (done->outcome) {
      case Outcome::cached:
        job.result = cachedResult(job);
        break;
      case Outcome::dependencyFailed: {
        auto const input = exe.failedInput(jid);
        job.result = nixcompat::failed(
            FailureStatus::DependencyFailed,
            fmt("dependency '%s' failed", input ? byId.at(*input)->drvPath.to_string() : "?"));
        break;
      }
      case Outcome::unplaceable:
        // nix reports only the top-level result. Say why a leaf failed.
        printError("%s: %s", job.drvPath.to_string(), done->message);
        [[fallthrough]];
      default:
        job.result = nixcompat::failed(FailureStatus::MiscFailure,
                                       isInterrupted() ? std::string("interrupted") : done->message);
      }
    }
  }
};

// A scheduler dying mid-stream shows as UNKNOWN/INTERNAL "Stream removed"
// or an RST_STREAM. It holds soft state only, so any of these is worth a
// reconnect. Config errors (auth, TLS, bad request) are not.
namespace {
auto schedulerGone(const grpc::Status & status) -> bool {
  return nixgrpc::goneAway(status) || status.error_code() == grpc::StatusCode::UNKNOWN
         || status.error_code() == grpc::StatusCode::INTERNAL;
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
// re-Want everything not done. Placed jobs carry on.
auto GrpcStore::scheduleUntilDone(Run & run, const Metadata & headers) -> grpc::Status {
  // One long build may be all there is, so a long-lived stream counts as progress.
  static constexpr std::chrono::seconds healthyStream{30};
  grpc::Status status;
  std::optional<nixgrpc::Backoff> backoff;
  bool handover = false;
  for (;;) {
    nixgrpc::Call call;
    addHeaders(call.ctx(), headers);
    auto const before = run.progress();
    auto const opened = std::chrono::steady_clock::now();
    auto stream = sched->Schedule(&call.ctx());
    run.attach(stream.get());
    debug("grpc: Schedule stream open, %d jobs", run.jobs.size());
    auto const [answered, restarting] = readSchedule(*stream, run, call.ctx());
    status = stream->Finish();
    if (run.detach() == 0) {
      return grpc::Status::OK;
    }
    if (isInterrupted() || !(restarting || schedulerGone(status))) {
      return status;
    }
    bool const recovered =
        run.progress() != before || (answered && std::chrono::steady_clock::now() - opened >= healthyStream);
    if (!backoff || recovered) {
      backoff.emplace(restartGrace());
    }
    if (backoff->exhausted()) {
      return status;
    }
    // The balancer has no healthy upstream until the new leader is up, so
    // retries after an announced restart belong to the same handover.
    handover = recovered ? restarting : handover || restarting;
    logReconnect(config->authority.to_string(), status, handover);
    std::this_thread::sleep_for(backoff->next());
  }
}

void GrpcStore::runJobs(std::map<StorePath, Job> & jobs, BuildMode buildMode, Store & evalStore) {
  if (jobs.empty()) {
    return;
  }
  auto const threadCount = std::min<size_t>(config->maxBuilds, jobs.size());
  Run run(*this, buildMode, evalStore, jobs, threadCount);
  auto const headers = routingFor(jobs.begin()->second.drv);
  // Uploading here keeps a build from waiting on the upload of its inputs' derivations.
  std::vector<StorePath> drvPaths;
  drvPaths.reserve(jobs.size());
  for (const auto & [drvPath, job] : jobs) {
    drvPaths.push_back(drvPath);
  }
  checkStatus(uploadDrvClosure(evalStore, std::move(drvPaths), headers), "AddMultipleToStore");
  std::vector<std::jthread> threads(threadCount);
  for (auto & thread : threads) {
    thread = std::jthread([&run] -> void { run.work(); });
  }
  std::jthread reaper([&run](const std::stop_token & stop) -> void { run.reapUnplaceable(stop); });
  std::stop_callback const wakeReaper(reaper.get_stop_token(), [&run] -> void { run.cv.notify_all(); });
  // Also on unwind: lets the threads drain so the join in ~jthread returns.
  Finally const abandon([&run] -> void { run.abandon(); });

  auto const status = scheduleUntilDone(run, headers);
  checkInterrupt();
  // Before the join: the abandoned jobs would only report a lost connection.
  if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED) {
    throw statusError(status, "Schedule");
  }
  run.abandon();
  reaper.request_stop();
  threads.clear(); // join
  run.collectResults();
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
  auto const validOpaque = opaque.empty() ? StorePathSet{} : queryValidPaths(opaque, Substitute);

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
// jobs: the worker fetches them when it builds the dependant.
//
// The server is queried top-down. One QueryValidPaths for the whole closure is
// slow: with a remote cache configured, the daemon publishes every path it
// holds before it answers.
//
// NoSubstitute: the daemon still reports what its substituters offer, but
// does not fetch anything just to answer.
auto GrpcStore::missingInputDrvs(const std::vector<DerivedPath> & reqs, Store & evalStore) -> StorePathSet {
  std::map<StorePath, Derivation> drvs;
  std::function<void(const StorePath &)> collect = [&](const StorePath & drvPath) -> void {
    if (drvs.contains(drvPath)) {
      return;
    }
    auto const & full = drvs.emplace(drvPath, evalStore.readDerivation(drvPath)).first->second;
    nixcompat::forInputDrvs(full, collect);
  };
  StorePathSet tops;
  for (const auto & req : reqs) {
    if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
      tops.insert(built->drvPath->getBaseStorePath());
      collect(built->drvPath->getBaseStorePath());
    }
  }
  return nixgrpc::missingTopDown(
      std::vector(tops.begin(), tops.end()),
      [&](const StorePath & drvPath) -> std::vector<StorePath> {
        std::vector<StorePath> outs;
        if (tops.contains(drvPath)) {
          return outs;
        }
        for (auto & [name, path] : evalStore.queryStaticPartialDerivationOutputMap(drvPath)) {
          if (path) {
            outs.push_back(*path);
          }
        }
        return outs;
      },
      [&](const StorePath & drvPath) -> std::vector<StorePath> {
        std::vector<StorePath> inputs;
        nixcompat::forInputDrvs(drvs.at(drvPath), [&](const StorePath & input) -> void { inputs.push_back(input); });
        return inputs;
      },
      [&](const StorePathSet & paths) -> StorePathSet { return queryValidPaths(paths, NoSubstitute); });
}

auto GrpcStore::farmJob(Store & evalStore, const StorePath & drvPath, const Derivation & full,
                        const std::function<bool(const StorePath &)> & isJob) -> Job {
  Job job{.drvPath = drvPath, .drv = basicForFarm(evalStore, drvPath, full)};
  nixcompat::forInputDrvs(full, [&](const StorePath & input) -> void {
    if (isJob(input)) {
      return;
    }
    for (auto & [name, path] : evalStore.queryStaticPartialDerivationOutputMap(input)) {
      if (path) {
        job.farmInputs.emplace(*path, input);
      }
    }
  });
  return job;
}

void GrpcStore::loadJobs(const std::vector<DerivedPath> & reqs, Store & evalStore,
                  std::map<StorePath, Job> & jobs) {
  auto const missing = missingInputDrvs(reqs, evalStore);
  std::function<Job &(const StorePath &)> load = [&](const StorePath & drvPath) -> Job & {
    if (auto const found = jobs.find(drvPath); found != jobs.end()) {
      return found->second;
    }
    auto const full = evalStore.readDerivation(drvPath);
    auto const isJob = [&](const StorePath & input) -> bool { return missing.contains(input); };
    auto & job = jobs.emplace(drvPath, farmJob(evalStore, drvPath, full, isJob)).first->second;
    nixcompat::forInputDrvs(full, [&](const StorePath & input) -> void {
      if (!isJob(input)) {
        return;
      }
      auto & dep = load(input);
      dep.dependants.push_back(&job);
      job.inputs.push_back(&dep);
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
