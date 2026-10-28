// Run the production scheduler, reverse worker and RPC dispatchers over mTLS.
// Do not replace WorkerSession or Register with an in-memory test transport.
#include "../src/daemon/main.cc"

#include <cassert>
#include <array>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>
#include <vector>
#include <openssl/rand.h>
#include <nix/store/worker-protocol.hh>
#include <nix/util/archive.hh>
#include <nix/util/serialise.hh>
#include <nix/util/signature/local-keys.hh>
#include <nix/util/signature/signer.hh>
#include <zstd.h>

void clientDrivenReverseBuild(const std::string & farmUri, const std::string & sourceUri,
                               const std::string & drvPath);
void clientDrivenConcurrentReverseBuild(const std::string & farmUri, const std::string & sourceUri,
                                         const std::string & first, const std::string & second);
void clientDrivenRecoveredReverseBuild(const std::string & farmUri, const std::string & sourceUri,
                                       const std::string & drvPath);

int main(int argc, char ** argv)
{
    assert(argc == 3);
    assert(OPENSSL_init_crypto(OPENSSL_INIT_NO_ATEXIT, nullptr) == 1);
    grpc_init();
    nix::initLibStore(false);
    const std::string dir = argv[1];
    // The worker connects to a separate nix-daemon process. Its trust policy
    // is set by the wrapper, not by this process's nix::settings.
    const std::string socket = nix::readFile(dir + "/daemon-socket-path");
    const std::string storeUri = "unix://" + socket;
    nixgrpc::Options schedulerOptions;
    schedulerOptions.builder = false;
    schedulerOptions.scheduler = true;
    schedulerOptions.maxJobs = 0;
    schedulerOptions.storeUri = "local?root=" + dir + "/scheduler-store";
    schedulerOptions.tlsCert = dir + "/server.crt";
    schedulerOptions.tlsKey = dir + "/server.key";
    schedulerOptions.clientCA = dir + "/server.crt";
    schedulerOptions.acl.addRule("localhost=trusted");
    schedulerOptions.acl.addRule("client=write");
    schedulerOptions.acl.requireIdentity();
    schedulerOptions.reverseOidcConfig = dir + "/oidc.json";
    schedulerOptions.reverseProvider = "worker-tunnel";
    schedulerOptions.reverseNamespace = "staging";
    schedulerOptions.reverseServiceAccount = "worker";
    schedulerOptions.reverseAudience = "grpc://worker-tunnel";
    nixgrpc::Metrics metrics("");
    nixgrpc::IdleTracker idle;
    std::optional<nixgrpc::oidc::Verifier> noClientOidc;
    nixgrpc::Auth schedulerAuth{.acl = schedulerOptions.acl, .oidc = &noClientOidc};
    nixgrpc::Coordinator scheduler(schedulerOptions, schedulerAuth, metrics, idle);
    nixgrpc::oidc::Verifier verifier(nixgrpc::oidc::loadConfig(schedulerOptions.reverseOidcConfig));
    nixgrpc::WorkerIdentity identity(verifier, "worker-tunnel", "staging", "worker", "grpc://worker-tunnel");
    auto const bound = identity.bind(nix::chomp(nix::readFile(dir + "/token")),
                                     "pod:staging:12345678-1234-1234-1234-123456789abc");
    if (!bound) {
        auto const result = verifier.verify(nix::chomp(nix::readFile(dir + "/token")));
        std::cerr << "fixture token identity: " << bool(result.identity) << " error: " << result.error << "\n";
        return 1;
    }
    scheduler.scheduler->setReverseIdentity(identity);
    nixgrpc::ReverseSessionService registration(identity, scheduler.reverseRegistry, schedulerAuth);
    NixRemoteService gateway("", schedulerOptions.storeUri, "scheduler", metrics, idle,
                             nixgrpc::LogLevel::info, schedulerAuth, scheduler);
    grpc::ServerBuilder farmBuilder;
    int farmPort = 0;
    farmBuilder.AddListeningPort("127.0.0.1:0", nixgrpc::makeServerCredentials(schedulerOptions), &farmPort);
    farmBuilder.RegisterService(scheduler.scheduler.get());
    farmBuilder.RegisterService(&registration);
    farmBuilder.RegisterService(&gateway);
    auto farm = farmBuilder.BuildAndStart();
    assert(farm && farmPort > 0);
    scheduler.start(*farm);

    nixgrpc::Options workerOptions;
    workerOptions.scheduler = false;
    workerOptions.builder = true;
    workerOptions.maxJobs = 8;
    workerOptions.storeUri = storeUri;
    workerOptions.socketPath = socket;
    workerOptions.tlsCert = schedulerOptions.tlsCert;
    workerOptions.tlsKey = schedulerOptions.tlsKey;
    workerOptions.clientCA = schedulerOptions.clientCA;
    workerOptions.schedulerAddr = "localhost:" + std::to_string(farmPort);
    workerOptions.reverseTokenFile = dir + "/token";
    workerOptions.reverseWorkerId = "pod:staging:12345678-1234-1234-1234-123456789abc";
    workerOptions.advertise = "reverse-worker";
    if (std::filesystem::exists(dir + "/cache-url")) {
        workerOptions.niks3.url = nix::readFile(dir + "/cache-url");
        workerOptions.niks3.tokenFile = dir + "/cache-token";
        workerOptions.niks3.pushArgv = {dir + "/cache-push", "push", "--stdin", "--server-url",
                                      workerOptions.niks3.url, "--auth-token-path", workerOptions.niks3.tokenFile};
    }
    nixgrpc::Auth workerAuth;
    nixgrpc::Coordinator worker(workerOptions, workerAuth, metrics, idle);
    NixRemoteService workerService(workerOptions.socketPath, workerOptions.storeUri, "worker", metrics, idle,
                                   nixgrpc::LogLevel::info, workerAuth, worker);
    worker.setReverseBuild([&workerService](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
        return workerService.reverseBuild(call, emit, cancelled, afterFinish);
    });
    worker.setReverseUpload([&workerService](auto const & call, auto const & cancelled) {
        return workerService.reverseUpload(call, cancelled);
    });
    grpc::ServerBuilder workerBuilder;
    workerBuilder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials());
    workerBuilder.RegisterService(&workerService);
    auto workerServer = workerBuilder.BuildAndStart();
    assert(workerServer);
    worker.start(*workerServer);

    constexpr auto timeout = std::chrono::steady_clock::duration(std::chrono::seconds(15));
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (!scheduler.reverseRegistry.find(workerOptions.reverseWorkerId)
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    assert(scheduler.reverseRegistry.find(workerOptions.reverseWorkerId));
    std::cerr << "production TLS worker registration ready\n";
    // The serve loop is not running here; enforce control-credential expiry
    // like Coordinator::tick does for the rest of the fixture.
    std::jthread credentialTicker([&](std::stop_token stop) {
        std::mutex tickMutex;
        std::condition_variable_any tickCv;
        std::unique_lock lock(tickMutex);
        while (!stop.stop_requested()) {
            tickCv.wait_for(lock, stop, std::chrono::seconds(1), [] { return false; });
            if (!stop.stop_requested()) scheduler.dispatcher->expireCredentials();
        }
    });
    {
        // Rotate the projected token the way kubelet does (atomic swap) and
        // keep both reverse sessions alive past the first token's expiry.
        auto const firstGeneration = scheduler.reverseRegistry.find(workerOptions.reverseWorkerId)->generation;
        std::filesystem::copy_file(dir + "/token-next", dir + "/token.tmp");
        std::filesystem::rename(dir + "/token.tmp", dir + "/token");
        auto const firstExpiry = std::chrono::system_clock::time_point(
            std::chrono::seconds(std::stoll(nix::chomp(nix::readFile(dir + "/token-expires")))));
        std::this_thread::sleep_until(firstExpiry + std::chrono::seconds(3));
        auto const live = scheduler.reverseRegistry.find(workerOptions.reverseWorkerId);
        assert(live && live->generation == firstGeneration);
        assert(scheduler.dispatcher->podConnected(workerOptions.reverseWorkerId));
        // Reachable only through its data session: never an EDS endpoint.
        auto const members = scheduler.dispatcher->membership();
        for (auto const & [system, list] : *members)
            for (auto const & member : list) assert(member.addr != workerOptions.advertise);
        assert(metrics.eventCounter("worker_credential_expired").Value() == 0);
        std::cerr << "rotated tunnel token kept data and control sessions past first expiry\n";
    }

    auto const source = nix::openStore("local?root=" + dir + "/source-store");
    std::string random(nixgrpc::ReverseFrames::maxQueuedBytes + 2 * 1024 * 1024, '\0');
    assert(RAND_bytes(reinterpret_cast<unsigned char *>(random.data()), static_cast<int>(random.size())) == 1);
    nix::StringSink nar;
    nix::dumpString(random, nar);
    nix::StringSource input(nar.s);
    auto const path = source->addToStoreFromDump(input, "reverse-production");
    nix::ValidPathInfo output(*source->queryPathInfo(path));
    output.ca.reset();
    output.sigs.clear();
    output.ultimate = false;
    auto const encodeUpload = [&](const nix::ValidPathInfo & info) {
        nix::StringSink body;
        body << static_cast<uint64_t>(1);
        nix::WorkerProto::Serialise<nix::ValidPathInfo>::write(
            *source, nix::WorkerProto::WriteConn{.to = body, .version = nixcompat::infoProtocolVersion()}, info);
        source->narFromPath(path, body);
        std::string packed(ZSTD_compressBound(body.s.size()), '\0');
        auto const size = ZSTD_compress(packed.data(), packed.size(), body.s.data(), body.s.size(), 1);
        assert(!ZSTD_isError(size));
        packed.resize(size);
        assert(packed.size() > nixgrpc::ReverseFrames::maxQueuedBytes);
        return packed;
    };
    auto const unsignedBody = encodeUpload(output);
    nix::LocalSigner signer(nix::SecretKey(nix::chomp(nix::readFile(dir + "/signing.key"))));
    output.sign(*source, signer);
    auto const signedBody = encodeUpload(output);

    grpc::SslCredentialsOptions clientTls;
    clientTls.pem_root_certs = nix::readFile(dir + "/server.crt");
    clientTls.pem_private_key = nix::readFile(dir + "/client.key");
    clientTls.pem_cert_chain = nix::readFile(dir + "/client.crt");
    auto channel = grpc::CreateChannel("localhost:" + std::to_string(farmPort), grpc::SslCredentials(clientTls));
    auto schedule = nix::remote::Scheduler::NewStub(channel);
    auto client = nix::remote::NixRemote::NewStub(channel);
    grpc::ClientContext scheduleContext;
    scheduleContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
    auto stream = schedule->Schedule(&scheduleContext);
    auto const drv = "00000000000000000000000000000000-reverse-production.drv";
    nix::remote::ClientMsgs want;
    auto * request = want.add_msgs()->mutable_want();
    request->set_drv_path(drv);
    request->set_system(nix::settings.thisSystem.get());
    assert(stream->Write(want));
    nix::remote::SchedMsgs scheduled;
    uint64_t assignment = 0;
    while (!assignment && stream->Read(&scheduled)) {
        for (auto const & msg : scheduled.msgs()) {
            if (msg.has_assigned()) {
                assert(msg.assigned().reverse() && msg.assigned().drv_path() == drv);
                assignment = msg.assigned().assign_id();
            }
        }
    }
    assert(assignment != 0);
    assert(scheduler.dispatcher->assignmentPod(drv, assignment) == workerOptions.reverseWorkerId);
    auto const expectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!worker.builder->expects(drv, assignment) && std::chrono::steady_clock::now() < expectDeadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(worker.builder->expects(drv, assignment));
    auto const upload = [&](const std::string & packed) {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        nix::remote::AddMultipleReply reply;
        auto writer = client->UploadBuildInputs(&ctx, &reply);
        nix::remote::UploadBuildInputsChunk header;
        header.set_drv_path(drv);
        header.set_assign_id(assignment);
        header.mutable_chunk()->set_check_sigs(true);
        if (!writer->Write(header)) return writer->Finish();
        for (size_t offset = 0; offset < packed.size(); offset += 128 * 1024) {
            nix::remote::UploadBuildInputsChunk chunk;
            chunk.mutable_chunk()->set_data(packed.substr(offset, 128 * 1024));
            if (!writer->Write(chunk)) return writer->Finish();
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        writer->WritesDone();
        return writer->Finish();
    };
    auto workerStore = nix::openStore(storeUri);
    auto status = upload(unsignedBody);
    assert(status.error_code() == grpc::StatusCode::INTERNAL
           && status.error_message().find("lacks a signature by a trusted key") != std::string::npos);
    assert(!workerStore->isValidPath(path));
    status = upload(signedBody);
    if (!status.ok()) std::cerr << "signed upload: " << status.error_code() << " " << status.error_message() << "\n";
    assert(status.ok() && workerStore->isValidPath(path));
    auto rootedStore = nix::openStore("local?root=" + dir + "/worker-store");
    // A rooted nix-daemon's RemoteStore::narFromPath tries the host /nix/store
    // path on Darwin. Verify the actual imported NAR via the rooted local
    // store until the fixture can run a daemon with a real isolated store.
    nix::StringSink readback;
    rootedStore->narFromPath(path, readback);
    assert(readback.s == nar.s);
    stream->WritesDone();
    stream->Finish();

    // A real derivation starts only in the evaluation store. Exercise the
    // production NOT_FOUND and exact-assignment upload. Darwin cannot build
    // using this diverted store, so the retry runs on Linux only.
    auto const buildDrv = source->parseStorePath(nix::chomp(nix::readFile(dir + "/build.drv")));
    assert(source->isValidPath(buildDrv) && !workerStore->isValidPath(buildDrv));
    auto const fullDrv = source->readDerivation(buildDrv);
    auto const basicDrv = nixcompat::toBasicDrv(fullDrv, [&](const nix::StorePath &, const std::string &) -> nix::StorePath {
        throw nix::Error("unexpected input derivation in fixture");
    });
    assert(basicDrv);
#ifdef __linux__
    // Exercise the actual GrpcStore build machinery, including its Schedule
    // stream and pinned missing-closure retry, on the production TLS endpoint.
    // The missing-closure retry is opt-in per store (reverse-uploads).
    auto const farmUri = "grpc://localhost:" + std::to_string(farmPort)
        + "?ca-cert=" + dir + "/server.crt&client-cert=" + dir + "/client.crt&client-key=" + dir + "/client.key"
        + "&reverse-uploads=true";
    clientDrivenReverseBuild(farmUri, "local?root=" + dir + "/source-store", source->printStorePath(buildDrv));
    assert(workerStore->isValidPath(buildDrv));
    auto const builtOutput = basicDrv->outputs.at("out").path(*source, basicDrv->name, "out");
    assert(builtOutput && workerStore->isValidPath(*builtOutput));
    nix::StringSink builtNar;
    rootedStore->narFromPath(*builtOutput, builtNar);
    assert(builtNar.s.find("reverse-built") != std::string::npos);
    if (worker.cache.hasRemote()) {
        // A distinct store with no access to the worker's rooted files must
        // fetch and verify the signed output through the cache read proxy.
        auto const cacheUrl = nix::readFile(dir + "/cache-url");
        auto const cacheKey = nix::chomp(nix::readFile(dir + "/cache-signing.pub"));
        nix::settings.trustedPublicKeys.set({cacheKey});
        auto cacheStore = nix::openStore(cacheUrl);
        auto readerStore = nix::openStore("local?root=" + dir + "/reader-store");
        assert(!readerStore->isValidPath(*builtOutput));
        nix::copyPaths(*cacheStore, *readerStore, nix::StorePathSet{*builtOutput});
        assert(readerStore->isValidPath(*builtOutput));
        nix::StringSink cacheNar;
        readerStore->narFromPath(*builtOutput, cacheNar);
        assert(cacheNar.s == builtNar.s);
    }
    {
        auto worker2Options = workerOptions;
        worker2Options.socketPath = nix::readFile(dir + "/daemon2-socket-path");
        worker2Options.storeUri = "unix://" + worker2Options.socketPath;
        worker2Options.reverseTokenFile = dir + "/token2";
        worker2Options.reverseWorkerId = "pod:staging:87654321-4321-4321-4321-cba987654321";
        worker2Options.advertise = "reverse-worker-2";
        if (worker2Options.niks3.enabled()) worker2Options.niks3.pushArgv[0] = dir + "/cache-push2";
        nixgrpc::Auth worker2Auth;
        nixgrpc::Coordinator worker2(worker2Options, worker2Auth, metrics, idle);
        NixRemoteService worker2Service(worker2Options.socketPath, worker2Options.storeUri, "worker-2", metrics, idle,
                                         nixgrpc::LogLevel::info, worker2Auth, worker2);
        worker2.setReverseBuild([&worker2Service](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
            return worker2Service.reverseBuild(call, emit, cancelled, afterFinish);
        });
        worker2.setReverseUpload([&worker2Service](auto const & call, auto const & cancelled) {
            return worker2Service.reverseUpload(call, cancelled);
        });
        grpc::ServerBuilder worker2Builder;
        worker2Builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials());
        worker2Builder.RegisterService(&worker2Service);
        auto worker2Server = worker2Builder.BuildAndStart();
        assert(worker2Server);
        worker2.start(*worker2Server);
        auto const secondDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (!scheduler.reverseRegistry.find(worker2Options.reverseWorkerId)
               && std::chrono::steady_clock::now() < secondDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        assert(scheduler.reverseRegistry.find(worker2Options.reverseWorkerId));
        std::cerr << "production TLS second worker registration ready\n";
        auto const first = nix::chomp(nix::readFile(dir + "/one.drv"));
        auto const second = nix::chomp(nix::readFile(dir + "/two.drv"));
        auto worker2Store = nix::openStore(worker2Options.storeUri);
        assert(!workerStore->isValidPath(source->parseStorePath(first)));
        assert(!worker2Store->isValidPath(source->parseStorePath(second)));
        clientDrivenConcurrentReverseBuild(farmUri, "local?root=" + dir + "/source-store", first, second);
        // Completed fixture jobs must send Done and free scheduler slots, not
        // merely leave valid outputs in their worker stores.
        auto const concurrentDoneDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (metrics.eventCounter("done_BUILT").Value() < 3
               && std::chrono::steady_clock::now() < concurrentDoneDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        assert(metrics.eventCounter("done_BUILT").Value() >= 3);
        // The single client Schedule stream can serialize retries even when
        // two requests are outstanding. Prepack both NARs before taking any
        // Expect (which expires after 60s), then independently assign two jobs
        // and hold their real, pinned upload RPCs at the worker callback.
        // Neither importer starts until both derivations have reached their
        // respective live assignments (even if both choose the same pod).
        std::mutex overlapMutex;
        std::condition_variable overlapCv;
        std::set<std::string> arrivedDrvs;
        bool uploadsOverlapped = false;
        auto const gateUpload = [&](const auto & call, const std::string & pod) -> grpc::Status {
            if (call.delegated.drvPath.find("reverse-overlap-") == std::string::npos)
                return grpc::Status::OK;
            std::unique_lock lock(overlapMutex);
            if (scheduler.dispatcher->assignmentPod(call.delegated.drvPath, call.delegated.assignId) != pod)
                return {grpc::StatusCode::FAILED_PRECONDITION, "upload lost its exact-worker assignment"};
            arrivedDrvs.insert(call.delegated.drvPath);
            if (arrivedDrvs.size() == 2) {
                uploadsOverlapped = true;
                overlapCv.notify_all();
            }
            if (!overlapCv.wait_for(lock, std::chrono::seconds(15), [&] { return uploadsOverlapped; }))
                return {grpc::StatusCode::DEADLINE_EXCEEDED, "second pinned upload did not overlap"};
            if (scheduler.dispatcher->assignmentPod(call.delegated.drvPath, call.delegated.assignId) != pod)
                return {grpc::StatusCode::FAILED_PRECONDITION, "upload assignment changed while held"};
            return grpc::Status::OK;
        };
        worker.setReverseUpload([&](auto const & call, auto const & cancelled) {
            auto status = gateUpload(call, workerOptions.reverseWorkerId);
            return status.ok() ? workerService.reverseUpload(call, cancelled) : status;
        });
        worker2.setReverseUpload([&](auto const & call, auto const & cancelled) {
            auto status = gateUpload(call, worker2Options.reverseWorkerId);
            return status.ok() ? worker2Service.reverseUpload(call, cancelled) : status;
        });
        struct PinnedUpload {
            std::string drv;
            std::string pod;
            uint64_t assignment = 0;
            std::string path;
            std::string packed;
            std::unique_ptr<grpc::ClientReaderWriter<nix::remote::ClientMsgs, nix::remote::SchedMsgs>> stream;
            std::unique_ptr<grpc::ClientContext> context;
        };
        std::vector<PinnedUpload> pinned;
        auto makeUpload = [&](std::string drv, const std::string & name) {
            PinnedUpload probe;
            probe.drv = std::move(drv);
            nix::StringSource probeNar(nar.s);
            auto const path = source->addToStoreFromDump(probeNar, name);
            probe.path = source->printStorePath(path);
            nix::ValidPathInfo info(*source->queryPathInfo(path));
            info.ca.reset();
            info.sigs.clear();
            info.ultimate = false;
            info.sign(*source, signer);
            nix::StringSink wire;
            wire << static_cast<uint64_t>(1);
            nix::WorkerProto::Serialise<nix::ValidPathInfo>::write(
                *source, nix::WorkerProto::WriteConn{.to = wire, .version = nixcompat::infoProtocolVersion()}, info);
            source->narFromPath(path, wire);
            probe.packed.resize(ZSTD_compressBound(wire.s.size()));
            auto const compressed = ZSTD_compress(probe.packed.data(), probe.packed.size(), wire.s.data(), wire.s.size(), 1);
            assert(!ZSTD_isError(compressed));
            probe.packed.resize(compressed);
            assert(probe.packed.size() > nixgrpc::ReverseFrames::maxQueuedBytes);
            return probe;
        };
        pinned.push_back(makeUpload("11111111111111111111111111111111-reverse-overlap-one.drv", "reverse-overlap-one"));
        pinned.push_back(makeUpload("22222222222222222222222222222222-reverse-overlap-two.drv", "reverse-overlap-two"));
        for (auto & probe : pinned) {
            probe.context = std::make_unique<grpc::ClientContext>();
            probe.context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
            probe.stream = schedule->Schedule(probe.context.get());
            nix::remote::ClientMsgs wantProbe;
            auto * want = wantProbe.add_msgs()->mutable_want();
            want->set_drv_path(probe.drv);
            want->set_system(nix::settings.thisSystem.get());
            assert(probe.stream->Write(wantProbe));
            nix::remote::SchedMsgs reply;
            while (!probe.assignment && probe.stream->Read(&reply)) {
                for (auto const & msg : reply.msgs())
                    if (msg.has_assigned() && msg.assigned().drv_path() == probe.drv) {
                        assert(msg.assigned().reverse());
                        probe.assignment = msg.assigned().assign_id();
                    }
            }
            assert(probe.assignment);
            auto const podDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            auto pod = scheduler.dispatcher->assignmentPod(probe.drv, probe.assignment);
            while (!pod && std::chrono::steady_clock::now() < podDeadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                pod = scheduler.dispatcher->assignmentPod(probe.drv, probe.assignment);
            }
            assert(pod);
            probe.pod = *pod;
            auto & selected = probe.pod == workerOptions.reverseWorkerId ? worker : worker2;
            auto const expectUntil = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!selected.builder->expects(probe.drv, probe.assignment) && std::chrono::steady_clock::now() < expectUntil)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            assert(selected.builder->expects(probe.drv, probe.assignment));
        }
        // The independent Schedule streams are both pinned. The scheduler may
        // choose the same worker twice; its bounded executor pool must still
        // allow both in-flight uploads to meet at the gate. Allow additional
        // fixture slots for earlier completed builds still awaiting retirement.
        for (auto const & probe : pinned) {
            auto & selected = probe.pod == workerOptions.reverseWorkerId ? worker : worker2;
            auto const until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!selected.builder->expects(probe.drv, probe.assignment) && std::chrono::steady_clock::now() < until)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            assert(selected.builder->expects(probe.drv, probe.assignment));
        }
        std::array<grpc::Status, 2> uploadResults;
        std::vector<std::thread> uploads;
        for (size_t i = 0; i < pinned.size(); ++i) uploads.emplace_back([&, i] {
            auto const & probe = pinned[i];
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(40));
            nix::remote::AddMultipleReply reply;
            auto writer = client->UploadBuildInputs(&context, &reply);
            nix::remote::UploadBuildInputsChunk header;
            header.set_drv_path(probe.drv);
            header.set_assign_id(probe.assignment);
            header.mutable_chunk()->set_check_sigs(true);
            if (!writer->Write(header)) { uploadResults[i] = writer->Finish(); return; }
            for (size_t offset = 0; offset < probe.packed.size(); offset += 128 * 1024) {
                nix::remote::UploadBuildInputsChunk data;
                data.mutable_chunk()->set_data(probe.packed.substr(offset, 128 * 1024));
                if (!writer->Write(data)) { uploadResults[i] = writer->Finish(); return; }
            }
            writer->WritesDone();
            uploadResults[i] = writer->Finish();
        });
        for (auto & uploadThread : uploads) uploadThread.join();
        assert(uploadsOverlapped && arrivedDrvs.size() == 2);
        for (size_t i = 0; i < pinned.size(); ++i) {
            if (!uploadResults[i].ok()) std::cerr << "overlap upload: " << uploadResults[i].error_message() << "\n";
            assert(uploadResults[i].ok());
            auto const & probe = pinned[i];
            auto const path = source->parseStorePath(probe.path);
            assert((probe.pod == workerOptions.reverseWorkerId ? workerStore : worker2Store)->isValidPath(path));
            assert(!(probe.pod == workerOptions.reverseWorkerId ? worker2Store : workerStore)->isValidPath(path));
            probe.stream->WritesDone();
            probe.stream->Finish();
        }
        std::cerr << "two pinned uploads overlapped on their exact assigned workers\n";
        // Stop the real importer after it has read decompressed bytes, then
        // cancel its client RPC and revoke the sole Schedule follower. The
        // production post-read validity check must reject those bytes.
        auto interrupted = makeUpload("33333333333333333333333333333333-reverse-interrupted.drv",
                                      "reverse-interrupted");
        std::mutex interruptMutex;
        std::condition_variable interruptCv;
        bool importerRead = false;
        bool partialWritten = false;
        bool interruptReleased = false;
        auto const interruptRead = [&](const std::string & drv, size_t bytesRead) {
            // Require multiple actual decompressed reads, not just a path
            // count or the first few bytes of the closure header.
            if (drv != interrupted.drv || bytesRead < 64 * 1024) return;
            std::unique_lock lock(interruptMutex);
            importerRead = true;
            interruptCv.notify_all();
            if (!interruptCv.wait_for(lock, std::chrono::seconds(10), [&] { return interruptReleased; }))
                throw nix::Error("fixture: upload read gate timed out");
        };
        workerService.setReverseUploadReadHook(interruptRead);
        worker2Service.setReverseUploadReadHook(interruptRead);
        interrupted.context = std::make_unique<grpc::ClientContext>();
        interrupted.context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        interrupted.stream = schedule->Schedule(interrupted.context.get());
        nix::remote::ClientMsgs wantInterrupted;
        wantInterrupted.add_msgs()->mutable_want()->set_drv_path(interrupted.drv);
        wantInterrupted.mutable_msgs(0)->mutable_want()->set_system(nix::settings.thisSystem.get());
        assert(interrupted.stream->Write(wantInterrupted));
        nix::remote::SchedMsgs interruptedReply;
        while (!interrupted.assignment && interrupted.stream->Read(&interruptedReply))
            for (auto const & msg : interruptedReply.msgs())
                if (msg.has_assigned() && msg.assigned().drv_path() == interrupted.drv) {
                    assert(msg.assigned().reverse());
                    interrupted.assignment = msg.assigned().assign_id();
                }
        assert(interrupted.assignment);
        auto const interruptPod = scheduler.dispatcher->assignmentPod(interrupted.drv, interrupted.assignment);
        assert(interruptPod);
        interrupted.pod = *interruptPod;
        auto & interruptWorker = interrupted.pod == workerOptions.reverseWorkerId ? worker : worker2;
        auto const interruptExpectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!interruptWorker.builder->expects(interrupted.drv, interrupted.assignment)
               && std::chrono::steady_clock::now() < interruptExpectDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(interruptWorker.builder->expects(interrupted.drv, interrupted.assignment));
        grpc::Status interruptedStatus;
        grpc::ClientContext partialContext;
        partialContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(20));
        std::thread partialUpload([&] {
            nix::remote::AddMultipleReply reply;
            auto writer = client->UploadBuildInputs(&partialContext, &reply);
            nix::remote::UploadBuildInputsChunk header;
            header.set_drv_path(interrupted.drv);
            header.set_assign_id(interrupted.assignment);
            header.mutable_chunk()->set_check_sigs(true);
            if (!writer->Write(header)) { interruptedStatus = writer->Finish(); return; }
            nix::remote::UploadBuildInputsChunk data;
            data.mutable_chunk()->set_data(interrupted.packed.substr(0, 128 * 1024));
            if (!writer->Write(data)) { interruptedStatus = writer->Finish(); return; }
            {
                const std::scoped_lock lock(interruptMutex);
                partialWritten = true;
            }
            interruptCv.notify_all();
            interruptedStatus = writer->Finish();
        });
        {
            std::unique_lock lock(interruptMutex);
            assert(interruptCv.wait_for(lock, std::chrono::seconds(10), [&] { return importerRead && partialWritten; }));
        }
        assert(scheduler.dispatcher->assignmentPod(interrupted.drv, interrupted.assignment) == interrupted.pod);
        assert(interruptWorker.builder->expects(interrupted.drv, interrupted.assignment));
        partialContext.TryCancel();
        nix::remote::ClientMsgs cancelInterrupted;
        cancelInterrupted.add_msgs()->mutable_cancel()->set_drv_path(interrupted.drv);
        assert(interrupted.stream->Write(cancelInterrupted));
        auto const revokedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (scheduler.dispatcher->assignmentPod(interrupted.drv, interrupted.assignment)
               && std::chrono::steady_clock::now() < revokedDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(!scheduler.dispatcher->assignmentPod(interrupted.drv, interrupted.assignment));
        {
            const std::scoped_lock lock(interruptMutex);
            interruptReleased = true;
        }
        interruptCv.notify_all();
        partialUpload.join();
        assert(!interruptedStatus.ok());
        assert(!workerStore->isValidPath(source->parseStorePath(interrupted.path)));
        assert(!worker2Store->isValidPath(source->parseStorePath(interrupted.path)));
        if (worker.cache.hasRemote()) {
            auto cache = nix::openStore(nix::readFile(dir + "/cache-url"));
            assert(!cache->isValidPath(source->parseStorePath(interrupted.path)));
        }
        interrupted.stream->WritesDone();
        interrupted.stream->Finish();
        workerService.setReverseUploadReadHook({});
        worker2Service.setReverseUploadReadHook({});
        std::cerr << "mid-import pinned upload revoked without importing a partial NAR\n";
        auto spooled = makeUpload("44444444444444444444444444444444-reverse-spooled.drv", "reverse-spooled");
        std::mutex spoolMutex;
        std::condition_variable spoolCv;
        bool spoolArrived = false;
        bool spoolReleased = false;
        auto const spoolGate = [&](const std::string & drv) {
            if (drv != spooled.drv) return;
            std::unique_lock lock(spoolMutex);
            spoolArrived = true;
            spoolCv.notify_all();
            if (!spoolCv.wait_for(lock, std::chrono::seconds(10), [&] { return spoolReleased; }))
                throw nix::Error("fixture: spooled upload gate timed out");
        };
        workerService.setReverseUploadSpooledHook(spoolGate);
        worker2Service.setReverseUploadSpooledHook(spoolGate);
        spooled.context = std::make_unique<grpc::ClientContext>();
        spooled.context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        spooled.stream = schedule->Schedule(spooled.context.get());
        nix::remote::ClientMsgs wantSpooled;
        wantSpooled.add_msgs()->mutable_want()->set_drv_path(spooled.drv);
        wantSpooled.mutable_msgs(0)->mutable_want()->set_system(nix::settings.thisSystem.get());
        assert(spooled.stream->Write(wantSpooled));
        nix::remote::SchedMsgs spooledReply;
        while (!spooled.assignment && spooled.stream->Read(&spooledReply))
            for (auto const & msg : spooledReply.msgs())
                if (msg.has_assigned() && msg.assigned().drv_path() == spooled.drv) {
                    assert(msg.assigned().reverse());
                    spooled.assignment = msg.assigned().assign_id();
                }
        assert(spooled.assignment);
        auto const spoolPod = scheduler.dispatcher->assignmentPod(spooled.drv, spooled.assignment);
        assert(spoolPod);
        spooled.pod = *spoolPod;
        auto & spoolWorker = spooled.pod == workerOptions.reverseWorkerId ? worker : worker2;
        auto const spoolExpectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!spoolWorker.builder->expects(spooled.drv, spooled.assignment)
               && std::chrono::steady_clock::now() < spoolExpectDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(spoolWorker.builder->expects(spooled.drv, spooled.assignment));
        grpc::Status spooledStatus;
        grpc::ClientContext spoolContext;
        spoolContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        std::thread completeUpload([&] {
            nix::remote::AddMultipleReply reply;
            auto writer = client->UploadBuildInputs(&spoolContext, &reply);
            nix::remote::UploadBuildInputsChunk header;
            header.set_drv_path(spooled.drv);
            header.set_assign_id(spooled.assignment);
            header.mutable_chunk()->set_check_sigs(true);
            if (!writer->Write(header)) { spooledStatus = writer->Finish(); return; }
            for (size_t offset = 0; offset < spooled.packed.size(); offset += 128 * 1024) {
                nix::remote::UploadBuildInputsChunk data;
                data.mutable_chunk()->set_data(spooled.packed.substr(offset, 128 * 1024));
                if (!writer->Write(data)) { spooledStatus = writer->Finish(); return; }
            }
            writer->WritesDone();
            spooledStatus = writer->Finish();
        });
        {
            std::unique_lock lock(spoolMutex);
            assert(spoolCv.wait_for(lock, std::chrono::seconds(15), [&] { return spoolArrived; }));
        }
        assert(scheduler.dispatcher->assignmentPod(spooled.drv, spooled.assignment) == spooled.pod);
        assert(spoolWorker.builder->expects(spooled.drv, spooled.assignment));
        nix::remote::ClientMsgs cancelSpooled;
        cancelSpooled.add_msgs()->mutable_cancel()->set_drv_path(spooled.drv);
        assert(spooled.stream->Write(cancelSpooled));
        auto const spoolRevokedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (scheduler.dispatcher->assignmentPod(spooled.drv, spooled.assignment)
               && std::chrono::steady_clock::now() < spoolRevokedDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(!scheduler.dispatcher->assignmentPod(spooled.drv, spooled.assignment));
        {
            const std::scoped_lock lock(spoolMutex);
            spoolReleased = true;
        }
        spoolCv.notify_all();
        completeUpload.join();
        assert(!spooledStatus.ok());
        assert(!workerStore->isValidPath(source->parseStorePath(spooled.path)));
        assert(!worker2Store->isValidPath(source->parseStorePath(spooled.path)));
        if (worker.cache.hasRemote()) {
            auto cache = nix::openStore(nix::readFile(dir + "/cache-url"));
            assert(!cache->isValidPath(source->parseStorePath(spooled.path)));
        }
        spooled.stream->WritesDone();
        spooled.stream->Finish();
        workerService.setReverseUploadSpooledHook({});
        worker2Service.setReverseUploadSpooledHook({});
        std::cerr << "fully spooled pinned upload revoked before store apply\n";
        // Upload an actual signed derivation before starting its build, then
        // revoke while the real nix-daemon builder runs a shell-builtin loop.
        auto const cancelPath = source->parseStorePath(nix::chomp(nix::readFile(dir + "/cancel.drv")));
        auto const cancelDrv = std::string(cancelPath.to_string());
        auto const cancelBasic = nixcompat::toBasicDrv(source->readDerivation(cancelPath),
            [&](const nix::StorePath &, const std::string &) -> nix::StorePath {
                throw nix::Error("unexpected cancellation input derivation");
            });
        assert(cancelBasic);
        auto const cancelOutput = cancelBasic->outputs.at("out").path(*source, cancelBasic->name, "out");
        assert(cancelOutput);
        grpc::ClientContext cancelScheduleContext;
        cancelScheduleContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        auto cancelSchedule = schedule->Schedule(&cancelScheduleContext);
        nix::remote::ClientMsgs cancelWant;
        cancelWant.add_msgs()->mutable_want()->set_drv_path(cancelDrv);
        cancelWant.mutable_msgs(0)->mutable_want()->set_system(nix::settings.thisSystem.get());
        assert(cancelSchedule->Write(cancelWant));
        uint64_t cancelAssignment = 0;
        nix::remote::SchedMsgs cancelReply;
        while (!cancelAssignment && cancelSchedule->Read(&cancelReply))
            for (auto const & msg : cancelReply.msgs())
                if (msg.has_assigned() && msg.assigned().drv_path() == cancelDrv) {
                    assert(msg.assigned().reverse());
                    cancelAssignment = msg.assigned().assign_id();
                }
        assert(cancelAssignment);
        auto const cancelPod = scheduler.dispatcher->assignmentPod(cancelDrv, cancelAssignment);
        assert(cancelPod);
        auto & cancelWorker = *cancelPod == workerOptions.reverseWorkerId ? worker : worker2;
        auto & cancelStore = *cancelPod == workerOptions.reverseWorkerId ? workerStore : worker2Store;
        auto const cancelExpectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!cancelWorker.builder->expects(cancelDrv, cancelAssignment)
               && std::chrono::steady_clock::now() < cancelExpectDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(cancelWorker.builder->expects(cancelDrv, cancelAssignment));
        nix::StringSink cancelWire;
        cancelWire << static_cast<uint64_t>(1);
        nix::WorkerProto::Serialise<nix::ValidPathInfo>::write(
            *source, nix::WorkerProto::WriteConn{.to = cancelWire, .version = nixcompat::infoProtocolVersion()},
            *source->queryPathInfo(cancelPath));
        source->narFromPath(cancelPath, cancelWire);
        std::string cancelPacked(ZSTD_compressBound(cancelWire.s.size()), '\0');
        auto const cancelPackedSize = ZSTD_compress(cancelPacked.data(), cancelPacked.size(),
                                                    cancelWire.s.data(), cancelWire.s.size(), 1);
        assert(!ZSTD_isError(cancelPackedSize));
        cancelPacked.resize(cancelPackedSize);
        {
            grpc::ClientContext uploadContext;
            uploadContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(20));
            nix::remote::AddMultipleReply reply;
            auto writer = client->UploadBuildInputs(&uploadContext, &reply);
            nix::remote::UploadBuildInputsChunk header;
            header.set_drv_path(cancelDrv);
            header.set_assign_id(cancelAssignment);
            header.mutable_chunk()->set_check_sigs(true);
            assert(writer->Write(header));
            nix::remote::UploadBuildInputsChunk data;
            data.mutable_chunk()->set_data(std::move(cancelPacked));
            assert(writer->Write(data));
            assert(writer->WritesDone());
            auto const uploaded = writer->Finish();
            if (!uploaded.ok()) std::cerr << "cancel build upload: " << uploaded.error_message() << "\n";
            assert(uploaded.ok() && cancelStore->isValidPath(cancelPath));
        }
        nix::remote::BuildDerivationRequest cancelRequest;
        cancelRequest.set_drv_path(cancelDrv);
        cancelRequest.set_assign_id(cancelAssignment);
        cancelRequest.set_build_mode(static_cast<uint32_t>(nix::bmNormal));
        cancelRequest.set_protocol(nixcompat::kBuildProtocolWire);
        nix::StringSink cancelDrvWire;
        nixcompat::writeDrv(cancelDrvWire, *source, *cancelBasic);
        *cancelRequest.mutable_drv() = std::move(cancelDrvWire.s);
        grpc::ClientContext runningContext;
        runningContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(25));
        auto running = client->BuildDerivation(&runningContext, cancelRequest);
        nix::remote::BuildDerivationChunk runningChunk;
        bool sawRunning = false;
        bool acceptedResult = false;
        while (running->Read(&runningChunk)) {
            if (runningChunk.has_done()) { acceptedResult = true; break; }
            if (runningChunk.ShortDebugString().find("reverse-cancel-running") != std::string::npos) {
                sawRunning = true;
                break;
            }
        }
        assert(sawRunning && !acceptedResult);
        assert(scheduler.dispatcher->assignmentPod(cancelDrv, cancelAssignment) == cancelPod);
        assert(!workerStore->isValidPath(*cancelOutput) && !worker2Store->isValidPath(*cancelOutput));
        nix::remote::ClientMsgs cancelRunning;
        cancelRunning.add_msgs()->mutable_cancel()->set_drv_path(cancelDrv);
        assert(cancelSchedule->Write(cancelRunning));
        auto const buildRevokeDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (scheduler.dispatcher->assignmentPod(cancelDrv, cancelAssignment)
               && std::chrono::steady_clock::now() < buildRevokeDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(!scheduler.dispatcher->assignmentPod(cancelDrv, cancelAssignment));
        while (running->Read(&runningChunk)) if (runningChunk.has_done()) acceptedResult = true;
        auto const cancelledBuild = running->Finish();
        assert(!cancelledBuild.ok() && !acceptedResult);
        assert(!workerStore->isValidPath(*cancelOutput) && !worker2Store->isValidPath(*cancelOutput));
        if (worker.cache.hasRemote()) {
            auto cache = nix::openStore(nix::readFile(dir + "/cache-url"));
            assert(!cache->isValidPath(*cancelOutput));
        }
        cancelSchedule->WritesDone();
        cancelSchedule->Finish();
        std::cerr << "running reverse build revoked without accepting a stale result\n";
        // Displace a live data registration while a fully spooled import is
        // held before apply. Keep the replacement stream open so the old
        // worker cannot silently reconnect and become current again.
        auto displaced = makeUpload("55555555555555555555555555555555-reverse-displaced.drv", "reverse-displaced");
        std::mutex displacementMutex;
        std::condition_variable displacementCv;
        bool displacementArrived = false;
        bool displacementReleased = false;
        auto const displacementGate = [&](const std::string & drv) {
            if (drv != displaced.drv) return;
            std::unique_lock lock(displacementMutex);
            displacementArrived = true;
            displacementCv.notify_all();
            if (!displacementCv.wait_for(lock, std::chrono::seconds(12), [&] { return displacementReleased; }))
                throw nix::Error("fixture: displaced upload gate timed out");
        };
        workerService.setReverseUploadSpooledHook(displacementGate);
        worker2Service.setReverseUploadSpooledHook(displacementGate);
        displaced.context = std::make_unique<grpc::ClientContext>();
        displaced.context->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(35));
        displaced.stream = schedule->Schedule(displaced.context.get());
        nix::remote::ClientMsgs displacedWant;
        displacedWant.add_msgs()->mutable_want()->set_drv_path(displaced.drv);
        displacedWant.mutable_msgs(0)->mutable_want()->set_system(nix::settings.thisSystem.get());
        assert(displaced.stream->Write(displacedWant));
        nix::remote::SchedMsgs displacedReply;
        while (!displaced.assignment && displaced.stream->Read(&displacedReply))
            for (auto const & msg : displacedReply.msgs())
                if (msg.has_assigned() && msg.assigned().drv_path() == displaced.drv) {
                    assert(msg.assigned().reverse());
                    displaced.assignment = msg.assigned().assign_id();
                }
        assert(displaced.assignment);
        auto const displacedPod = scheduler.dispatcher->assignmentPod(displaced.drv, displaced.assignment);
        assert(displacedPod);
        displaced.pod = *displacedPod;
        auto const oldSession = scheduler.reverseRegistry.find(displaced.pod);
        assert(oldSession && scheduler.reverseRegistry.current(oldSession));
        auto & displacedWorker = displaced.pod == workerOptions.reverseWorkerId ? worker : worker2;
        auto const displacedExpectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!displacedWorker.builder->expects(displaced.drv, displaced.assignment)
               && std::chrono::steady_clock::now() < displacedExpectDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(displacedWorker.builder->expects(displaced.drv, displaced.assignment));
        grpc::Status displacedStatus;
        grpc::ClientContext displacedContext;
        displacedContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(25));
        std::thread displacedUpload([&] {
            nix::remote::AddMultipleReply reply;
            auto writer = client->UploadBuildInputs(&displacedContext, &reply);
            nix::remote::UploadBuildInputsChunk header;
            header.set_drv_path(displaced.drv);
            header.set_assign_id(displaced.assignment);
            header.mutable_chunk()->set_check_sigs(true);
            if (!writer->Write(header)) { displacedStatus = writer->Finish(); return; }
            for (size_t offset = 0; offset < displaced.packed.size(); offset += 128 * 1024) {
                nix::remote::UploadBuildInputsChunk data;
                data.mutable_chunk()->set_data(displaced.packed.substr(offset, 128 * 1024));
                if (!writer->Write(data)) { displacedStatus = writer->Finish(); return; }
            }
            writer->WritesDone();
            displacedStatus = writer->Finish();
        });
        {
            std::unique_lock lock(displacementMutex);
            assert(displacementCv.wait_for(lock, std::chrono::seconds(15), [&] { return displacementArrived; }));
        }
        assert(scheduler.dispatcher->assignmentPod(displaced.drv, displaced.assignment) == displaced.pod);
        grpc::SslCredentialsOptions replacementTls;
        replacementTls.pem_root_certs = nix::readFile(dir + "/server.crt");
        replacementTls.pem_private_key = nix::readFile(dir + "/server.key");
        replacementTls.pem_cert_chain = nix::readFile(dir + "/server.crt");
        auto replacementChannel = grpc::CreateChannel("localhost:" + std::to_string(farmPort),
            grpc::SslCredentials(replacementTls));
        auto replacement = nix::remote::ReverseSession::NewStub(replacementChannel);
        grpc::ClientContext replacementContext;
        replacementContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(20));
        replacementContext.AddMetadata("authorization", "Bearer " + nix::chomp(nix::readFile(
            dir + (displaced.pod == workerOptions.reverseWorkerId ? "/token" : "/token2"))));
        auto replacementStream = replacement->Register(&replacementContext);
        nix::remote::ReverseWorkerMsg registrationFrame;
        registrationFrame.set_worker_id(displaced.pod);
        registrationFrame.set_protocol(nixgrpc::reverseProtocolVersion);
        assert(replacementStream->Write(registrationFrame));
        nix::remote::ReverseFarmMsg replacementAck;
        assert(replacementStream->Read(&replacementAck) && replacementAck.generation() > oldSession->generation);
        assert(!scheduler.reverseRegistry.current(oldSession));
        assert(scheduler.reverseRegistry.find(displaced.pod)->generation == replacementAck.generation());
        displacedUpload.join();
        assert(!displacedStatus.ok());
        {
            const std::scoped_lock lock(displacementMutex);
            displacementReleased = true;
        }
        displacementCv.notify_all();
        auto const displacedPath = source->parseStorePath(displaced.path);
        assert(!workerStore->isValidPath(displacedPath) && !worker2Store->isValidPath(displacedPath));
        if (worker.cache.hasRemote()) {
            auto cache = nix::openStore(nix::readFile(dir + "/cache-url"));
            assert(!cache->isValidPath(displacedPath));
        }
        replacementContext.TryCancel();
        replacementStream->Finish();
        displaced.stream->WritesDone();
        displaced.stream->Finish();
        workerService.setReverseUploadSpooledHook({});
        worker2Service.setReverseUploadSpooledHook({});
        std::cerr << "replaced reverse session fenced an in-flight upload without rerouting\n";
        // The preceding test's temporary registration has gone away. Wait
        // for the actual worker to regain its data session before scheduling
        // new work; an assignment alone does not imply a data route exists.
        auto const restoredDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
        auto restored = scheduler.reverseRegistry.find(displaced.pod);
        while ((!restored || restored->generation <= replacementAck.generation())
               && std::chrono::steady_clock::now() < restoredDeadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            restored = scheduler.reverseRegistry.find(displaced.pod);
        }
        assert(restored && restored->generation > replacementAck.generation());
        // Disconnect an actual worker-owned data registration during a real
        // running build. The control session remains up, and the worker must
        // reconnect with a fresh generation without accepting the old result.
        auto const disconnectPath = source->parseStorePath(nix::chomp(nix::readFile(dir + "/disconnect.drv")));
        auto const disconnectDrv = std::string(disconnectPath.to_string());
        auto const disconnectBasic = nixcompat::toBasicDrv(source->readDerivation(disconnectPath),
            [&](const nix::StorePath &, const std::string &) -> nix::StorePath {
                throw nix::Error("unexpected disconnect input derivation");
            });
        assert(disconnectBasic);
        auto const disconnectOutput = disconnectBasic->outputs.at("out").path(*source, disconnectBasic->name, "out");
        assert(disconnectOutput);
        grpc::ClientContext disconnectScheduleContext;
        disconnectScheduleContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(35));
        auto disconnectSchedule = schedule->Schedule(&disconnectScheduleContext);
        nix::remote::ClientMsgs disconnectWant;
        disconnectWant.add_msgs()->mutable_want()->set_drv_path(disconnectDrv);
        disconnectWant.mutable_msgs(0)->mutable_want()->set_system(nix::settings.thisSystem.get());
        assert(disconnectSchedule->Write(disconnectWant));
        uint64_t disconnectAssignment = 0;
        nix::remote::SchedMsgs disconnectReply;
        while (!disconnectAssignment && disconnectSchedule->Read(&disconnectReply))
            for (auto const & msg : disconnectReply.msgs())
                if (msg.has_assigned() && msg.assigned().drv_path() == disconnectDrv) {
                    assert(msg.assigned().reverse());
                    disconnectAssignment = msg.assigned().assign_id();
                }
        assert(disconnectAssignment);
        auto const disconnectPod = scheduler.dispatcher->assignmentPod(disconnectDrv, disconnectAssignment);
        assert(disconnectPod);
        auto & disconnectWorker = *disconnectPod == workerOptions.reverseWorkerId ? worker : worker2;
        auto & disconnectStore = *disconnectPod == workerOptions.reverseWorkerId ? workerStore : worker2Store;
        auto const disconnectExpectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!disconnectWorker.builder->expects(disconnectDrv, disconnectAssignment)
               && std::chrono::steady_clock::now() < disconnectExpectDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(disconnectWorker.builder->expects(disconnectDrv, disconnectAssignment));
        nix::StringSink disconnectWire;
        disconnectWire << static_cast<uint64_t>(1);
        nix::WorkerProto::Serialise<nix::ValidPathInfo>::write(
            *source, nix::WorkerProto::WriteConn{.to = disconnectWire, .version = nixcompat::infoProtocolVersion()},
            *source->queryPathInfo(disconnectPath));
        source->narFromPath(disconnectPath, disconnectWire);
        std::string disconnectPacked(ZSTD_compressBound(disconnectWire.s.size()), '\0');
        auto const disconnectPackedSize = ZSTD_compress(disconnectPacked.data(), disconnectPacked.size(),
                                                        disconnectWire.s.data(), disconnectWire.s.size(), 1);
        assert(!ZSTD_isError(disconnectPackedSize));
        disconnectPacked.resize(disconnectPackedSize);
        {
            grpc::ClientContext uploadContext;
            uploadContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(20));
            nix::remote::AddMultipleReply reply;
            auto writer = client->UploadBuildInputs(&uploadContext, &reply);
            nix::remote::UploadBuildInputsChunk header;
            header.set_drv_path(disconnectDrv);
            header.set_assign_id(disconnectAssignment);
            header.mutable_chunk()->set_check_sigs(true);
            assert(writer->Write(header));
            nix::remote::UploadBuildInputsChunk data;
            data.mutable_chunk()->set_data(std::move(disconnectPacked));
            assert(writer->Write(data) && writer->WritesDone());
            auto const uploaded = writer->Finish();
            if (!uploaded.ok()) std::cerr << "disconnect build upload: " << uploaded.error_message() << "\n";
            assert(uploaded.ok() && disconnectStore->isValidPath(disconnectPath));
        }
        nix::remote::BuildDerivationRequest disconnectRequest;
        disconnectRequest.set_drv_path(disconnectDrv);
        disconnectRequest.set_assign_id(disconnectAssignment);
        disconnectRequest.set_build_mode(static_cast<uint32_t>(nix::bmNormal));
        disconnectRequest.set_protocol(nixcompat::kBuildProtocolWire);
        nix::StringSink disconnectDrvWire;
        nixcompat::writeDrv(disconnectDrvWire, *source, *disconnectBasic);
        *disconnectRequest.mutable_drv() = std::move(disconnectDrvWire.s);
        grpc::ClientContext disconnectedContext;
        disconnectedContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(25));
        auto disconnectedBuild = client->BuildDerivation(&disconnectedContext, disconnectRequest);
        nix::remote::BuildDerivationChunk disconnectChunk;
        bool sawDisconnectRunning = false;
        bool receivedDisconnectResult = false;
        while (disconnectedBuild->Read(&disconnectChunk)) {
            if (disconnectChunk.has_done()) { receivedDisconnectResult = true; break; }
            if (disconnectChunk.ShortDebugString().find("reverse-disconnect-running") != std::string::npos) {
                sawDisconnectRunning = true;
                break;
            }
        }
        assert(sawDisconnectRunning && !receivedDisconnectResult);
        assert(scheduler.dispatcher->assignmentPod(disconnectDrv, disconnectAssignment) == disconnectPod);
        assert(!workerStore->isValidPath(*disconnectOutput) && !worker2Store->isValidPath(*disconnectOutput));
        auto const disconnectedSession = scheduler.reverseRegistry.find(*disconnectPod);
        assert(disconnectedSession && scheduler.reverseRegistry.current(disconnectedSession));
        // This closes the real Register transport and cancels its context;
        // unlike the preceding synthetic registration it lets the worker's
        // reconnect loop establish the successor itself.
        scheduler.reverseRegistry.disconnected(disconnectedSession);
        assert(!scheduler.reverseRegistry.current(disconnectedSession));
        while (disconnectedBuild->Read(&disconnectChunk))
            if (disconnectChunk.has_done()) receivedDisconnectResult = true;
        auto const disconnectStatus = disconnectedBuild->Finish();
        assert(disconnectStatus.error_code() == grpc::StatusCode::UNAVAILABLE && !receivedDisconnectResult);
        auto const reconnectedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
        auto reconnected = scheduler.reverseRegistry.find(*disconnectPod);
        while ((!reconnected || reconnected->generation <= disconnectedSession->generation)
               && std::chrono::steady_clock::now() < reconnectedDeadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            reconnected = scheduler.reverseRegistry.find(*disconnectPod);
        }
        assert(reconnected && reconnected->generation > disconnectedSession->generation);
        assert(!workerStore->isValidPath(*disconnectOutput) && !worker2Store->isValidPath(*disconnectOutput));
        auto const retiredDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (scheduler.dispatcher->assignmentPod(disconnectDrv, disconnectAssignment)
               && std::chrono::steady_clock::now() < retiredDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(!scheduler.dispatcher->assignmentPod(disconnectDrv, disconnectAssignment));
        std::cerr << "disconnected build assignment retired after reconnect\n";
        assert(!scheduler.dispatcher->podOwnsAssignment(
            *disconnectPod == workerOptions.reverseWorkerId ? worker2Options.reverseWorkerId : workerOptions.reverseWorkerId,
            disconnectDrv, disconnectAssignment));
        // Do not prime Nix's negative narinfo cache for this path: the
        // successful retry below publishes it to the same cache URL.
        // Release the only follower even if the control assignment outlived
        // the data connection; no subsequent request may use this old ID.
        nix::remote::ClientMsgs cancelDisconnected;
        cancelDisconnected.add_msgs()->mutable_cancel()->set_drv_path(disconnectDrv);
        assert(disconnectSchedule->Write(cancelDisconnected));
        auto const disconnectRevokedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (scheduler.dispatcher->assignmentPod(disconnectDrv, disconnectAssignment)
               && std::chrono::steady_clock::now() < disconnectRevokedDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(!scheduler.dispatcher->assignmentPod(disconnectDrv, disconnectAssignment));
        auto const oldBuildEndedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (disconnectWorker.builder->expects(disconnectDrv, disconnectAssignment)
               && std::chrono::steady_clock::now() < oldBuildEndedDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(!disconnectWorker.builder->expects(disconnectDrv, disconnectAssignment));
        grpc::ClientContext staleContext;
        staleContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto staleBuild = client->BuildDerivation(&staleContext, disconnectRequest);
        bool staleDone = false;
        while (staleBuild->Read(&disconnectChunk)) if (disconnectChunk.has_done()) staleDone = true;
        assert(!staleBuild->Finish().ok() && !staleDone);
        disconnectSchedule->WritesDone();
        disconnectSchedule->Finish();
        std::cerr << "interrupted running build failed closed across worker data-session reconnection\n";
        // The old attempt has exited, its assignment is gone, and its output
        // is absent. Request the same derivation through the real GrpcStore
        // Schedule/NOT_FOUND/upload/build path, with a bounded builder delay.
        assert(!workerStore->isValidPath(*disconnectOutput) && !worker2Store->isValidPath(*disconnectOutput));
        std::atomic<uint64_t> recoveredOnFirst{0};
        std::atomic<uint64_t> recoveredOnSecond{0};
        worker.setReverseBuild([&](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
            if (call.delegated.drvPath == disconnectDrv)
                recoveredOnFirst = call.delegated.assignId;
            return workerService.reverseBuild(call, emit, cancelled, afterFinish);
        });
        worker2.setReverseBuild([&](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
            if (call.delegated.drvPath == disconnectDrv)
                recoveredOnSecond = call.delegated.assignId;
            return worker2Service.reverseBuild(call, emit, cancelled, afterFinish);
        });
        clientDrivenRecoveredReverseBuild(farmUri, "local?root=" + dir + "/source-store",
                                          source->printStorePath(disconnectPath));
        auto const onFirst = workerStore->isValidPath(*disconnectOutput);
        auto const onSecond = worker2Store->isValidPath(*disconnectOutput);
        assert(onFirst != onSecond);
        assert((onFirst ? recoveredOnFirst.load() : recoveredOnSecond.load()) != 0);
        assert((onFirst ? recoveredOnFirst.load() : recoveredOnSecond.load()) != disconnectAssignment);
        assert((onFirst ? recoveredOnSecond.load() : recoveredOnFirst.load()) == 0);
        auto const recoveredStore = nix::openStore("local?root=" + dir + (onFirst ? "/worker-store" : "/worker2-store"));
        nix::StringSink recoveredNar;
        recoveredStore->narFromPath(*disconnectOutput, recoveredNar);
        assert(recoveredNar.s.find("reverse-recovered") != std::string::npos);
        if (worker.cache.hasRemote()) {
            auto cache = nix::openStore(nix::readFile(dir + "/cache-url"));
            auto reader = nix::openStore("local?root=" + dir + "/recovered-reader-store");
            assert(!reader->isValidPath(*disconnectOutput));
            nix::copyPaths(*cache, *reader, nix::StorePathSet{*disconnectOutput});
            nix::StringSink cachedNar;
            reader->narFromPath(*disconnectOutput, cachedNar);
            assert(cachedNar.s == recoveredNar.s);
        }
        std::cerr << "fresh client-driven assignment rebuilt interrupted derivation on exactly one worker\n";
        // A separate derivation uses one uninterrupted GrpcStore invocation.
        // Observe the real worker's build log, disconnect that worker's data
        // session, then let GrpcStore request a replacement assignment itself.
        auto const autoPath = source->parseStorePath(nix::chomp(nix::readFile(dir + "/auto.drv")));
        auto const autoDrv = std::string(autoPath.to_string());
        auto const autoBasic = nixcompat::toBasicDrv(source->readDerivation(autoPath),
            [&](const nix::StorePath &, const std::string &) -> nix::StorePath {
                throw nix::Error("unexpected auto retry input derivation");
            });
        assert(autoBasic);
        auto const autoOutput = autoBasic->outputs.at("out").path(*source, autoBasic->name, "out");
        assert(autoOutput);
        std::mutex autoMutex;
        std::condition_variable autoCv;
        std::string autoPod;
        uint64_t autoOriginalId = 0;
        std::atomic<uint64_t> autoRetriedId{0};
        auto const observeAuto = [&](auto & service, const std::string & pod, auto const & call,
                                     auto const & emit, auto const & cancelled, auto & afterFinish) -> grpc::Status {
            if (call.delegated.drvPath == autoDrv) {
                std::scoped_lock lock(autoMutex);
                if (autoOriginalId && call.delegated.assignId != autoOriginalId) {
                    autoRetriedId = call.delegated.assignId;
                }
            }
            auto watched = [&](const nix::remote::BuildDerivationChunk & chunk) {
                if (call.delegated.drvPath == autoDrv
                    && chunk.ShortDebugString().find("reverse-auto-running") != std::string::npos) {
                    std::scoped_lock lock(autoMutex);
                    if (autoPod.empty()) {
                        autoPod = pod;
                        autoOriginalId = call.delegated.assignId;
                        autoCv.notify_all();
                    }
                }
                return emit(chunk);
            };
            return service.reverseBuild(call, watched, cancelled, afterFinish);
        };
        worker.setReverseBuild([&](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
            return observeAuto(workerService, workerOptions.reverseWorkerId, call, emit, cancelled, afterFinish);
        });
        worker2.setReverseBuild([&](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
            return observeAuto(worker2Service, worker2Options.reverseWorkerId, call, emit, cancelled, afterFinish);
        });
        std::thread autoClient([&] {
            clientDrivenRecoveredReverseBuild(farmUri, "local?root=" + dir + "/source-store",
                                              source->printStorePath(autoPath));
        });
        {
            std::unique_lock lock(autoMutex);
            assert(autoCv.wait_for(lock, std::chrono::seconds(15), [&] { return !autoPod.empty(); }));
        }
        assert(autoOriginalId && scheduler.dispatcher->assignmentPod(autoDrv, autoOriginalId) == autoPod);
        assert(!workerStore->isValidPath(*autoOutput) && !worker2Store->isValidPath(*autoOutput));
        auto const autoSession = scheduler.reverseRegistry.find(autoPod);
        assert(autoSession && scheduler.reverseRegistry.current(autoSession));
        scheduler.reverseRegistry.disconnected(autoSession);
        autoClient.join();
        assert(!scheduler.dispatcher->assignmentPod(autoDrv, autoOriginalId));
        assert(autoRetriedId && autoRetriedId != autoOriginalId);
        auto const autoOnFirst = workerStore->isValidPath(*autoOutput);
        auto const autoOnSecond = worker2Store->isValidPath(*autoOutput);
        assert(autoOnFirst != autoOnSecond);
        auto const autoDoneDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (scheduler.dispatcher->assignmentPod(autoDrv, autoRetriedId)
               && std::chrono::steady_clock::now() < autoDoneDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        assert(!scheduler.dispatcher->assignmentPod(autoDrv, autoRetriedId));
        auto const autoStore = nix::openStore("local?root=" + dir + (autoOnFirst ? "/worker-store" : "/worker2-store"));
        nix::StringSink autoNar;
        autoStore->narFromPath(*autoOutput, autoNar);
        assert(autoNar.s.find("reverse-auto-recovered") != std::string::npos);
        if (worker.cache.hasRemote()) {
            auto cache = nix::openStore(nix::readFile(dir + "/cache-url"));
            auto reader = nix::openStore("local?root=" + dir + "/auto-reader-store");
            nix::copyPaths(*cache, *reader, nix::StorePathSet{*autoOutput});
            nix::StringSink cachedNar;
            reader->narFromPath(*autoOutput, cachedNar);
            assert(cachedNar.s == autoNar.s);
        }
        std::cerr << "one client invocation recovered a disconnected reverse build and read its output\n";
        {
            // A build whose publish fails delivers no result, so the gateway
            // never acknowledges it. The worker must still retire the
            // assignment at once; otherwise the scheduler keeps the slot busy
            // until the data session drops and a retry finds "not scheduled
            // here" (as when a worker cannot reach the cache).
            auto const failPath = source->parseStorePath(nix::chomp(nix::readFile(dir + "/publishfail.drv")));
            auto const failDrv = std::string(failPath.to_string());
            std::atomic<int> failedPublishes{0};
            auto const failOnce = [&](const std::string & drv) {
                if (drv == failDrv && failedPublishes.fetch_add(1) == 0)
                    throw nix::Error("fixture: cache unreachable");
            };
            workerService.setPublishHook(failOnce);
            worker2Service.setPublishHook(failOnce);
            auto const givenBack = metrics.eventCounter("done_GIVEN_BACK").Value();
            clientDrivenRecoveredReverseBuild(farmUri, "local?root=" + dir + "/source-store",
                                              source->printStorePath(failPath));
            workerService.setPublishHook({});
            worker2Service.setPublishHook({});
            // Exactly one failed publish, given back with a Done, then a retry
            // that built and published.
            assert(failedPublishes.load() == 2);
            assert(metrics.eventCounter("done_GIVEN_BACK").Value() == givenBack + 1);
            std::cerr << "a failed publish retired its assignment and the retry built the output\n";
        }
        worker.setReverseUpload([&workerService](auto const & call, auto const & cancelled) {
            return workerService.reverseUpload(call, cancelled);
        });
        worker2.setReverseUpload([&worker2Service](auto const & call, auto const & cancelled) {
            return worker2Service.reverseUpload(call, cancelled);
        });
        std::shared_ptr<nix::Store> cacheStore;
        std::shared_ptr<nix::Store> readerStore;
        if (worker.cache.hasRemote()) {
            cacheStore = nix::openStore(nix::readFile(dir + "/cache-url")).get_ptr();
            readerStore = nix::openStore("local?root=" + dir + "/concurrent-reader-store").get_ptr();
        }
        for (auto const & drv : {first, second}) {
            auto const drvPath = source->parseStorePath(drv);
            auto const basic = nixcompat::toBasicDrv(source->readDerivation(drvPath),
                [&](const nix::StorePath &, const std::string &) -> nix::StorePath {
                    throw nix::Error("unexpected concurrent input derivation");
                });
            assert(basic);
            auto const built = basic->outputs.at("out").path(*source, basic->name, "out");
            assert(built);
            auto const onFirst = workerStore->isValidPath(drvPath);
            auto const onSecond = worker2Store->isValidPath(drvPath);
            assert(onFirst != onSecond);
            auto const outputStore = nix::openStore("local?root=" + dir + (onFirst ? "/worker-store" : "/worker2-store"));
            assert((onFirst ? workerStore : worker2Store)->isValidPath(*built));
            assert(!(onFirst ? worker2Store : workerStore)->isValidPath(*built));
            nix::StringSink outputNar;
            outputStore->narFromPath(*built, outputNar);
            assert(outputNar.s.find("reverse-" + basic->name.substr(std::string("reverse-concurrent-").size())) != std::string::npos);
            if (cacheStore) {
                assert(!readerStore->isValidPath(*built));
                nix::copyPaths(*cacheStore, *readerStore, nix::StorePathSet{*built});
                nix::StringSink cacheNar;
                readerStore->narFromPath(*built, cacheNar);
                assert(cacheNar.s == outputNar.s);
            }
        }
        assert(workerStore->isValidPath(source->parseStorePath(first)) != workerStore->isValidPath(source->parseStorePath(second)));
        std::cerr << "two concurrent reverse builds landed on distinct workers and outputs read back\n";
        // A cert-only direct worker must still use the ordinary local build
        // and upload path, without any pod assignment or reverse registration.
        auto directOptions = schedulerOptions;
        directOptions.builder = true;
        directOptions.maxJobs = 2;
        directOptions.socketPath = worker2Options.socketPath;
        directOptions.storeUri = worker2Options.storeUri;
        directOptions.advertise = "direct-worker";
        nixgrpc::Coordinator direct(directOptions, schedulerAuth, metrics, idle);
        NixRemoteService directService(directOptions.socketPath, directOptions.storeUri, "direct", metrics, idle,
                                       nixgrpc::LogLevel::info, schedulerAuth, direct);
        grpc::ServerBuilder directBuilder;
        int directPort = 0;
        directBuilder.AddListeningPort("127.0.0.1:0", nixgrpc::makeServerCredentials(directOptions), &directPort);
        directBuilder.RegisterService(direct.scheduler.get());
        directBuilder.RegisterService(&directService);
        auto directServer = directBuilder.BuildAndStart();
        assert(directServer && directPort > 0);
        direct.start(*directServer);
        auto const directPath = source->parseStorePath(nix::chomp(nix::readFile(dir + "/direct.drv")));
        assert(!worker2Store->isValidPath(directPath));
        auto const directUri = "grpc://localhost:" + std::to_string(directPort)
            + "?ca-cert=" + dir + "/server.crt&client-cert=" + dir + "/client.crt&client-key=" + dir + "/client.key"
        + "&reverse-uploads=true";
        clientDrivenRecoveredReverseBuild(directUri, "local?root=" + dir + "/source-store",
                                          source->printStorePath(directPath));
        auto const directBasic = nixcompat::toBasicDrv(source->readDerivation(directPath),
            [&](const nix::StorePath &, const std::string &) -> nix::StorePath {
                throw nix::Error("unexpected direct input derivation");
            });
        assert(directBasic);
        auto const directOutput = directBasic->outputs.at("out").path(*source, directBasic->name, "out");
        assert(directOutput && worker2Store->isValidPath(*directOutput));
        assert(!workerStore->isValidPath(directPath) && !workerStore->isValidPath(*directOutput));
        assert(!direct.reverseRegistry.find("direct-worker"));
        nix::StringSink directNar;
        nix::openStore("local?root=" + dir + "/worker2-store")->narFromPath(*directOutput, directNar);
        assert(directNar.s.find("direct-worker-ok") != std::string::npos);
        directServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        directServer->Wait();
        std::cerr << "direct worker built through the unchanged local path without a reverse pod route\n";
        // Two independent production service stacks: a passive scheduler
        // rejects traffic, then takes over with a worker connecting to its
        // own TLS endpoint. This is a local service handover, not Envoy/lock
        // election or a running-build migration proof.
        nixgrpc::Coordinator standby(schedulerOptions, schedulerAuth, metrics, idle);
        standby.scheduler->setReverseIdentity(identity);
        nixgrpc::ReverseSessionService standbyRegistration(identity, standby.reverseRegistry, schedulerAuth);
        NixRemoteService standbyGateway("", schedulerOptions.storeUri, "standby", metrics, idle,
                                        nixgrpc::LogLevel::info, schedulerAuth, standby);
        grpc::ServerBuilder standbyBuilder;
        int standbyPort = 0;
        standbyBuilder.AddListeningPort("127.0.0.1:0", nixgrpc::makeServerCredentials(schedulerOptions), &standbyPort);
        standbyBuilder.RegisterService(standby.scheduler.get());
        standbyBuilder.RegisterService(&standbyRegistration);
        standbyBuilder.RegisterService(&standbyGateway);
        auto standbyServer = standbyBuilder.BuildAndStart();
        assert(standbyServer && standbyPort > 0);
        standby.scheduler->setActive(false);
        standby.reverseRegistry.setActive(false);
        auto standbyChannel = grpc::CreateChannel("localhost:" + std::to_string(standbyPort),
                                                  grpc::SslCredentials(clientTls));
        auto standbySchedule = nix::remote::Scheduler::NewStub(standbyChannel);
        grpc::ClientContext passiveContext;
        passiveContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        auto passiveStream = standbySchedule->Schedule(&passiveContext);
        nix::remote::SchedMsgs passiveReply;
        assert(!passiveStream->Read(&passiveReply));
        assert(passiveStream->Finish().error_code() == grpc::StatusCode::UNAVAILABLE);
        assert(!standby.reverseRegistry.find(worker2Options.reverseWorkerId));
        standby.reverseRegistry.setActive(true);
        standby.scheduler->setActive(true);
        auto standbyWorkerOptions = worker2Options;
        standbyWorkerOptions.schedulerAddr = "localhost:" + std::to_string(standbyPort);
        standbyWorkerOptions.advertise = "standby-reverse-worker";
        nixgrpc::Coordinator standbyWorker(standbyWorkerOptions, worker2Auth, metrics, idle);
        NixRemoteService standbyWorkerService(standbyWorkerOptions.socketPath, standbyWorkerOptions.storeUri,
                                              "standby-worker", metrics, idle, nixgrpc::LogLevel::info,
                                              worker2Auth, standbyWorker);
        standbyWorker.setReverseBuild([&](auto const & call, auto const & emit, auto const & cancelled, auto & afterFinish) {
            return standbyWorkerService.reverseBuild(call, emit, cancelled, afterFinish);
        });
        standbyWorker.setReverseUpload([&](auto const & call, auto const & cancelled) {
            return standbyWorkerService.reverseUpload(call, cancelled);
        });
        grpc::ServerBuilder standbyWorkerBuilder;
        standbyWorkerBuilder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials());
        standbyWorkerBuilder.RegisterService(&standbyWorkerService);
        auto standbyWorkerServer = standbyWorkerBuilder.BuildAndStart();
        assert(standbyWorkerServer);
        standbyWorker.start(*standbyWorkerServer);
        auto const standbyReadyDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(12);
        while (!standby.reverseRegistry.find(worker2Options.reverseWorkerId)
               && std::chrono::steady_clock::now() < standbyReadyDeadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        assert(standby.reverseRegistry.find(worker2Options.reverseWorkerId));
        auto const standbyPath = source->parseStorePath(nix::chomp(nix::readFile(dir + "/standby.drv")));
        auto const standbyUri = "grpc://localhost:" + std::to_string(standbyPort)
            + "?ca-cert=" + dir + "/server.crt&client-cert=" + dir + "/client.crt&client-key=" + dir + "/client.key"
        + "&reverse-uploads=true";
        clientDrivenRecoveredReverseBuild(standbyUri, "local?root=" + dir + "/source-store",
                                          source->printStorePath(standbyPath));
        auto const standbyBasic = nixcompat::toBasicDrv(source->readDerivation(standbyPath),
            [&](const nix::StorePath &, const std::string &) -> nix::StorePath {
                throw nix::Error("unexpected standby input derivation");
            });
        assert(standbyBasic);
        auto const standbyOutput = standbyBasic->outputs.at("out").path(*source, standbyBasic->name, "out");
        assert(standbyOutput && worker2Store->isValidPath(*standbyOutput));
        assert(!workerStore->isValidPath(*standbyOutput));
        nix::StringSink standbyNar;
        nix::openStore("local?root=" + dir + "/worker2-store")->narFromPath(*standbyOutput, standbyNar);
        assert(standbyNar.s.find("standby-worker-ok") != std::string::npos);
        if (worker.cache.hasRemote()) {
            auto cache = nix::openStore(nix::readFile(dir + "/cache-url"));
            auto reader = nix::openStore("local?root=" + dir + "/standby-reader-store");
            nix::copyPaths(*cache, *reader, nix::StorePathSet{*standbyOutput});
            nix::StringSink cachedNar;
            reader->narFromPath(*standbyOutput, cachedNar);
            assert(cachedNar.s == standbyNar.s);
        }
        standbyWorkerServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        standbyWorkerServer->Wait();
        standbyServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        standbyServer->Wait();
        std::cerr << "passive scheduler rejected traffic; promoted local scheduler built via a pod-bound worker\n";
        worker2Server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        worker2Server->Wait();
    }
#else
    grpc::ClientContext buildScheduleContext;
    buildScheduleContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
    auto buildSchedule = schedule->Schedule(&buildScheduleContext);
    nix::remote::ClientMsgs buildWant;
    buildWant.add_msgs()->mutable_want()->set_drv_path(std::string(buildDrv.to_string()));
    buildWant.mutable_msgs(0)->mutable_want()->set_system(nix::settings.thisSystem.get());
    assert(buildSchedule->Write(buildWant));
    uint64_t buildAssignment = 0;
    while (!buildAssignment && buildSchedule->Read(&scheduled))
        for (auto const & msg : scheduled.msgs())
            if (msg.has_assigned()) {
                assert(msg.assigned().reverse() && msg.assigned().drv_path() == buildDrv.to_string());
                buildAssignment = msg.assigned().assign_id();
            }
    assert(buildAssignment && scheduler.dispatcher->assignmentPod(buildDrv.to_string(), buildAssignment)
                                  == workerOptions.reverseWorkerId);
    nix::remote::BuildDerivationRequest buildRequest;
    buildRequest.set_drv_path(std::string(buildDrv.to_string()));
    buildRequest.set_build_mode(static_cast<uint32_t>(nix::bmNormal));
    buildRequest.set_protocol(nixcompat::kBuildProtocolWire);
    buildRequest.set_assign_id(buildAssignment);
    nix::StringSink drvWire;
    nixcompat::writeDrv(drvWire, *source, *basicDrv);
    *buildRequest.mutable_drv() = std::move(drvWire.s);
    grpc::ClientContext probeContext;
    probeContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
    auto reader = client->BuildDerivation(&probeContext, buildRequest);
    nix::remote::BuildDerivationChunk chunk;
    while (reader->Read(&chunk)) assert(!chunk.has_done());
    auto const probe = reader->Finish();
    assert(probe.error_code() == grpc::StatusCode::NOT_FOUND);
    assert(!workerStore->isValidPath(buildDrv));
    auto const drvInfo = source->queryPathInfo(buildDrv);
    nix::StringSink drvNar;
    source->narFromPath(buildDrv, drvNar);
    nix::StringSink serialized;
    serialized << static_cast<uint64_t>(1);
    nix::WorkerProto::Serialise<nix::ValidPathInfo>::write(
        *source, nix::WorkerProto::WriteConn{.to = serialized, .version = nixcompat::infoProtocolVersion()}, *drvInfo);
    serialized.s += drvNar.s;
    std::string packed(ZSTD_compressBound(serialized.s.size()), '\0');
    auto const packedSize = ZSTD_compress(packed.data(), packed.size(), serialized.s.data(), serialized.s.size(), 1);
    assert(!ZSTD_isError(packedSize));
    packed.resize(packedSize);
    {
        grpc::ClientContext uploadContext;
        uploadContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        nix::remote::AddMultipleReply reply;
        auto writer = client->UploadBuildInputs(&uploadContext, &reply);
        nix::remote::UploadBuildInputsChunk header;
        header.set_drv_path(std::string(buildDrv.to_string()));
        header.set_assign_id(buildAssignment);
        header.mutable_chunk()->set_check_sigs(true);
        assert(writer->Write(header));
        nix::remote::UploadBuildInputsChunk data;
        data.mutable_chunk()->set_data(std::move(packed));
        assert(writer->Write(data));
        assert(writer->WritesDone());
        auto const uploaded = writer->Finish();
        if (!uploaded.ok()) std::cerr << "derivation upload: " << uploaded.error_code() << " " << uploaded.error_message() << "\n";
        assert(uploaded.ok() && workerStore->isValidPath(buildDrv));
    }
#ifdef __linux__
    {
        grpc::ClientContext retryContext;
        retryContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        auto retry = client->BuildDerivation(&retryContext, buildRequest);
        bool gotResult = false;
        while (retry->Read(&chunk)) {
            if (chunk.has_log_line()) std::cerr << "build log: " << chunk.log_line() << "\n";
            if (chunk.has_done()) {
                gotResult = true;
                std::cerr << "build result outputs=" << chunk.done().outputs_size() << "\n";
            }
        }
        auto const built = retry->Finish();
        if (!built.ok()) std::cerr << "pinned build retry: " << built.error_code() << " " << built.error_message() << "\n";
        assert(built.ok() && gotResult);
        auto const output = basicDrv->outputs.at("out").path(*source, basicDrv->name, "out");
        assert(output && workerStore->isValidPath(*output));
        nix::StringSink outputNar;
        rootedStore->narFromPath(*output, outputNar);
        assert(outputNar.s.find("reverse-built") != std::string::npos);
    }
#endif
    buildSchedule->WritesDone();
    buildSchedule->Finish();
#endif

    scheduler.restarting();
    farm->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    workerServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    workerServer->Wait();
    farm->Wait();
}
