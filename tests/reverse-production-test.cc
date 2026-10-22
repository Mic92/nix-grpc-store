// Run the production scheduler, reverse worker and RPC dispatchers over mTLS.
// Do not replace WorkerSession or Register with an in-memory test transport.
#include "../src/daemon/main.cc"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <openssl/rand.h>
#include <nix/store/worker-protocol.hh>
#include <nix/util/archive.hh>
#include <nix/util/serialise.hh>
#include <nix/util/signature/local-keys.hh>
#include <nix/util/signature/signer.hh>
#include <zstd.h>

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
    schedulerOptions.acl.requireCertificate();
    schedulerOptions.reverseOidcConfig = dir + "/oidc.json";
    schedulerOptions.reverseProvider = "worker-tunnel";
    schedulerOptions.reverseNamespace = "staging";
    schedulerOptions.reverseServiceAccount = "worker";
    schedulerOptions.reverseAudience = "grpc://worker-tunnel";
    nixgrpc::Metrics metrics("");
    nixgrpc::IdleTracker idle;
    std::optional<nixgrpc::oidc::Verifier> noClientOidc;
    nixgrpc::Auth schedulerAuth{.acl = schedulerOptions.acl, .oidc = &noClientOidc};
    nixgrpc::Coordinator scheduler(schedulerOptions, schedulerAuth, metrics);
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
    workerOptions.maxJobs = 2;
    workerOptions.storeUri = storeUri;
    workerOptions.socketPath = socket;
    workerOptions.tlsCert = schedulerOptions.tlsCert;
    workerOptions.tlsKey = schedulerOptions.tlsKey;
    workerOptions.clientCA = schedulerOptions.clientCA;
    workerOptions.schedulerAddr = "localhost:" + std::to_string(farmPort);
    workerOptions.reverseTokenFile = dir + "/token";
    workerOptions.reverseWorkerId = "pod:staging:12345678-1234-1234-1234-123456789abc";
    workerOptions.advertise = "reverse-worker";
    nixgrpc::Auth workerAuth;
    nixgrpc::Coordinator worker(workerOptions, workerAuth, metrics);
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

    scheduler.restarting();
    farm->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    workerServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    workerServer->Wait();
    farm->Wait();
}
