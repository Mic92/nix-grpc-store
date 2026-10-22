#include <cassert>
#include <memory>

#include <nix/util/file-system.hh>

#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <openssl/crypto.h>

#include "nix_remote.grpc.pb.h"
#include "reverse-gateway.hh"
#include "reverse-upload-relay.hh"

class ClientService final : public nix::remote::NixRemote::Service
{
  public:
    nixgrpc::ReverseGateway &gateway;
    nixgrpc::ReverseRegistry &registry;
    std::string worker;
    grpc::Status last;
    std::optional<nixgrpc::ReverseGateway::Prepared> prepared;
    std::string method = "StoreInfo";
    std::string drvPath;
    uint64_t assignId = 0;
    uint32_t buildMode = 0;
    bool schedulerRoute = false;
    bool uploadRoute = false;
    bool uploadRelayEnabled = false;
    bool * assignmentLive = nullptr;
    size_t resolverCalls = 0;

    ClientService(nixgrpc::ReverseGateway &gateway, nixgrpc::ReverseRegistry &registry, std::string worker)
        : gateway(gateway), registry(registry), worker(std::move(worker)) {}

    auto UploadBuildInputs(grpc::ServerContext *ctx,
                           grpc::ServerReader<nix::remote::UploadBuildInputsChunk> * reader,
                           nix::remote::AddMultipleReply *) -> grpc::Status override
    {
        if (!uploadRelayEnabled) return {grpc::StatusCode::UNIMPLEMENTED, "test upload disabled"};
        nix::remote::UploadBuildInputsChunk first;
        if (!reader->Read(&first)) return {grpc::StatusCode::INVALID_ARGUMENT, "empty upload"};
        auto const drv = first.drv_path();
        auto const assignId = first.assign_id();
        nixgrpc::ReverseUploadRelay relay(gateway, registry);
        return relay.call(*ctx, [&, drv, assignId](uint64_t id) {
            return gateway.prepareBuildInputs(*ctx, [&](std::string_view drv, uint64_t assign)
                -> std::optional<std::string> {
                if (*assignmentLive && drv == "assigned.drv" && assign == 42) return worker;
                return std::nullopt;
            }, id, drv, assignId);
        }, [&] { return *assignmentLive; },
        [&](nix::remote::UploadBuildInputsChunk & chunk) { return reader->Read(&chunk); }, std::move(first));
    }

    auto StoreInfo(grpc::ServerContext *ctx, const nix::remote::StoreInfoRequest *, nix::remote::StoreInfoReply *reply)
        -> grpc::Status override
    {
        auto result = uploadRoute
                          ? gateway.prepareBuildInputs(*ctx, [&](std::string_view drv, uint64_t id) -> std::optional<std::string> {
                                ++resolverCalls;
                                if (drv == "assigned.drv" && id == 42) return worker;
                                return std::nullopt;
                            }, 1, drvPath, assignId)
                          : schedulerRoute
                          ? gateway.prepareBuild(*ctx, [&](std::string_view drv, uint64_t id) -> std::optional<std::string> {
                                ++resolverCalls;
                                if (drv == "assigned.drv" && id == 42) return worker;
                                return std::nullopt;
                            }, 1, drvPath, assignId, buildMode)
                          : gateway.prepare(*ctx, worker, 1, method, drvPath, assignId, buildMode);
        last = result.status;
        prepared = std::move(result.prepared);
        if (prepared)
        {
            auto decoded = nixgrpc::ReverseCapability::authorizeOpen(prepared->session->sessionKey, prepared->open,
                                                                     worker, prepared->session->generation, 1);
            assert(decoded && decoded->caller.role && decoded->caller.name == "localhost");
            reply->set_trusted(decoded->caller.role == nixgrpc::Role::trusted);
        }
        return last;
    }
};

int main(int argc, char **argv)
{
    assert(argc == 2);
    // gRPC event-engine threads may outlive process-level OpenSSL cleanup on
    // Darwin; keep crypto providers alive until the OS reclaims the process.
    assert(OPENSSL_init_crypto(OPENSSL_INIT_NO_ATEXIT, nullptr) == 1);
    grpc_init();
    {
        nixgrpc::ReverseRegistry registry;
        registry.setActive(true);
        auto key = nixgrpc::ReverseCapability::newKey();
        assert(key);
        auto const worker = std::string("pod:staging:01234567-89ab-cdef-0123-456789abcdef");
        auto session = *registry.registerWorker(worker, worker, [] {}, *key,
                                                std::make_shared<nixgrpc::ReverseTransport>());
        nixgrpc::Auth auth;
        std::optional<nixgrpc::oidc::Verifier> oidc;
        auth.oidc = &oidc;
        auth.acl.addRule("localhost=read-only");
        bool assigned = true;
        nixgrpc::ReverseGateway gateway(auth, registry,
                                         [&](std::string_view selected, std::string_view drv, uint64_t id)
                                         { return assigned && selected == worker && drv == "assigned.drv" && id == 42; });
        ClientService service(gateway, registry, worker);
        service.assignmentLive = &assigned;
        grpc::ServerBuilder builder;
        int port = 0;
        int tlsPort = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        grpc::SslServerCredentialsOptions tls;
        tls.pem_root_certs = nix::readFile(std::string(argv[1]) + "/server.crt");
        tls.pem_key_cert_pairs.push_back({nix::readFile(std::string(argv[1]) + "/server.key"), tls.pem_root_certs});
        tls.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
        builder.AddListeningPort("127.0.0.1:0", grpc::SslServerCredentials(tls), &tlsPort);
        builder.RegisterService(&service);
        auto server = builder.BuildAndStart();
        assert(server && port > 0 && tlsPort > 0);
        auto stub = nix::remote::NixRemote::NewStub(
            grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
        {
            grpc::ClientContext context;
            nix::remote::StoreInfoRequest req;
            nix::remote::StoreInfoReply reply;
            // Metadata pretending to be a TLS peer or worker ID cannot create a
            // client identity. The gateway uses Auth::identify on the actual context.
            context.AddMetadata("x-nix-worker", worker);
            context.AddMetadata("x-forwarded-client-cert", "Subject=CN=client");
            auto const status = stub->StoreInfo(&context, req, &reply);
            assert(!status.ok() && status.error_code() == grpc::StatusCode::UNAUTHENTICATED);
            assert(!service.prepared);
            // Even explicitly permitted anonymous direct clients cannot obtain a
            // delegated capability for a remote worker.
            auth.acl.allowAnonymous(nixgrpc::Role::trusted);
            grpc::ClientContext anonymous;
            assert(stub->StoreInfo(&anonymous, req, &reply).error_code() == grpc::StatusCode::UNAUTHENTICATED);
            assert(!service.prepared);
            grpc::SslCredentialsOptions clientTls;
            clientTls.pem_root_certs = tls.pem_root_certs;
            clientTls.pem_private_key = tls.pem_key_cert_pairs.front().private_key;
            clientTls.pem_cert_chain = tls.pem_key_cert_pairs.front().cert_chain;
            auto tlsStub = nix::remote::NixRemote::NewStub(
                grpc::CreateChannel("localhost:" + std::to_string(tlsPort), grpc::SslCredentials(clientTls)));
            grpc::ClientContext legitimate;
            assert(tlsStub->StoreInfo(&legitimate, req, &reply).ok());
            assert(service.prepared && service.prepared->session == session);
            assert(gateway.enqueue(*service.prepared, 1).ok());
            nix::remote::ReverseFarmMsg queued;
            assert(session->transport->outgoing.take(queued) && queued.has_open() && queued.stream_id() == 1);
            assert(session->transport->outgoing.finished(1));
            assert(session->transport->responses.cancel(1));
            assert(gateway.enqueue(*service.prepared, 1).error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED);
            auto tampered = *service.prepared;
            tampered.drvPath = "another.drv";
            assert(gateway.enqueue(std::move(tampered), 2).error_code() == grpc::StatusCode::PERMISSION_DENIED);

            service.worker = "unknown-worker";
            grpc::ClientContext missing;
            assert(tlsStub->StoreInfo(&missing, req, &reply).error_code() == grpc::StatusCode::UNAVAILABLE);
            assert(!service.prepared); // no fallback to a different worker
            service.worker = worker;

            service.method = "BuildDerivation";
            service.drvPath = "assigned.drv";
            service.assignId = 41;
            grpc::ClientContext wrongAssignment;
            assert(tlsStub->StoreInfo(&wrongAssignment, req, &reply).error_code() ==
                   grpc::StatusCode::PERMISSION_DENIED); // read-only role checked first
            assert(!service.prepared);
            service.uploadRoute = true;
            service.drvPath = "assigned.drv";
            service.assignId = 42;
            auto const uploadsBeforeAnonymous = service.resolverCalls;
            grpc::ClientContext anonymousUpload;
            assert(stub->StoreInfo(&anonymousUpload, req, &reply).error_code() == grpc::StatusCode::UNAUTHENTICATED);
            assert(service.resolverCalls == uploadsBeforeAnonymous && !service.prepared);
            grpc::ClientContext readOnlyUpload;
            assert(tlsStub->StoreInfo(&readOnlyUpload, req, &reply).error_code() == grpc::StatusCode::PERMISSION_DENIED);
            assert(service.resolverCalls == uploadsBeforeAnonymous && !service.prepared);
            service.uploadRoute = false;
            service.drvPath = "assigned.drv";
            service.assignId = 41;
            auth.acl = nixgrpc::Acl{};
            auth.acl.addRule("localhost=write");
            grpc::ClientContext wrongAssignmentWriter;
            assert(tlsStub->StoreInfo(&wrongAssignmentWriter, req, &reply).error_code() ==
                   grpc::StatusCode::FAILED_PRECONDITION);
            service.assignId = 42;
            service.schedulerRoute = true;
            // Anonymous callers are refused before the resolver can reveal
            // whether this derivation has an active assignment.
            auto const callsBeforeAnonymous = service.resolverCalls;
            grpc::ClientContext anonymousFirst;
            assert(stub->StoreInfo(&anonymousFirst, req, &reply).error_code() == grpc::StatusCode::UNAUTHENTICATED);
            assert(service.resolverCalls == callsBeforeAnonymous);
            grpc::ClientContext validAssignment;
            assert(tlsStub->StoreInfo(&validAssignment, req, &reply).ok());
            assert(service.prepared && service.prepared->session == session);
            auto delegated = nixgrpc::ReverseCapability::authorizeOpen(session->sessionKey, service.prepared->open,
                                                                       worker, session->generation, 1);
            assert(delegated && delegated->assignId == 42 && delegated->drvPath == "assigned.drv" &&
                   delegated->caller.role == nixgrpc::Role::write);
            service.worker = "another-pod";
            grpc::ClientContext noFallback;
            assert(tlsStub->StoreInfo(&noFallback, req, &reply).error_code() == grpc::StatusCode::FAILED_PRECONDITION);
            assert(!service.prepared); // scheduler-selected pod never falls back
            service.worker = worker;
            grpc::ClientContext restored;
            assert(tlsStub->StoreInfo(&restored, req, &reply).ok());
            assert(service.prepared);
            grpc::ClientContext anonymousBuild;
            assert(stub->StoreInfo(&anonymousBuild, req, &reply).error_code() == grpc::StatusCode::UNAUTHENTICATED);
            assert(!service.prepared);
            grpc::ClientContext restoredAfterAnonymous;
            assert(tlsStub->StoreInfo(&restoredAfterAnonymous, req, &reply).ok());
            assigned = false;
            assert(gateway.enqueue(*service.prepared, 1).error_code() == grpc::StatusCode::FAILED_PRECONDITION);
            assert(!session->transport->outgoing.take(queued));
            assigned = true;
            service.buildMode = static_cast<uint32_t>(nix::bmRepair);
            grpc::ClientContext repair;
            assert(tlsStub->StoreInfo(&repair, req, &reply).error_code() == grpc::StatusCode::PERMISSION_DENIED);
            auth.acl = nixgrpc::Acl{};
            auth.acl.addRule("localhost=trusted");
            grpc::ClientContext trustedRepair;
            assert(tlsStub->StoreInfo(&trustedRepair, req, &reply).ok());
            assert(service.prepared);
            service.method = "UploadBuildInputs";
            service.schedulerRoute = false;
            service.uploadRoute = true;
            service.buildMode = 0;
            grpc::ClientContext upload;
            assert(tlsStub->StoreInfo(&upload, req, &reply).ok());
            assert(service.prepared && service.prepared->drvPath == "assigned.drv" &&
                   service.prepared->assignId == 42);
            auto pinnedUpload = *service.prepared;
            assigned = false;
            assert(gateway.enqueue(std::move(pinnedUpload), 2).error_code() == grpc::StatusCode::FAILED_PRECONDITION);
            assigned = true;
            service.assignId = 43;
            grpc::ClientContext staleUpload;
            assert(tlsStub->StoreInfo(&staleUpload, req, &reply).error_code() == grpc::StatusCode::FAILED_PRECONDITION);
            service.assignId = 42;
            grpc::ClientContext validUpload;
            assert(tlsStub->StoreInfo(&validUpload, req, &reply).ok());
            assert(service.prepared);
            auto oldSession = session;
            auto stalePrepared = *service.prepared;
            auto newKey = nixgrpc::ReverseCapability::newKey();
            assert(newKey);
            session = *registry.registerWorker(worker, worker, [] {}, *newKey,
                                                std::make_shared<nixgrpc::ReverseTransport>());
            assert(!registry.current(oldSession));
            assert(gateway.enqueue(std::move(stalePrepared), 1).error_code() == grpc::StatusCode::UNAVAILABLE);
            assert(!nixgrpc::ReverseCapability::authorizeOpen(session->sessionKey, service.prepared->open, worker,
                                                              session->generation, 1));
            service.method = "StoreInfo";
            service.schedulerRoute = false;
            service.uploadRoute = false;
            service.drvPath.clear();
            service.assignId = 0;
            service.buildMode = 0;
            grpc::ClientContext replaced;
            assert(tlsStub->StoreInfo(&replaced, req, &reply).ok());
            assert(service.prepared && service.prepared->session == session);

            service.uploadRelayEnabled = true;
            auto rejectUpload = [&](nix::remote::NixRemote::Stub & client, grpc::StatusCode expected) {
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
                nix::remote::AddMultipleReply result;
                auto stream = client.UploadBuildInputs(&context, &result);
                nix::remote::UploadBuildInputsChunk header;
                header.set_drv_path("assigned.drv");
                header.set_assign_id(42);
                header.mutable_chunk()->set_check_sigs(true);
                (void) stream->Write(header);
                stream->WritesDone();
                assert(stream->Finish().error_code() == expected);
            };
            rejectUpload(*stub, grpc::StatusCode::UNAUTHENTICATED);
            assigned = false;
            rejectUpload(*tlsStub, grpc::StatusCode::FAILED_PRECONDITION);
            assigned = true;
            assert(!session->transport->outgoing.take(queued));
            std::string const large(96 * 1024, 'x');
            constexpr size_t uploadChunks = 48; // >4 MiB through a 4 MiB bounded queue
            std::jthread uploadWorker([&] {
                nix::remote::ReverseFarmMsg frame;
                assert(session->transport->outgoing.waitTake(frame, std::stop_token{}) && frame.has_open());
                auto const streamId = frame.stream_id();
                auto delegated = nixgrpc::ReverseCapability::authorizeOpen(session->sessionKey, frame.open(),
                    worker, session->generation, streamId);
                assert(delegated && delegated->method == "UploadBuildInputs" && delegated->assignId == 42);
                nixgrpc::ReverseUpload::Decoder decoder(*delegated);
                size_t chunks = 0;
                while (session->transport->outgoing.waitTake(frame, std::stop_token{}) && frame.stream_id() == streamId) {
                    if (frame.has_data()) {
                        assert(decoder.feed(frame.data().data(), [&](nix::remote::UploadBuildInputsChunk && chunk) {
                            ++chunks;
                            return chunks == 1 ? chunk.chunk().check_sigs() && chunk.drv_path() == "assigned.drv"
                                               : chunk.chunk().data() == large;
                        }));
                    } else if (frame.half_close()) break;
                    else assert(false);
                }
                assert(chunks == uploadChunks + 1 && decoder.complete());
                nix::remote::ReverseWorkerMsg reply;
                reply.set_stream_id(streamId);
                reply.mutable_finish()->set_code(static_cast<uint32_t>(grpc::StatusCode::UNIMPLEMENTED));
                reply.mutable_finish()->set_message("worker importer not enabled");
                assert(session->transport->responses.accept(std::move(reply)));
            });
            grpc::ClientContext uploadCtx;
            uploadCtx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(20));
            uploadCtx.AddMetadata("x-nix-worker", "another-pod");
            nix::remote::AddMultipleReply uploadReply;
            auto uploader = tlsStub->UploadBuildInputs(&uploadCtx, &uploadReply);
            nix::remote::UploadBuildInputsChunk firstUpload;
            firstUpload.set_drv_path("assigned.drv");
            firstUpload.set_assign_id(42);
            firstUpload.mutable_chunk()->set_check_sigs(true);
            assert(uploader->Write(firstUpload));
            nix::remote::UploadBuildInputsChunk nextUpload;
            nextUpload.mutable_chunk()->set_data(large);
            for (size_t i = 0; i < uploadChunks; ++i) assert(uploader->Write(nextUpload));
            uploader->WritesDone();
            auto uploadStatus = uploader->Finish();
            assert(uploadStatus.error_code() == grpc::StatusCode::UNIMPLEMENTED);
            uploadWorker.join();
            service.uploadRelayEnabled = false;

            registry.setActive(false);
            grpc::ClientContext standby;
            assert(tlsStub->StoreInfo(&standby, req, &reply).error_code() == grpc::StatusCode::UNAVAILABLE);
            assert(!service.prepared);
        }
        server->Shutdown();
        server->Wait();
        // No live route remains after leadership loss.
        assert(!registry.current(session));
    }
    // Wait for channel event-engine threads before OpenSSL static teardown.
    grpc_shutdown_blocking();
}
