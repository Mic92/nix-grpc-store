// Disk-space health of a farm worker.

#include "farm.hh"

#include <cstdint>

#include <sys/statvfs.h>

#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/server.h>

#include "logfmt.hh"

namespace nixgrpc {

void Farm::updateHealth(grpc::Server & server)
{
    struct statvfs vfs{};
    if (minFree == 0 || statvfs(storeDir.c_str(), &vfs) != 0) {
        return;
    }
    bool const now = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize >= minFree;
    if (healthy.exchange(now) == now) {
        return;
    }
    server.GetHealthCheckService()->SetServingStatus(now);
    logLine(LogLevel::info, {{"event", now ? "healthy" : "unhealthy"}, {"reason", "min_free"}});
}

} // namespace nixgrpc
