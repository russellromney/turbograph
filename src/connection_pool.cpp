#include "connection_pool.h"

#include "httplib.h"

#include <cstdint>
#include <stdexcept>
#include <string>

namespace lbug {
namespace tiered {

// --- ConnectionPool ---

namespace {

struct ParsedEndpoint {
    bool https = true;
    std::string host;
    int port = 443;
};

ParsedEndpoint parseEndpoint(const std::string& endpoint) {
    ParsedEndpoint parsed;
    std::string hostPort = endpoint;
    if (hostPort.rfind("https://", 0) == 0) {
        parsed.https = true;
        parsed.port = 443;
        hostPort = hostPort.substr(8);
    } else if (hostPort.rfind("http://", 0) == 0) {
        parsed.https = false;
        parsed.port = 80;
        hostPort = hostPort.substr(7);
    }

    auto slash = hostPort.find('/');
    if (slash != std::string::npos) {
        hostPort = hostPort.substr(0, slash);
    }

    auto colon = hostPort.rfind(':');
    if (colon != std::string::npos) {
        parsed.host = hostPort.substr(0, colon);
        parsed.port = std::stoi(hostPort.substr(colon + 1));
    } else {
        parsed.host = hostPort;
    }

    if (parsed.host.empty()) {
        throw std::invalid_argument("ConnectionPool endpoint is missing a host");
    }

    return parsed;
}

} // namespace

std::unique_ptr<httplib::ClientImpl> ConnectionPool::createClient() const {
    auto endpoint = parseEndpoint(endpoint_);
    std::unique_ptr<httplib::ClientImpl> client;
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    if (endpoint.https) {
        client = std::make_unique<httplib::SSLClient>(endpoint.host, endpoint.port);
    } else {
        client = std::make_unique<httplib::ClientImpl>(endpoint.host, endpoint.port);
    }
#else
    if (endpoint.https) {
        throw std::runtime_error(
            "HTTPS endpoint requires cpp-httplib OpenSSL support");
    }
    client = std::make_unique<httplib::ClientImpl>(endpoint.host, endpoint.port);
#endif
    client->set_connection_timeout(10);
    client->set_read_timeout(30);
    client->set_write_timeout(30);
    client->set_keep_alive(true);
    return client;
}

ConnectionPool::ConnectionPool(const std::string& endpoint, uint32_t poolSize)
    : endpoint_(endpoint), poolSize_(poolSize) {
    for (uint32_t i = 0; i < poolSize; i++) {
        clients_.push(createClient());
    }
}

ConnectionPool::~ConnectionPool() = default;

std::unique_ptr<httplib::ClientImpl> ConnectionPool::acquire() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] { return !clients_.empty(); });
    auto client = std::move(clients_.front());
    clients_.pop();
    return client;
}

void ConnectionPool::release(std::unique_ptr<httplib::ClientImpl> client) {
    std::lock_guard lock(mu_);
    clients_.push(std::move(client));
    cv_.notify_one();
}

uint32_t ConnectionPool::available() const {
    std::lock_guard lock(mu_);
    return static_cast<uint32_t>(clients_.size());
}

// --- PooledClient ---

PooledClient::PooledClient(ConnectionPool& pool) : pool_(&pool), client_(pool.acquire()) {}

PooledClient::~PooledClient() {
    if (client_) {
        pool_->release(std::move(client_));
    }
}

PooledClient::PooledClient(PooledClient&& other) noexcept
    : pool_(other.pool_), client_(std::move(other.client_)) {
    other.pool_ = nullptr;
}

PooledClient& PooledClient::operator=(PooledClient&& other) noexcept {
    if (this != &other) {
        if (client_) {
            pool_->release(std::move(client_));
        }
        pool_ = other.pool_;
        client_ = std::move(other.client_);
        other.pool_ = nullptr;
    }
    return *this;
}

} // namespace tiered
} // namespace lbug
