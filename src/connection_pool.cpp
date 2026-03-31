#include "connection_pool.h"

#include "httplib.h"

namespace lbug {
namespace tiered {

// --- ConnectionPool ---

std::unique_ptr<httplib::Client> ConnectionPool::createClient() const {
    auto client = std::make_unique<httplib::Client>(endpoint_);
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

std::unique_ptr<httplib::Client> ConnectionPool::acquire() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] { return !clients_.empty(); });
    auto client = std::move(clients_.front());
    clients_.pop();
    return client;
}

void ConnectionPool::release(std::unique_ptr<httplib::Client> client) {
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
