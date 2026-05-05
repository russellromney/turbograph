#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>

namespace httplib {
class ClientImpl;
}

namespace lbug {
namespace tiered {

// Thread-safe pool of persistent httplib::ClientImpl instances.
// Clients are checked out via acquire(), used, and returned via release().
class ConnectionPool {
public:
    // Creates poolSize persistent keep-alive connections to endpoint.
    ConnectionPool(const std::string& endpoint, uint32_t poolSize);
    ~ConnectionPool();

    // Check out a client. Blocks if all clients are in use.
    std::unique_ptr<httplib::ClientImpl> acquire();

    // Return a client to the pool.
    void release(std::unique_ptr<httplib::ClientImpl> client);

    uint32_t poolSize() const { return poolSize_; }
    uint32_t available() const;

private:
    std::string endpoint_;
    uint32_t poolSize_;
    std::queue<std::unique_ptr<httplib::ClientImpl>> clients_;
    mutable std::mutex mu_;
    std::condition_variable cv_;

    std::unique_ptr<httplib::ClientImpl> createClient() const;
};

// RAII wrapper: acquires a client on construction, releases on destruction.
class PooledClient {
public:
    explicit PooledClient(ConnectionPool& pool);
    ~PooledClient();

    // Non-copyable.
    PooledClient(const PooledClient&) = delete;
    PooledClient& operator=(const PooledClient&) = delete;

    // Movable.
    PooledClient(PooledClient&& other) noexcept;
    PooledClient& operator=(PooledClient&& other) noexcept;

    httplib::ClientImpl* operator->() { return client_.get(); }
    httplib::ClientImpl& operator*() { return *client_; }

private:
    ConnectionPool* pool_;
    std::unique_ptr<httplib::ClientImpl> client_;
};

} // namespace tiered
} // namespace lbug
