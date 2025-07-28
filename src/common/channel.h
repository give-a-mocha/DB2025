#pragma once

#include <condition_variable> 
#include <mutex> 
#include <queue>
#include <utility>

template <class T>
class Channel {
public:
    Channel() = default;
    ~Channel() = default;

    void Put(T element) {
        std::unique_lock<std::mutex> lk(m_);
        q_.push(std::move(element));
        lk.unlock();
        cv_.notify_all();
    }

    auto Get() -> T {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&]() { return !q_.empty(); });
        T element = std::move(q_.front());
        q_.pop();
        return element;
    }

private:
	std::mutex m_;
	std::condition_variable cv_;
	std::queue<T> q_;
};
