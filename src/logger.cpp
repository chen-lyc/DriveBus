#include "../include/logger.h"
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
using namespace std;

static string getTimestamp() {
    auto now = chrono::system_clock::now();
    auto ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
    time_t t = chrono::system_clock::to_time_t(now);
    tm local_tm{};
    if (localtime_r(&t, &local_tm) == nullptr) {
        return {};
    }
    string buf;
    buf.resize(32);
    size_t len = strftime(buf.data(), buf.size(), "%Y-%m-%d %H:%M:%S", &local_tm);
    buf.resize(len);
    buf += ".";
    buf += to_string(ms.count());
    return buf;
}

Logger::Logger(const std::filesystem::path &log_file_path, size_t flush_threshold) : flush_threshold_(flush_threshold) {
    const auto parent_path = log_file_path.parent_path();
    if (!parent_path.empty()) {
        std::filesystem::create_directories(parent_path);
    }

    file_.open(log_file_path, ios::app);
    if (!file_.is_open()) {
        std::cerr << "log file open failed!" << std::endl;
    }

    backend_ = thread(&Logger::backend, this);
}

Logger::~Logger() {
    {
        lock_guard<mutex> lock(mutex_);
        stop_ = true;
    }

    cond_.notify_all();
    if (backend_.joinable()) {
        backend_.join();
    }
}

constexpr size_t STACK_BUF_SIZE = 1024;

void Logger::logf(LOGLEVEL level, const char *file, int line, const char *func, const char *fmt, ...) {
    if (level < getMinLevel()) return;

    char stack_buf[STACK_BUF_SIZE];
    int prefix_len = snprintf(stack_buf, STACK_BUF_SIZE, "%s:%d %s ", file, line, func);
    if (prefix_len < 0) {
        log(ERROR, "snprintf prefix failed");
        return;
    }

    va_list args, args_copy;
    va_start(args, fmt);
    va_copy(args_copy, args);
    if (prefix_len < STACK_BUF_SIZE) {
        int body_len = vsnprintf(stack_buf + prefix_len, STACK_BUF_SIZE - prefix_len, fmt, args);
        if (body_len < 0) {
            va_end(args);
            va_end(args_copy);
            log(ERROR, "vsnprintf body failed");
            return;
        }
        int total_len = prefix_len + body_len;
        if (total_len < STACK_BUF_SIZE) {
            va_end(args);
            va_end(args_copy);
            log(level, string_view(stack_buf, total_len));
            return;
        }

        unique_ptr<char[]> heap_buf = make_unique<char[]>(total_len + 1);
        memcpy(heap_buf.get(), stack_buf, prefix_len);
        vsnprintf(heap_buf.get() + prefix_len, body_len + 1, fmt, args_copy);

        va_end(args);
        va_end(args_copy);
        log(level, string_view(heap_buf.get(), total_len));
        return;
    }

    int body_len = vsnprintf(nullptr, 0, fmt, args);
    if (body_len < 0) {
        va_end(args);
        va_end(args_copy);
        log(ERROR, "vsnprintf body failed");
        return;
    }
    int total_len = prefix_len + body_len;
    unique_ptr<char[]> heap_buf = make_unique<char[]>(total_len + 1);
    snprintf(heap_buf.get(), prefix_len + 1, "%s:%d %s ", file, line, func);
    vsnprintf(heap_buf.get() + prefix_len, body_len + 1, fmt, args_copy);

    va_end(args);
    va_end(args_copy);
    log(level, string_view(heap_buf.get(), total_len));
}

void Logger::log(LOGLEVEL level, string_view message) {
    if (level < min_level_) return;

    string entry;
    entry.reserve(128);

    const string timestamp = getTimestamp();
    entry += timestamp.empty() ? "<timestamp-unavailable>" : timestamp;
    entry += " ";
    entry += levelToString(level);
    entry += " ";
    entry += message;

    {
        lock_guard<mutex> lock(mutex_);
        front_buffer_.emplace(std::move(entry));
        if (front_buffer_.size() >= flush_threshold_) {
            cond_.notify_one();
        }
    }
}

void Logger::setLogFilePath(std::filesystem::path &log_file_path) {
    const auto parent_path = log_file_path.parent_path();
    if (!parent_path.empty()) {
        std::filesystem::create_directories(parent_path);
    }

    {
        lock_guard<mutex> lock(mutex_);
        file_.close();
        file_.open(log_file_path, ios::app);

        if (!file_.is_open()) {
            std::cerr << "log file open failed!" << std::endl;
        }
    }
}

void Logger::setLevel(string_view min_level) {
    if (min_level == "DEBUG") min_level_ = DEBUG;
    else if (min_level == "INFO") min_level_ = INFO;
    else if (min_level == "WARN") min_level_ = WARN;
    else if (min_level == "ERROR") min_level_ = ERROR;
    else if (min_level == "FATAL") min_level_ = FATAL;
    else {
        cerr << "no level" << endl;
    }
}

string_view Logger::levelToString(Logger::LOGLEVEL level) const {
    switch (level) {
        case DEBUG:
            return "DEBUG";
        case INFO:
            return "INFO";
        case WARN:
            return "WARN";
        case ERROR:
            return "ERROR";
        case FATAL:
            return "FATAL";
        default:
            return "UNKNOWN";
    }
}

void Logger::backend() {
    while (1) {
        unique_lock<mutex> lock(mutex_);
        cond_.wait_for(lock, std::chrono::seconds(3), [this] { return stop_ || front_buffer_.size() >= flush_threshold_; });

        back_buffer_.swap(front_buffer_);

        while (!back_buffer_.empty()) {
            file_ << back_buffer_.front() << '\n';
            back_buffer_.pop();
        }
        file_.flush();

        if (stop_) {
            return;
        }
    }
}
