#include "logger.h"
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <iostream>
using namespace std;

AsyncLogger::LOGLEVEL AsyncLogger::m_min_level = AsyncLogger::INFO;

static string getTimestamp() {
    auto now = chrono::system_clock::now();
    auto ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
    time_t t = chrono::system_clock::to_time_t(now);
    string buf;
    buf.resize(32);
    size_t len = strftime(buf.data(), buf.capacity(), "%Y-%m-%d %H:%M:%S", localtime(&t));
    buf.resize(len);
    buf += ".";
    buf += to_string(ms.count());
    return buf;
}

AsyncLogger *g_instance = nullptr;

AsyncLogger &AsyncLogger::getInstance() {
    static once_flag flag;
    call_once(flag, [] {
        g_instance = new AsyncLogger(5);
        pthread_atfork(nullptr, nullptr, AsyncLogger::forkChildReset);
        atexit([] {
            delete g_instance;
            g_instance = nullptr;
        });
    });
    return *g_instance;
}

void AsyncLogger::forkChildReset() {
    g_instance = new AsyncLogger(5);
}

AsyncLogger::AsyncLogger(size_t flush_threshold) : m_flush_threshold(flush_threshold) {
    m_file.open("logs/server.log", ios::app);
    if (!m_file.is_open()) {
        std::cerr << "log file open failed!" << std::endl;
    }

    m_backend = thread(&AsyncLogger::backend, this);
}

AsyncLogger::~AsyncLogger() {
    {
        lock_guard<mutex> lock(m_mutex);
        m_stop = true;
    }

    m_cond.notify_one();
    if (m_backend.joinable()) {
        m_backend.join();
    }
}

constexpr size_t STACK_BUF_SIZE = 1024;

void AsyncLogger::logf(LOGLEVEL level, const char *file, int line, const char *func, const char *fmt, ...) {
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

void AsyncLogger::log(LOGLEVEL level, string_view message) {
    if (level < m_min_level) return;

    string entry;
    entry.reserve(128);
    entry += getTimestamp();
    entry += " ";
    entry += levelToString(level);
    entry += " ";
    entry += message;

    {
        lock_guard<mutex> lock(m_mutex);
        m_front_buffer.emplace(std::move(entry));
        if (m_front_buffer.size() > m_flush_threshold) {
            m_cond.notify_one();
        }
    }
}

void AsyncLogger::setFilePath(const string file_path) {
    m_file.close();
    m_file.open(file_path, ios::app);
}

void AsyncLogger::setLevel(string_view min_level) {
    if (min_level == "DEBUG") m_min_level = DEBUG;
    else if (min_level == "INFO") m_min_level = INFO;
    else if (min_level == "WARN") m_min_level = WARN;
    else if (min_level == "ERROR") m_min_level = ERROR;
    else if (min_level == "FATAL") m_min_level = FATAL;
    else {
        cerr << "no level" << endl;
    }
}

string_view AsyncLogger::levelToString(AsyncLogger::LOGLEVEL level) const {
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

void AsyncLogger::backend() {
    while (1) {
        {
            unique_lock<mutex> lock(m_mutex);
            m_cond.wait_for(lock, std::chrono::seconds(3), [this] { return m_stop || m_front_buffer.size() >= m_flush_threshold; });

            m_back_buffer.swap(m_front_buffer);
        }

        while (!m_back_buffer.empty()) {
            m_file << m_back_buffer.front() << '\n';
            m_back_buffer.pop();
        }
        m_file.flush();

        if (m_stop) {
            return;
        }
    }
}