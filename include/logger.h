#pragma once

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <queue>
#include <string_view>
#include <thread>

#define IF_DEBUG if (AsyncLogger::getMinLevel() == AsyncLogger::DEBUG)

#define LOG_DEBUG(fmt, ...) \
    AsyncLogger::getInstance().logf(AsyncLogger::DEBUG, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_INFO(fmt, ...) \
    AsyncLogger::getInstance().logf(AsyncLogger::INFO, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_WARN(fmt, ...) \
    AsyncLogger::getInstance().logf(AsyncLogger::WARN, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_ERROR(fmt, ...) \
    AsyncLogger::getInstance().logf(AsyncLogger::ERROR, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_FATAL(fmt, ...) \
    AsyncLogger::getInstance().logf(AsyncLogger::FATAL, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

class AsyncLogger {
  public:
    enum LOGLEVEL {
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL
    };

    static AsyncLogger &getInstance();
    ~AsyncLogger();
    void logf(LOGLEVEL level, const char *file, int line, const char *func, const char *fmt, ...);
    void setFilePath(const std::string file_path);
    void setLevel(std::string_view min_level);
    static void setLevel(LOGLEVEL min_level) {
        m_min_level = min_level;
    }
    static LOGLEVEL getMinLevel() {
        return m_min_level;
    }

  private:
    AsyncLogger(size_t flush_threshold);
    void log(LOGLEVEL level, std::string_view message);
    static void forkChildReset();
    std::string_view levelToString(LOGLEVEL level) const;
    void backend();

  private:
    std::queue<std::string> m_front_buffer;
    std::queue<std::string> m_back_buffer;
    std::mutex m_mutex;
    std::condition_variable m_cond;
    std::thread m_backend;
    size_t m_flush_threshold;
    std::ofstream m_file;
    static LOGLEVEL m_min_level;
    bool m_stop = false;
};