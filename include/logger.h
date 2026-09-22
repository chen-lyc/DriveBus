#pragma once

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <queue>
#include <string_view>
#include <thread>

#define IF_DEBUG if (Logger::getInstance().getMinLevel() == Logger::DEBUG)

#define LOG_DEBUG(fmt, ...) \
    Logger::getInstance().logf(Logger::DEBUG, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_INFO(fmt, ...) \
    Logger::getInstance().logf(Logger::INFO, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_WARN(fmt, ...) \
    Logger::getInstance().logf(Logger::WARN, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_ERROR(fmt, ...) \
    Logger::getInstance().logf(Logger::ERROR, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

#define LOG_FATAL(fmt, ...) \
    Logger::getInstance().logf(Logger::FATAL, __FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)

class Logger {
  public:
    enum LOGLEVEL {
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL
    };

    static void init(const std::filesystem::path &log_file_path) {
        instance(log_file_path);
    }

    static Logger &getInstance() {
        return instance("logs/data.log");
    }

    ~Logger();

    void logf(LOGLEVEL level, const char *file, int line, const char *func, const char *fmt, ...);

    void setLogFilePath(std::filesystem::path &log_file_path);
    void setLevel(std::string_view min_level);
    void setLevel(LOGLEVEL min_level) {
        min_level_.store(min_level, std::memory_order_relaxed);
    }
    LOGLEVEL getMinLevel() {
        return min_level_.load(std::memory_order_relaxed);
    }

  private:
    static Logger &instance(const std::filesystem::path &log_file_path) {
        static Logger logger(log_file_path, 5);
        return logger;
    }

    Logger(const std::filesystem::path &log_file_path, size_t flush_threshold);
    void log(LOGLEVEL level, std::string_view message);
    std::string_view levelToString(LOGLEVEL level) const;
    void backend();

  private:
    std::queue<std::string> front_buffer_;
    std::queue<std::string> back_buffer_;

    std::mutex mutex_;
    std::condition_variable cond_;

    std::thread backend_;
    size_t flush_threshold_;
    std::ofstream file_;
    std::atomic<LOGLEVEL> min_level_{INFO};
    bool stop_ = false;
};