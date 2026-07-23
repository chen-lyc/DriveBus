CXX := g++
CPPFLAGS := -Iinclude
CXXFLAGS := -std=c++20 -Wall -Wextra -Wpedantic -pthread

DEBUG_CHECKS ?= 0
DEBUG_SYMBOLS ?= 0

ifeq ($(DEBUG_SYMBOLS),1)
CXXFLAGS += -g
endif

ifeq ($(DEBUG_CHECKS),1)
CPPFLAGS += -DENABLE_DEBUG_CHECKS
endif

HEADERS := $(wildcard include/*.h include/*.hpp)
TARGETS := a.out b.out broker.out

.PHONY: all rebuild

# 在项目根目录执行 make，编译全部程序。
all: $(TARGETS)

# 无论目标是否已存在，都强制重新编译。
rebuild:
	$(MAKE) -B all

a.out: a.cpp $(HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) a.cpp -o $@

b.out: b.cpp src/fd_helpers.cpp $(HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) b.cpp src/fd_helpers.cpp -o $@

broker.out: broker.cpp src/fd_helpers.cpp $(HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) broker.cpp src/fd_helpers.cpp -o $@
