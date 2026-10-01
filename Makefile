CXX := g++
CPPFLAGS := -Iinclude
CXXFLAGS := -std=c++20 -Wall -Wextra -Wpedantic -pthread -DENABLE_DEBUG_CHECKS

DEBUG_CHECKS ?= 0
DEBUG_SYMBOLS ?= 0

ifeq ($(DEBUG_SYMBOLS),1)
CXXFLAGS += -g
endif

ifeq ($(DEBUG_CHECKS),1)
CPPFLAGS += -DENABLE_DEBUG_CHECKS
endif

HEADERS := $(wildcard include/*.h include/*.hpp)
TARGETS := broker.out broker-status pub_node sub_node

.PHONY: all rebuild

# 在项目根目录执行 make，编译全部程序。
all: $(TARGETS)

# 无论目标是否已存在，都强制重新编译。
rebuild:
	$(MAKE) -B all

broker.out: broker.cpp src/fd_helpers.cpp src/logger.cpp $(HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) broker.cpp src/fd_helpers.cpp src/logger.cpp -o $@

broker-status: broker_status_cli.cpp $(HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) broker_status_cli.cpp -o $@

pub_node: pub_node.cpp src/publisher.cpp src/fd_helpers.cpp src/logger.cpp $(HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) pub_node.cpp src/publisher.cpp src/fd_helpers.cpp src/logger.cpp -o $@

sub_node: sub_node.cpp src/subscriber.cpp src/fd_helpers.cpp src/logger.cpp $(HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) sub_node.cpp src/subscriber.cpp src/fd_helpers.cpp src/logger.cpp -o $@
