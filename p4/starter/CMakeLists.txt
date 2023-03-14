cmake_minimum_required(VERSION 3.16)
project(starter)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(starter
        Bucket.cpp
        Bucket.hpp
        constants.hpp
        Disk.cpp
        Disk.hpp
        Join.cpp
        Join.hpp
        main.cpp
        Mem.cpp
        Mem.hpp
        Page.cpp
        Page.hpp
        Record.cpp
        Record.hpp)
