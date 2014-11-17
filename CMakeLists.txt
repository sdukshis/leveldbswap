cmake_minimum_required(VERSION 3.0)

project(leveldbswap 
        VERSION 0.0.1
        LANGUAGES CXX)

add_definitions(-Wall)
add_definitions(-std=c++11)

add_executable(${PROJECT_NAME} src/main.cpp
                               src/meminfo.cpp)

add_executable(memtest src/memtest.cpp
                       src/meminfo.cpp)

target_link_libraries(${PROJECT_NAME} leveldb)

