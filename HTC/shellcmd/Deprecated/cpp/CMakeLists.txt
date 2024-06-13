cmake_minimum_required(VERSION 3.10)

project(htc-shellcmd VERSION 0.1)

message("compiler=${CMAKE_CXX_COMPILER}")

include_directories(third-party)

get_filename_component(CXX_COMPILER_NAME ${CMAKE_CXX_COMPILER} NAME)
if (${CXX_COMPILER_NAME} MATCHES ^mpi)
    message(STATUS "MPI detected")
    add_compile_definitions(USE_MPI)
endif()

add_subdirectory(${CMAKE_SOURCE_DIR}/src)