cmake_minimum_required(VERSION 3.16)
project(CppBoostRedisApps)

# Require the user to define BOOST_DEPLOYED_DIRECTORY
if(NOT DEFINED ENV{BOOST_DEPLOYED_DIRECTORY})
  message(FATAL_ERROR "Please set BOOST_DEPLOYED_DIRECTORY environment variable before running CMake.")
endif()

set(BOOST_INCLUDE_DIR "$ENV{BOOST_DEPLOYED_DIRECTORY}/boost/include")

include_directories(${BOOST_INCLUDE_DIR})

add_subdirectory(single_ioc)
add_subdirectory(multiple_ioc)

