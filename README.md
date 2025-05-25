# C++ Boost Redis Asio Applications

This repository contains two C++20 applications that use Boost, Redis, and Asio.

## Requirements

- GCC 10+
- CMake 3.16+
- Boost (custom location)
- OpenSSL

## Setup

Before building, define the environment variable pointing to your deployed Boost directory:
```bash
export BOOST_DEPLOYED_DIRECTORY=<your CEPH workspace>/ceph/build/

