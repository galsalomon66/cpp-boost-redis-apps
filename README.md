# C++ Boost Redis Asio Applications

the purpose of this repo is to search for the better perform redis/boost/asio client.
the valkey-server enables to increase throughput by adding io-threads=4 to its configuration

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

## Running
both applications enable to control
-- number of redis operation
-- connection pool size
-- number of threads
-- number of coroutines 

./multiple_ioc/multiple_ioc 200000 4 512
./single_ioc/single_ioc 200000 1 1 512
