#define BOOST_ASIO_DISABLE_BUFFER_DEBUGGING
#define BOOST_REDIS_NO_LOG

#include <boost/asio.hpp>
#include <boost/redis.hpp>
#include <boost/redis/src.hpp> // <-- Needed if you don't link Boost.Redis as a compiled library.
#include <iostream>
#include <mutex>
#include <vector>
#include <queue>
#include <memory>
#include <condition_variable>
#include <chrono>

namespace net = boost::asio;
using namespace std::chrono_literals;
using boost::redis::connection;
using boost::redis::config;
using boost::redis::request;
using boost::redis::response;

// Global atomic counter for active tasks(coroutines)
std::atomic_int actives = 0;

// Global condition variable and mutex for task completion
std::condition_variable completion_cv;
std::mutex completion_mutex;

std::string g_key_prefix = "key_" + std::to_string(int(getpid())) + "_"; 

class RedisPool {
// handling of connection pool
  public:
    RedisPool(net::io_context& ioc, config const& cfg, std::size_t size)
      : ioc_(ioc), cfg_(cfg) {
	{
	  for (std::size_t i = 0; i < size; ++i) {
	    auto conn = std::make_shared<connection>(ioc_.get_executor());
	    conn->async_run(cfg_, {}, net::consign(net::detached, conn));
	    pool_.push(conn);
	  }
	}
      }

    std::shared_ptr<connection> acquire() {
      std::unique_lock<std::mutex> lock(mutex_);

      {//wait until pool is not empty
	cv_.wait(lock, [this]() { return !pool_.empty(); });  // wait until pool is not empty
      }

      auto conn = pool_.front();
      pool_.pop();
      return conn;
    }

    void release(std::shared_ptr<connection> conn) {
      {//connection is back to pool
	std::lock_guard<std::mutex> lock(mutex_);
	pool_.push(conn);
      }
      cv_.notify_one();  // wake up one waiting thread
    }

    int current_pool_size() {
      std::lock_guard<std::mutex> lock(mutex_);
      return pool_.size();
    }

    void cancel_connection() {
      std::lock_guard<std::mutex> lock(mutex_);
      while (!pool_.empty()) {
	pool_.front()->cancel();
	pool_.pop();
      }
    }

  private:
    net::io_context& ioc_;
    config cfg_;
    std::queue<std::shared_ptr<connection>> pool_;
    std::mutex mutex_;
    std::condition_variable cv_;
};

void increase_actives() {
  //actives++;
  actives.fetch_add(1, std::memory_order_relaxed);
  completion_cv.notify_all();
}

void decrease_actives() {
  //actives--;
  actives.fetch_sub(1, std::memory_order_relaxed);
  completion_cv.notify_all();
}


net::awaitable<void> setnx(std::shared_ptr<connection> conn, std::string key,RedisPool* rd_pool,bool release_=true) {

  increase_actives();
  request req;
  req.push("SETNX", key,"1000");
  response<std::string> resp;

  try {
    co_await conn->async_exec(req, resp, net::deferred);
    //auto& result = std::get<0>(resp);

  } catch (std::exception& e) {
    std::cerr << "Redis error: " << e.what() << "\n";
  }

  //upon single connection usage, the release_ sould be false
  if(release_) {
    rd_pool->release(conn);
  } 


  decrease_actives();
}

net::awaitable<void> get_and_incr(std::shared_ptr<connection> conn, std::string key,RedisPool* rd_pool,bool release_=true) {

  increase_actives();

  request req;
  response<std::string> resp;
  req.push("GET", key);

  try {
    co_await conn->async_exec(req, resp, net::deferred);

    auto& result = std::get<0>(resp);
    if (result.has_value()) {
      //std::cout << "Got [" << key << "] = " << result.value() << "\n";
      request req1;
      req1.push("INCR", key);
      co_await conn->async_exec(req1, resp, net::deferred);
    }

  } catch (std::exception& e) {
    std::cerr << "Redis error: " << e.what() << "\n";
  }

  //the release_ could be false upon using the same connection (single connection option)
  if(release_) {
    rd_pool->release(conn);
  }

  decrease_actives();
}

net::awaitable<void> set_get_incr(std::shared_ptr<connection> conn, std::string key,RedisPool* rd_pool,bool release_=true) {

  increase_actives();

  request req;
  response<std::string> resp;
  req.push("SETNX", key,"1000");


  try {
    //co_await conn->async_exec(req, resp, use_awaitable);
    co_await conn->async_exec(req, resp, net::deferred);
    //std::cout << "Worker " << id << " got response: " << "\n";
  } catch (std::exception& e) {
    //std::cerr << "Worker " << id << " error: " << e.what() << "\n";
  }

  req.clear();
  req.push("GET", key);
  try {
    co_await conn->async_exec(req, resp, net::deferred);
    //std::cout << "Worker " << id << " got response: GET "  << "\n";
  } catch (std::exception& e) {
    //std::cerr << "Worker " << id << " error: " << e.what() << "\n";
  }

  req.clear();
  req.push("INCR", key);
  try {
    co_await conn->async_exec(req, resp, net::deferred);
    //std::cout << "Worker " << id << " got response: INCR " << "\n";
  } catch (std::exception& e) {
    //std::cerr << "Worker " << id << " error: " << e.what() << "\n";
  }

  //the release_ could be false upon using the same connection (single connection option)
  if(release_) {
    rd_pool->release(conn);
  }

  decrease_actives();
}

int g_num_of_operations=0;
int g_connection_pool_size=0;
int g_no_connection=0;
int g_thead_pool_size=0;
int g_num_of_actives=0;

bool launch_redis_operations(net::io_context& ioc, RedisPool& my_connection_pool)
{
  //the connection pool is used for all operations for acquire and release
  for (int i = 0; i < g_num_of_operations; ++i) {

    std::string key = g_key_prefix + std::to_string(i);

    //wait until the number of active tasks is less than g_num_of_actives
    std::unique_lock lock(completion_mutex);
    completion_cv.wait(lock, [&] {
    return actives.load(std::memory_order_relaxed) < g_num_of_actives;
    });

    //net::co_spawn(ioc, setnx(my_connection_pool.acquire(), key, &my_connection_pool), net::detached);
    //net::co_spawn(ioc, get_and_incr(my_connection_pool.acquire(), key, &my_connection_pool), net::detached);
    net::co_spawn(ioc, set_get_incr(my_connection_pool.acquire(), key, &my_connection_pool), net::detached);
  }

  std::cout << "##my_connection_pool.current_pool_size() = " << my_connection_pool.current_pool_size() << std::endl;

  return true;
}

bool launch_redis_operations_single_connection(net::io_context& ioc, RedisPool& my_connection_pool)
{
//the same connetion is used for all operations (no release or acquire)
  std::shared_ptr<connection> conn1,conn2;
  conn1 = my_connection_pool.acquire();
  //conn2 = my_connection_pool.acquire();

  for (int i = 0; i < g_num_of_operations; ++i) {

    std::string key = g_key_prefix + std::to_string(i);

    std::unique_lock lock(completion_mutex);
    completion_cv.wait(lock, [&] {
    return actives.load(std::memory_order_relaxed) < g_num_of_actives;
    });

    //net::co_spawn(ioc, setnx(conn1, key, &my_connection_pool, false), net::detached,false);
    //net::co_spawn(ioc, get_and_incr(conn2, key, &my_connection_pool, false), net::detached,false);
    net::co_spawn(ioc, set_get_incr(conn1, key, &my_connection_pool, false), net::detached);
  }

  return true;
}

void wait_for_task_completion() {
    std::unique_lock<std::mutex> lock(completion_mutex);
    completion_cv.wait(lock, [&]() { return actives == 0; });
}

int main(int argc,char** argv)
{
  if(argc < 5) {
    std::cout << "num-of-redis-operation, connection-pool-size, number of threads, number of co-routines" << std::endl;
    return -1;
  }

  //close stderr because of boost/redis debug messages (instead of redirecting to /dev/null).
  //no problem to comment this line
  close(2);

  net::io_context ioc;
  auto work_guard = boost::asio::make_work_guard(ioc);

  {
    config cfg;
    cfg.addr.host = "localhost"; 
    cfg.addr.port = "6379";


    g_num_of_operations = atoi(argv[1]);
    g_connection_pool_size = atoi(argv[2]);
    g_thead_pool_size = atoi(argv[3]);
    g_num_of_actives = atoi(argv[4]);

    std::cout << "g_num_of_operations = " << g_num_of_operations 
      << " g_connection_pool_size = " << g_connection_pool_size 
      << " g_thead_pool_size = " << g_thead_pool_size 
      << " g_num_of_actives = " << g_num_of_actives << std::endl; 

    if(g_connection_pool_size == 1 && g_thead_pool_size > 1) {
      //several threads are trying to use the same connection
      std::cout << "g_connection_pool_size = 1 and g_thead_pool_size > 1, abort execution" << std::endl;
      return -1;
    }

    RedisPool my_connection_pool(ioc, cfg, g_connection_pool_size);

    //attaching thread pool to io_context
    std::vector<std::thread> threads;
    for (int i = 0; i < g_thead_pool_size; ++i) {
      threads.emplace_back([&ioc] {
	  ioc.run();  // Each thread runs the same io_context
	  });
    }

    if(g_connection_pool_size == 1)
    {
      launch_redis_operations_single_connection(ioc,my_connection_pool);
    }
    else launch_redis_operations(ioc,my_connection_pool);

    wait_for_task_completion();
    ioc.stop();
    for (auto& t : threads) { t.join(); }

  }

  return 0;
}

