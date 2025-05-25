#include <boost/asio.hpp>
#include <boost/redis/src.hpp> // <-- Needed if you don't link Boost.Redis as a compiled library.
#include <boost/redis.hpp>
#include <iostream>
#include <thread>
#include <vector>
#include <memory>
#include <atomic>

namespace asio = boost::asio;
namespace redis = boost::redis;
namespace net = boost::asio;
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::use_awaitable;
using tcp = asio::ip::tcp;

int NUM_THREADS = 4;
std::atomic<int> active_tasks = 0;
int g_active_tasks = 0;

//using mutex and condition variable to wait for tasks to finish
std::mutex active_tasks_mutex;
std::condition_variable active_tasks_cv;

struct Worker {
  //each worker had its own io_context , thread and connection
  asio::io_context ioc;
  std::shared_ptr<redis::connection> redis_conn;
  //it needs for work_guard to keep the ioc alive(otherwise it will stop)
  asio::executor_work_guard<asio::io_context::executor_type> work_guard;
  std::thread thread;

  Worker()
    : ioc(1),
    redis_conn(std::make_shared<redis::connection>(ioc)),
    work_guard(asio::make_work_guard(ioc)), // keeps ioc alive
    thread([this] { ioc.run(); }) {}
};

void increase_active_tasks() {
  //active_tasks++;
  active_tasks.fetch_add(1, std::memory_order_relaxed);
  active_tasks_cv.notify_all(); 
}

void decrease_active_tasks() {
  //active_tasks--;
  active_tasks.fetch_sub(1, std::memory_order_relaxed);
  active_tasks_cv.notify_all(); 
}

awaitable<void> redis_task_2(std::shared_ptr<redis::connection> conn, int id, const std::string key) {
  //chain of co_awaitable calls (GET and INCR)

  redis::request req;
  redis::response<std::string> resp;
  increase_active_tasks();

  req.push("GET", key);
  try {
    co_await conn->async_exec(req, resp, use_awaitable);
    auto& result = std::get<0>(resp);
    if (result.has_value()) {
      req.clear();
      req.push("INCR", key);
      co_await conn->async_exec(req, resp, use_awaitable);
      //std::cout << "Worker " << id << " got response: INCR " << "\n";
    }
  } catch (std::exception& e) {
    //std::cerr << "Worker " << id << " error: " << e.what() << "\n";
  }

  decrease_active_tasks();
}

awaitable<void> redis_task(std::shared_ptr<redis::connection> conn, int id, const std::string key) {

  redis::request req;
  req.push("SETNX", key,"1000");
  redis::response<std::string> resp;

  increase_active_tasks();

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
    co_await conn->async_exec(req, resp, use_awaitable);
    //std::cout << "Worker " << id << " got response: GET "  << "\n";
  } catch (std::exception& e) {
    //std::cerr << "Worker " << id << " error: " << e.what() << "\n";
  }

  req.clear();
  req.push("INCR", key);
  try {
    co_await conn->async_exec(req, resp, use_awaitable);
    //std::cout << "Worker " << id << " got response: INCR " << "\n";
  } catch (std::exception& e) {
    //std::cerr << "Worker " << id << " error: " << e.what() << "\n";
  }

  decrease_active_tasks();
}

void wait_for_task_completion()
{
  std::unique_lock lock(active_tasks_mutex);
  active_tasks_cv.wait(lock, [] { return active_tasks == 0; });
}

void stop_workers(std::vector<std::unique_ptr<Worker>>& workers) {
  for (auto& worker : workers) {
    worker->work_guard.reset(); // Release the work guard
    worker->ioc.stop();
    worker->thread.join();
  }
}

int main(int argc, char* argv[]) {

  if((argc<4)){
    std::cout << "Usage: <number_of_redis_operations> <num of threads> <num of co-routines>\n";
    return -1;
  }

  // setting up the number of redis operations and number of threads
  auto number_of_redis_operations = std::stoi(argv[1]);
  NUM_THREADS = std::stoi(argv[2]);
  int num_of_coroutines = std::stoi(argv[3]);

  std::cout << "Number of redis operations: " << number_of_redis_operations 
    << " number of threads: " << NUM_THREADS 
    << " number of coroutines: " << num_of_coroutines << "\n";

  //close stderr because of boost/redis debug messages (instead of redirecting to /dev/null).
  //no problem to comment this line
  close(2);

  // Create workers (as many as threads)
  std::vector<std::unique_ptr<Worker>> workers;
  for (int i = 0; i < NUM_THREADS; ++i) {
    auto worker = std::make_unique<Worker>();

    co_spawn(worker->ioc, [conn=worker->redis_conn]() -> awaitable<void> {
	try{
	redis::config cfg;
	cfg.addr.host = "127.0.0.1"; //redis server address
	cfg.addr.port = "6379";
	co_await conn->async_run(cfg, {}, use_awaitable);
	} catch (std::exception& e) {
	std::cerr << "Worker  error: " << e.what() << "\n";
	}
	}, detached);

    workers.push_back(std::move(worker));
  }

  // Simple round-robin scheduler
  std::atomic<int> counter = 0;
  for (int i = 0; i < number_of_redis_operations; ++i) {
    std::string key = "key_new_" + std::to_string(i);
    int idx = counter++ % NUM_THREADS;
    auto& worker = workers[idx];

    //limit the number of active tasks according to the user input
    std::unique_lock lock(active_tasks_mutex);
    active_tasks_cv.wait(lock, [num_of_coroutines] { return active_tasks < num_of_coroutines; });

    try{ 
      //spawn the redis tasks
      co_spawn(worker->ioc, redis_task(worker->redis_conn, idx, key), detached);
      //co_spawn(worker->ioc, redis_task_2(worker->redis_conn, idx, key), detached);
    } catch (std::exception& e) {
      std::cerr << "Worker " << idx << " error: " << e.what() << "\n";
    }
    catch (...){
      std::cerr << "Worker " << idx << " error: unknown error\n";
    }
  }

  wait_for_task_completion();

  stop_workers(workers);

  return 0;
}

