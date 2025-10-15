#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    max_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::threadFunction(std::vector<int> work, int num_total_tasks)
{
    for (int i : work) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::thread workers[max_threads];
    std::vector<int> args[max_threads];

    this->runnable = runnable;

    for (int i = 0; i < num_total_tasks; i++) {
        args[i % max_threads].push_back(i);
    }

    for (int i = 1; i < max_threads; i++) {
        workers[i] = std::thread(&TaskSystemParallelSpawn::threadFunction, this, args[i], num_total_tasks);;
    }

    threadFunction(args[0], num_total_tasks);

    for (int i = 1; i < max_threads; i++)
    {
        workers[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}



TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    threadPool = std::vector<std::thread>(num_threads);
    remaining.store(0);
    done.store(0);
    shutdown.store(false);
    for (int i = 0; i < num_threads; i++)
    {
        threadPool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::threadFunction, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    shutdown.store(true);
    for (int i = 0; i < threadPool.size(); i++)
    {
        threadPool[i].join();
    }
}


void TaskSystemParallelThreadPoolSpinning::threadFunction()
{
    while (!shutdown.load())
    {

        remaining_lock.lock();
        if (remaining <= 0)
        {
            remaining_lock.unlock();
            continue;
        }
        int myRemaining = remaining--;
        remaining_lock.unlock();
        runnable->runTask(myRemaining - 1, num_total_tasks);
        done++;
       
    }
}


void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    done.store(0);
    remaining_lock.lock();
    remaining.store(num_total_tasks);
    remaining_lock.unlock();
    //put work in queue
    while (done.load() < num_total_tasks)
    {
        ;;
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::threadFunction()
{
    while (!shutdown.load())
        {
            std::unique_lock<std::mutex> lock_rm(remaining_lock);
            remaining_cv.wait(lock_rm, [this]{return remaining > 0 || shutdown.load();});
            if (shutdown.load())
                break;
            int myRemaining = remaining--;
            lock_rm.unlock();
            runnable->runTask(myRemaining - 1, num_total_tasks);

            std::unique_lock<std::mutex> lock(done_lock);
            done++;
            if (done == num_total_tasks)
            {
                lock.unlock();
                done_cv.notify_one();
            }
               
        }
}


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    threadPool = std::vector<std::thread>(num_threads);
    remaining = 0;
    done = 0;
    shutdown.store(false);
    for (int i = 0; i < num_threads; i++)
    {
        threadPool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadFunction, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    std::unique_lock<std::mutex> lock_rm(remaining_lock);
    shutdown.store(true);
    lock_rm.unlock();

    remaining_cv.notify_all();
    for (int i = 0; i < threadPool.size(); i++)
    {
        threadPool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    done_lock.lock();
    done = 0;
    done_lock.unlock();

    std::unique_lock<std::mutex> lock_rm(remaining_lock);
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    remaining = num_total_tasks;
    lock_rm.unlock();
    remaining_cv.notify_all();
   
    //put work in queue
    std::unique_lock<std::mutex> lock(done_lock);
    done_cv.wait(lock, [this]{return done == this->num_total_tasks;});
    lock.unlock();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {

                                                       
    // so first we                                                    
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

