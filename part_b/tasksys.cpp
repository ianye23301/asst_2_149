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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    //just do runAsyncWithDeps and sync
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    threadPool = std::vector<std::thread>(num_threads);
    shutdown.store(false);
    readyQueue = std::vector<Task*>(0);
    bulkList = std::vector<BulkTask*>(0);
    working.store(0);

    this->num_threads = num_threads;
    last_id = 0;
    for (int i = 0; i < num_threads; i++)
    {
        threadPool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadFunction, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    sync();
    shutdown.store(true);
    readyQueueCv.notify_all();      // wake sleepers so they can see shutdown

    for (int i = 0; i < num_threads; i++)
    {
        threadPool[i].join();
    }
    for (int i = 0; i < bulkList.size(); i++)
    {
        delete bulkList[i];
    }
}


void TaskSystemParallelThreadPoolSleeping::threadFunction()
{
    // pop off of ready queue 
    // wait for ready queue not empty
    while (!shutdown.load())
    {
        std::unique_lock<std::mutex> lock(readyQueueLock);
        readyQueueCv.wait(lock, [this]{return readyQueue.size() || shutdown.load();});
        if (shutdown.load())
            break;
        Task* myTask = readyQueue.back();
        readyQueue.pop_back();
        lock.unlock();

        myTask->runnable->runTask(myTask->id, myTask->num_total_tasks);
        std::unique_lock<std::mutex> ql(readyQueueLock);
        std::unique_lock<std::mutex> bl(bulkLock);

        int old = myTask->bulk->instances.fetch_sub(1);
        int deps_enqueued = 0;

        if (old == 1) {
            for (BulkTask* b : myTask->bulk->myDeps) {
                if (b->remainingDeps.fetch_sub(1) - 1 == 0) {
                    // enqueue all tasks for b
                    for (int i = 0; i < b->num_total_tasks; ++i) {
                        Task* t = new Task{ b->runnable, b->num_total_tasks, i, b->launchId, b };
                        readyQueue.push_back(t);
                    }
                    deps_enqueued += b->num_total_tasks;
                }
            }
        }

        // Now update working: -1 for the finished task, +deps_enqueued for new ones
        working.fetch_add(deps_enqueued - 1);

        // locks go out of scope here (bulk first, then queue)
        bl.unlock();
        ql.unlock();

        readyQueueCv.notify_all();
    }
    
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> deps(0);
    int res = runAsyncWithDeps(runnable, num_total_tasks, deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
                                       
                                                        
    std::unique_lock<std::mutex> ql(readyQueueLock);
    std::unique_lock<std::mutex> bl(bulkLock);
                                                    
    int myId = last_id.fetch_add(1);
    BulkTask* bulkTask = new BulkTask;
    bulkTask->instances.store(num_total_tasks);
    bulkTask->num_total_tasks = num_total_tasks;
    bulkTask->launchId = myId;
    bulkTask->runnable = runnable;
    bulkTask->remainingDeps.store(0);

    //no thread computation is allowed to happen while tasks are being added, 
    
    for (TaskID t : deps)
    {
        for (BulkTask* b : bulkList)
        {
            if (b->launchId == t && b->instances > 0) // ok now the problem is deps remaining = bulk tasks that it depends on that haven't finished. guaranteed that only 1 bulk task corresponds to 1 dep. 
            {
                b->myDeps.push_back(bulkTask);
                bulkTask->remainingDeps++;
            }
        }
    }
    
    bulkList.push_back(bulkTask);

    //add to ready queue if no deps left

    if (bulkTask->remainingDeps == 0) {
        for (int i = 0; i < num_total_tasks; i++)
            {    
                Task* task_p = new Task;
                task_p->runnable = runnable;
                task_p->num_total_tasks = num_total_tasks;
                task_p->id = i;
                task_p->task_id = myId;
                task_p->bulk = bulkTask;
                readyQueue.push_back(task_p);
                working.fetch_add(1);
            } 
    }

    bl.unlock();
    ql.unlock();
    

    readyQueueCv.notify_all();
    
    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    // printf("my id = %d", myId);
    return myId;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    std::unique_lock<std::mutex> lock(readyQueueLock);
    readyQueueCv.wait(lock, [this]{return readyQueue.size() == 0 && working.load() <= 0;});
    lock.unlock();
    return;
}
