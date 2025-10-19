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
    working.store(0);
    lastId.store(0);

    for (int i = 0; i < num_threads; i++)
    {
        threadPool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadFunction, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    sync();

    shutdown.store(true);
    bulkListCv.notify_all();
    for (int i = 0; i < threadPool.size(); i++)
    {
        threadPool[i].join();
    }
    for (auto& pair : allBulks) {
        delete pair.second;  
    }
}


void TaskSystemParallelThreadPoolSleeping::threadFunction()
{
    while (!shutdown.load())
    {
        std::unique_lock<std::mutex> bl(bulkListLock);
        bulkListCv.wait(bl, [this]{return !bulkList.empty() || shutdown.load();});
        if (shutdown.load())
            break;
        BulkTask* myTask = bulkList.back();
        int myRemaining = myTask->remaining.fetch_sub(1);
        if (myRemaining == 1)
        {
            bulkList.pop_back();
        }
        bl.unlock();
        myTask->runnable->runTask(myRemaining - 1, myTask->num_total_tasks);
        
        int myDone = myTask->done++;

        if (myDone == myTask->num_total_tasks - 1)
        {
           bl.lock();

           working--;

           for (BulkTask* b : myTask->children)
           {
                if (b->parents.fetch_sub(1) == 1)
                {
                    bulkList.push_back(b);
                }
           }
           bl.unlock();

           bulkListCv.notify_all();
           continue;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> deps = {};
    // printf("RUN \n");
    runAsyncWithDeps(runnable, num_total_tasks, deps);
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) { 
    int myId = lastId.fetch_add(1);                                                                                      
    std::unique_lock<std::mutex> bl(bulkListLock);
    BulkTask* b = new BulkTask;

    b->launchId = myId;
    b->num_total_tasks = num_total_tasks;
    b->remaining = num_total_tasks;
    b->parents.store(0);
    b->children = {};
    b->runnable = runnable;
    b->done.store(0);

    for (TaskID dep : deps)
    {
        if (allBulks.find(dep) == allBulks.end())
            continue;
        BulkTask* parent = allBulks[dep];
        parent->lock.lock();

        if (parent->done.load() < parent->num_total_tasks)
        {
            parent->children.push_back(b);
            b->parents.fetch_add(1);
        }
        parent->lock.unlock();
    }

    allBulks[myId] = b;
    working.fetch_add(1);

    if (b->parents.load() == 0)
        bulkList.push_back(b);

    bl.unlock();
    bulkListCv.notify_all();    
    return myId;         
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    std::unique_lock<std::mutex> bl(bulkListLock);
    bulkListCv.wait(bl, [this]{
        return working.load() == 0;});
    // printf("ENDED SYNC \n");
    return;
}
