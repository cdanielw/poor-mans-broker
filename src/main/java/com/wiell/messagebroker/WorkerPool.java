package com.wiell.messagebroker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class WorkerPool {
    private final MessageProcessingListener listener;
    private final Map<String, BlockingQueue<Worker>> availableWorkersByConsumer = new ConcurrentHashMap<String, BlockingQueue<Worker>>();
    private final ExecutorService workerExecutor = Executors.newCachedThreadPool();
    private final ExecutorService jobTakingExecutor = Executors.newSingleThreadExecutor();
    private AtomicBoolean takingJobs = new AtomicBoolean();

    WorkerPool(MessageProcessingListener listener) {
        this.listener = listener;
    }

    void addWorkersFor(MessageConsumer<?> consumer) {
        availableWorkersByConsumer.put(consumer.id, new ArrayBlockingQueue<Worker>(consumer.workerCount));
        for (int i = 0; i < consumer.workerCount; i++)
            makeWorkerAvailable(new Worker(consumer));
    }

    void execute(final JobTaker jobTaker) throws InterruptedException {
        if (!takingJobs.compareAndSet(false, true))
            return; // We're running already
        jobTakingExecutor.execute(new Runnable() {
            public void run() {
                takeJobs(jobTaker);
            }
        });
    }

    void stop() {
        jobTakingExecutor.shutdownNow();
        workerExecutor.shutdownNow();
    }

    private void takeJobs(JobTaker jobTaker) {
        final Collection<MessageProcessingJob> jobs = new ArrayList<MessageProcessingJob>();
        try {
            Collection<MessageProcessingJobRequest> requests = requestsForAvailableWorkers();
            if (!requests.isEmpty()) {
                jobTaker.takeJobs(requests, new MessageProcessingJob.Callback() {
                    public void onJob(MessageProcessingJob job) {
                        Worker worker = takeWorker(job.consumerId);
                        worker.execute(job);
                        jobs.add(job);
                    }
                });
            }
        } finally {
            takingJobs.set(false);
        }
        listener.jobsTaken(jobs);
    }

    private Collection<MessageProcessingJobRequest> requestsForAvailableWorkers() {
        Collection<MessageProcessingJobRequest> requests = new ArrayList<MessageProcessingJobRequest>();
        for (Map.Entry<String, BlockingQueue<Worker>> entry : availableWorkersByConsumer.entrySet()) {
            String consumerId = entry.getKey();
            BlockingQueue<Worker> availableWorkers = entry.getValue();
            if (!availableWorkers.isEmpty())
                requests.add(new MessageProcessingJobRequest(consumerId, availableWorkers.size()));
        }
        return requests;
    }

    private void makeWorkerAvailable(Worker worker) {
        BlockingQueue<Worker> workers = availableWorkersByConsumer.get(worker.consumer.id);
        workers.add(worker);
    }

    private Worker takeWorker(String consumerId) {
        BlockingQueue<Worker> workers = availableWorkersByConsumer.get(consumerId);
        Worker worker = workers.poll();
        if (worker == null)
            throw new IllegalStateException("No worker available for consumer " + consumerId);
        return worker;
    }

    interface JobTaker {
        void takeJobs(Collection<MessageProcessingJobRequest> requests, MessageProcessingJob.Callback callback);
    }

    private class Worker {
        final MessageConsumer consumer;

        Worker(MessageConsumer consumer) {
            this.consumer = consumer;
        }

        void execute(final MessageProcessingJob job) {
            workerExecutor.execute(new Runnable() {
                @SuppressWarnings("unchecked")
                public void run() {
                    try {
                        int retries = 0;
                        int maxRetries = consumer.maxRetries < 0 ? Integer.MAX_VALUE - 1 : consumer.maxRetries;
                        while (retries <= maxRetries) {
                            try {
                                listener.processingStarted(job);
                                consumer.consume(job.message, keepAlive(job));
                                listener.processingCompleted(job);
                                return;
                            } catch (Exception e) {
                                listener.processingFailed(job, retries, e);
                                retries++;
                            }
                        }
                    } finally {
                        makeWorkerAvailable(Worker.this);
                    }
                }
            });
        }

        private KeepAlive keepAlive(final MessageProcessingJob job) {
            return new KeepAlive() {
                public void send() {
                    listener.keepAlive(job);
                }
            };
        }
    }
}
