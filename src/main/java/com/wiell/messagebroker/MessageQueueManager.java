package com.wiell.messagebroker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

final class MessageQueueManager {
    private final MessageRepository repository;
    private final TransactionSynchronizer transactionSynchronizer;
    private final WorkerPool workerPool;

    private final Map<String, List<MessageConsumer<?>>> consumersByQueueId = new ConcurrentHashMap<String, List<MessageConsumer<?>>>();
    private final Set<String> consumerIds = new HashSet<String>(); // For asserting global consumer id uniqueness

    private final AtomicBoolean started = new AtomicBoolean();

    public MessageQueueManager(MessageBrokerConfig config) {
        this.repository = config.messageRepository;
        this.transactionSynchronizer = config.transactionSynchronizer;
        this.workerPool = new WorkerPool(new JobListener());
    }

    <M> void publish(String queueId, M message) {
        assertInTransaction();
        List<MessageConsumer<?>> consumers = consumersByQueueId.get(queueId);
        repository.enqueue(queueId, consumers, message);
        pollForJobsOnCommit();
    }

    void registerQueue(String queueId, List<MessageConsumer<?>> consumers) {
        assertConsumerUniqueness(consumers);
        consumersByQueueId.put(queueId, consumers);
        for (MessageConsumer<?> consumer : consumers)
            workerPool.addWorkersFor(consumer);
    }


    void start() {
        started.set(true);
        // TODO: Schedule abandoned jobs check
    }

    void stop() {
        started.set(false);
        workerPool.stop();
    }

    private void pollForJobsOnCommit() {
        transactionSynchronizer.notifyOnCommit(new TransactionSynchronizer.CommitListener() {
            public void committed() {
                takeJobs();
            }
        });
    }

    private void takeJobs() {
        if (!started.get())
            return;
        try {
            workerPool.execute(new WorkerPool.JobTaker() {
                public void takeJobs(Collection<MessageProcessingJobRequest> requests, MessageProcessingJob.Callback callback) {
                    repository.takeJobs(requests, callback);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop();
        }
    }

    private void assertInTransaction() {
        if (!transactionSynchronizer.isInTransaction())
            throw new NotInTransaction("Trying to publish message outside of a transaction");
    }

    private void assertConsumerUniqueness(List<MessageConsumer<?>> consumers) {
        for (MessageConsumer<?> consumer : consumers)
            if (!consumerIds.add(consumer.id))
                throw new IllegalArgumentException("Duplicate consumer id: " + consumer.id);
    }

    private class JobListener implements MessageProcessingListener {
        public void jobsTaken(Collection<MessageProcessingJob> jobs) {
            if (!jobs.isEmpty())
                takeJobs(); // Take new jobs enqueued while taking these
        }

        public void processingStarted(MessageProcessingJob job) {

        }

        public void processingFailed(MessageProcessingJob job, int retries, Exception exception) {
            repository.failed(job, retries, exception);
            // TODO: Update timestamp in repository
            // TODO: Keep track of a global retry count, and pass to the error handler

            // After error handler has run, the job is retried, considered finished or abandoned (?)
            // Handler can make decisions as it wants, do throttling etc.
            // Skip ThrottleStrategy and maxRetries for a consumer - have a user specified ErrorHandler

            // How to prevent another process taking over the job while an error handler is throttling a retry?
            // Cannot lock the whole job using a separate state - the machine doing the error handling can crash, and lock the job forever
            // Easy way to deal with keep-alive while throttling.
            //      Add throttling part of the API? Return Throttle(10, SECONDS)?
            //          Include timeout in db job row, and increase it with the throttle time?
            //

            // TODO: Execute error handling strategy
        }

        public void processingCompleted(MessageProcessingJob job) {
            repository.completed(job);
            takeJobs(); // Take new jobs enqueued while processing this
        }

        public void keepAlive(MessageProcessingJob job) {
            repository.keepAlive(job);
        }

    }
}
