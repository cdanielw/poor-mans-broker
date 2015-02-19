package com.wiell.messagebroker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

final class MessageQueueManager {
    private final MessageRepository repository;
    private final TransactionSynchronizer transactionSynchronizer;
    private final WorkerPool workerPool;

    private final Map<String, List<MessageConsumer<?>>> consumersByQueueId = new ConcurrentHashMap<String, List<MessageConsumer<?>>>();
    private final Set<String> consumerIds = new HashSet<String>(); // For asserting global consumer id uniqueness

    public MessageQueueManager(MessageBrokerConfig config) {
        this.repository = config.messageRepository;
        this.transactionSynchronizer = config.transactionSynchronizer;
        this.workerPool = new WorkerPool(new WorkerPoolListener());
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
        takeJobs();
    }

    // TODO: Schedule - to catch time-outs

    void start() {
        takeJobs();
    }

    void stop() {
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

    private class WorkerPoolListener implements MessageProcessingListener {
        public void jobsTaken(Collection<MessageProcessingJob> jobs) {
            if (!jobs.isEmpty())
                takeJobs();
        }

        public void processingStarted(MessageProcessingJob job) {

        }

        public void processingFailed(MessageProcessingJob job, int retries, Exception exception) {
            repository.failed(job, retries, exception);
            // TODO: Execute error handling strategy
        }

        public void processingCompleted(MessageProcessingJob job) {
            repository.completed(job);
        }

        public void keepAlive(MessageProcessingJob job) {
            repository.keepAlive(job);
        }

    }
}
