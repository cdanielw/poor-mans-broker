package com.wiell.messagebroker;

import com.wiell.messagebroker.MessageConsumer;
import com.wiell.messagebroker.MessageProcessingJob;
import com.wiell.messagebroker.MessageProcessingJobRequest;

import java.util.Collection;
import java.util.List;

public interface MessageRepository {
    void enqueue(String queueId, List<MessageConsumer<?>> consumers, Object message);

    void takeJobs(Collection<MessageProcessingJobRequest> requests, MessageProcessingJob.Callback callback);

    void keepAlive(MessageProcessingJob job);

    void completed(MessageProcessingJob job);

    void failed(MessageProcessingJob job, int retries, Exception exception);
}
