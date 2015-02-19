package com.wiell.messagebroker;

import java.util.Collection;

interface MessageProcessingListener {
    void jobsTaken(Collection<MessageProcessingJob> jobs);

    void processingStarted(MessageProcessingJob job);

    void processingFailed(MessageProcessingJob job, int retries, Exception exception);

    void processingCompleted(MessageProcessingJob job);

    void keepAlive(MessageProcessingJob job);
}
