package com.wiell.messagebroker.jdbc;

import com.wiell.messagebroker.MessageConsumer;
import com.wiell.messagebroker.MessageProcessingJob;
import com.wiell.messagebroker.MessageProcessingJobRequest;
import com.wiell.messagebroker.MessageSerializer;
import com.wiell.messagebroker.MessageRepository;

import java.util.Collection;
import java.util.List;

public final class JdbcMessageRepository implements MessageRepository {
    public JdbcMessageRepository(JdbcConnectionManager connectionManager, MessageSerializer messageSerializer) {

    }

    public void enqueue(String queueId, List<MessageConsumer<?>> consumers, Object message) {

    }

    public void takeJobs(Collection<MessageProcessingJobRequest> requests, MessageProcessingJob.Callback callback) {

    }

    public void keepAlive(MessageProcessingJob job) {

    }

    public void completed(MessageProcessingJob job) {

    }

    public void failed(MessageProcessingJob job, int retries, Exception exception) {

    }
/*

 -----------------
| message_processing
|-----------------
| message_id
| consumer_id
| version
| last_updated
|


*/
}
