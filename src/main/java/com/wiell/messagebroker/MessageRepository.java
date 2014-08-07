package com.wiell.messagebroker;

import java.util.List;

public interface MessageRepository {
//    public void insert(SerializedMessage message, List<String> consumerIds);
//
//    public TakenMessages takePendingMessages(String queueId); // TODO: Return some iterator instead, to allow for streaming?
//
//    public TakenMessages takePendingMessages();
//
//    public void done(String messageId);


    public static class SerializedMessage {
        String queueId;
        String message;
    }
}
