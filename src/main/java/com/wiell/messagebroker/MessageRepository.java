package com.wiell.messagebroker;

import java.util.Map;

public interface MessageRepository {
    public <M> void submit(String message, String queueId, Iterable<MessageConsumer<M>> consumers);

    public TakenMessages takePendingMessages(String queueId); // TODO: Return some iterator instead, to allow for streaming?
//
//    public TakenMessages takePendingMessages();

    public void completed(String messageId, String consumerId);


    public static class SerializedMessage {
        String queueId;
        String message;
    }

    public static class TakenMessages {
        Map<String, Message> messageByConsumer;
    }

    public static class Message {

    }
}
