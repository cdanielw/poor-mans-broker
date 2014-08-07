package com.wiell.messagebroker;


import java.util.concurrent.*;

public abstract class AbstractMessageBroker implements MessageBroker, Publisher {
    private final MessageRepository messageRepository;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public AbstractMessageBroker(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    public final void start() {
    }

    public final void stop() {
        executor.shutdownNow();
    }

    public final <M> MessageQueue.Builder<M> queue(Class<M> messageType) {
        return new MessageQueue.Builder<M>(messageType, this);
    }

    public final <M, R> RequestResponseMessageQueue.Builder<M, R> requestResponseQueue(RespondingMessageHandler<M, R> responseHandler) {
        return new RequestResponseMessageQueue.Builder<M, R>(responseHandler, this);
    }

    public <M> void publish(M message, MessageQueue.Default<M> queue) {
//        messageRepository.insert(serializeMessage(message), consumers);
//        doAsync {
//            messageRepository.takePendingMessages(queue.id);
//            for (notification: notifications)
//                notification.consumer.push(notification.message, new After() {
//                    void success() {
//                        messageRepository.done(notification.messageId);
//                    }
//
//                    void error() {
//                        // TODO: Log (with some injected API)
//                        messageRepository.retry(notification.message, notification.consumerId);
//                    }
//                });
//        }
    }

    public <M, R> Future<R> publish(final M message, final RequestResponseMessageQueue.Default<M, R> queue) {
        final Future<R> future = executor.submit(new Callable<R>() {
            public R call() throws Exception {
                return queue.respondingMessageHandler.handle(message);
            }
        });
        executor.submit(new Runnable() {
            public void run() {
                try {
                    R response = future.get();
                    for (RequestResponseMessageQueue.Consumer<M, R> consumer : queue.consumers)
                        consumer.handle(new MessageResponse<M, R>(message, response));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        });
        return future;
    }
}
