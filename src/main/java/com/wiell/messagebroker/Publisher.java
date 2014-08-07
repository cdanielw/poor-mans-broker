package com.wiell.messagebroker;

import java.util.concurrent.Future;

interface Publisher {
    <M> void publish(M message, MessageQueue.Default<M> queue);

    <M, R> Future<R> publish(M message, RequestResponseMessageQueue.Default<M, R> queue);
}
