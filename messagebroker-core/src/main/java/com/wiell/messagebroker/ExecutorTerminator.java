package com.wiell.messagebroker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

class ExecutorTerminator {
    static void shutdownAndAwaitTermination(ExecutorService... executors) {
        for (ExecutorService executor : executors)
            shutdownAndAwaitTermination(executor);
    }

    private static void shutdownAndAwaitTermination(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(1, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
