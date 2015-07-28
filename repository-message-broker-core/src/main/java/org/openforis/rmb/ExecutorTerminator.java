package org.openforis.rmb;

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
                executor.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
