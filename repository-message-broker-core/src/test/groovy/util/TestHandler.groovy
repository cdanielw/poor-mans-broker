package util

import org.openforis.rmb.messagebroker.KeepAlive
import org.openforis.rmb.messagebroker.KeepAliveMessageHandler
import org.spockframework.runtime.SpockTimeoutError
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class TestHandler<T> implements KeepAliveMessageHandler<T> {
    private final currentlyExecuting = new AtomicInteger()
    private final Random random = new Random()
    final ConcurrentHashMap<T, Boolean> messagesHandled = new ConcurrentHashMap()
    int messagesHandledInParallel = 1
    def handlerDelayMillis = 0
    def randomHandlerDelayMillis = 0
    int timeoutSecs = 5
    Closure handler

    void handle(T message, KeepAlive keepAlive) {
        def value = currentlyExecuting.incrementAndGet()
        if (value > messagesHandledInParallel)
            throw new IllegalStateException("More than $messagesHandledInParallel handlers executing at the same time")

        if (handler)
            invokeHandler(message, keepAlive)
        delay(message)
        messagesHandled[message] = true

        def afterValue = currentlyExecuting.decrementAndGet()
        if (afterValue >= messagesHandledInParallel)
            throw new IllegalStateException("More than $messagesHandledInParallel handlers executing at the same time")
    }

    private void invokeHandler(T message, KeepAlive keepAlive) {
        try {
            if (handler.maximumNumberOfParameters >= 2)
                handler(message, keepAlive)
            else
                handler(message)
        } catch (Exception e) {
            currentlyExecuting.decrementAndGet()
            throw e
        }
    }

    private void delay(T message) {
        if (handlerDelayMillis) {
            Thread.sleep(handlerDelayMillis instanceof Closure ? handlerDelayMillis(message) : handlerDelayMillis)
        } else if (randomHandlerDelayMillis)
            Thread.sleep((long) random.nextInt(randomHandlerDelayMillis + 1))
    }

    void handled(Collection<T> expected) {
        def expectedSet = new HashSet(expected)
        def conds = new PollingConditions(timeout: timeoutSecs)
        try {
            conds.eventually {
                def actual = messagesHandled.keySet()
                def missing = minus(expectedSet, actual)
                def unexpected = minus(actual, expectedSet)
                assert missing.empty
                assert unexpected.empty
            }
        } catch (SpockTimeoutError e) {
            e.printStackTrace()
            throw e
        }
    }

    void handled(T... messages) {
        handled(messages as List)
    }

    private <T> Collection<T> minus(Set<T> expected, Set<T> actual) {
        expected.findAll {
            !actual.contains(it)
        }
    }
}
