package util

import com.wiell.messagebroker.MessageHandler
import org.spockframework.runtime.SpockTimeoutError
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class TestHandler<T> implements MessageHandler<T> {
    private final currentlyExecuting = new AtomicInteger()
    private final Random random = new Random()
    final ConcurrentHashMap<T, Boolean> messagesHandled = new ConcurrentHashMap()
    int workerCount = 1
    def handlerDelayMillis = 0
    def randomHandlerDelayMillis = 0
    int timeoutSecs = 5

    TestHandler() {
    }

    void handle(T message) {
        def value = currentlyExecuting.incrementAndGet()
        if (value > workerCount)
            throw new IllegalStateException("More than $workerCount handlers executing at the same time")

        delay(message)
        messagesHandled[message] = true

        def afterValue = currentlyExecuting.decrementAndGet()
        if (afterValue >= workerCount)
            throw new IllegalStateException("More than $workerCount handlers executing at the same time")
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
            def unexpected = minus(expectedSet, actual)
            def notHandled = minus(actual, expectedSet)
            assert unexpected.empty
            assert notHandled.empty
        }
        } catch(SpockTimeoutError e) {
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
