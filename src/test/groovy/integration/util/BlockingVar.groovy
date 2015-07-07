package integration.util

import org.spockframework.runtime.SpockTimeoutError
import spock.util.concurrent.BlockingVariable

class BlockingVar<T> extends BlockingVariable<T> {
    private final String timeoutMessageFormat

    BlockingVar(String timeoutMessageFormat) {
        this(1, timeoutMessageFormat)
    }

    BlockingVar(double timeout, String timeoutMessageFormat) {
        super(timeout)
        this.timeoutMessageFormat = timeoutMessageFormat
    }

    @SuppressWarnings("GroovyUncheckedAssignmentOfMemberOfRawType")
    T get() throws InterruptedException {
        try {
            return super.get()
        } catch (SpockTimeoutError e) {
            throw new SpockTimeoutError(e.timeout, String.format(timeoutMessageFormat, e.timeout))
        }
    }

}
