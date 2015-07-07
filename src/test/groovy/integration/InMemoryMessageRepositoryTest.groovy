package integration

import com.wiell.messagebroker.MessageConsumer
import com.wiell.messagebroker.MessageHandler
import com.wiell.messagebroker.MessageProcessingJob
import com.wiell.messagebroker.MessageProcessingJobRequest
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository
import spock.lang.Specification

class InMemoryMessageRepositoryTest extends Specification {
    def repo = new InMemoryMessageRepository()
    def consumer = MessageConsumer.builder('consumer', {} as MessageHandler).build()
    def callback = Mock(MessageProcessingJob.Callback)

    def 'Can take an enqueued job'() {
        enqueue('message')

        when: take(1)
        then: 1 * callback.onJob(_)
    }

    def 'Can request more jobs then enqueued'() {
        enqueue('message')

        when: take(2)
        then: 1 * callback.onJob(_)
    }

    def 'Can take several enqueued jobs'() {
        enqueue('message 1', 'message 2')

        when: take(2)
        then: 2 * callback.onJob(_)
    }

    def 'Can take fewer jobs then enqueued'() {
        enqueue('message 1', 'message 2')

        when: take(1)
        then: 1 * callback.onJob(_)
    }

    // TODO: Concurrent something...

    private enqueue(String... messages) {
        messages.each {
            repo.enqueue('queue', [consumer], it)
        }
    }

    private take(int jobsToTake) {
        repo.takeJobs([new MessageProcessingJobRequest('consumer', jobsToTake)], callback)
    }
}
