package com.wiell.messagebroker.inmemory

import com.wiell.messagebroker.MessageConsumer
import com.wiell.messagebroker.MessageHandler
import com.wiell.messagebroker.MessageRepository
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository
import spock.lang.Specification

class InMemoryMessageRepositoryTest extends Specification {
    def repo = new InMemoryMessageRepository()
    def consumer = MessageConsumer.builder('consumer', {} as MessageHandler).build()
    def callback = Mock(MessageRepository.MessageCallback)

    def 'Can take a job'() {
        enqueue('message')

        when:
            take(1)
        then:
            1 * callback.messageTaken(consumer, _ as String, 'message')
    }

    def 'Can request more jobs then enqueued'() {
        enqueue('message')

        when:
            take(2)
        then:
            1 * callback.messageTaken(consumer, _ as String, 'message')
    }

    def 'Can take several enqueued jobs'() {
        enqueue('message 1', 'message 2')

        when:
            take(2)
        then:
            1 * callback.messageTaken(consumer, _ as String, 'message 1')
        then:
            1 * callback.messageTaken(consumer, _ as String, 'message 2')
    }

    def 'Can take fewer jobs then enqueued'() {
        enqueue('message 1', 'message 2')

        when:
            take(1)
        then:
            1 * callback.messageTaken(consumer, _ as String, 'message 1')
    }

    private enqueue(String... messages) {
        messages.each {
            repo.addMessage('queue', [consumer], it)
        }
    }

    private take(int jobsToTake) {
        repo.takeMessages([(consumer): jobsToTake], callback)
    }
}
