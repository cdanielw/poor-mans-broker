package org.openforis.rmb

import org.openforis.rmb.monitor.MessageUpdateConflictEvent
import org.openforis.rmb.monitor.TakingMessagesFailedEvent
import org.openforis.rmb.objectserialization.ObjectSerializationMessageSerializer
import org.openforis.rmb.spi.MessageDetails
import org.openforis.rmb.spi.MessageProcessingStatus
import org.openforis.rmb.spi.MessageProcessingUpdate
import org.openforis.rmb.spi.MessageRepository
import spock.lang.Specification
import spock.util.concurrent.PollingConditions
import util.CollectingMonitor

import static org.openforis.rmb.spi.MessageProcessingStatus.State.PENDING
import static org.openforis.rmb.spi.MessageProcessingStatus.State.PROCESSING

class MessagePollerTest extends Specification {
    def repository = Mock(MessageRepository)
    def monitor = new CollectingMonitor()
    def serializer = new ObjectSerializationMessageSerializer()
    def consumer = MessageConsumer.builder('consumer id', {} as MessageHandler).build()

    def poller = new MessagePoller(repository, serializer, new Monitors([monitor]))

    def setup() {
        poller.registerConsumers([consumer])
    }

    def 'When repository fails to update message processing, monitors receive a MessageUpdateConflictEvent'() {
        repositoryTakesMessage()
        repository.update(_) >> false
        when:
            poller.poll()
        then:
            new PollingConditions().eventually {
                assert monitor.events.find { it.class == MessageUpdateConflictEvent }
            }
    }

    def 'When taking messages fails, monitors receive a TakingMessagesFailedEvent'() {
        repository.take(*_) >> { throw new RuntimeException() }
        when:
            poller.poll()
        then:
            new PollingConditions().eventually {
                assert monitor.events.find { it.class == TakingMessagesFailedEvent }
            }
    }

    private void repositoryTakesMessage() {
        def update = MessageProcessingUpdate.create(
                new MessageDetails('queue id', 'message id', new Date()),
                consumer,
                new MessageProcessingStatus(PENDING, 0, null, new Date(), 'from version id'),
                new MessageProcessingStatus(PROCESSING, 0, null, new Date(), 'to version id')
        )
        repository.take(_ as Map, _ as MessageRepository.MessageTakenCallback) >> {
            (it[1] as MessageRepository.MessageTakenCallback).taken(update, serializer.serialize('a message'));
        }
    }
}
