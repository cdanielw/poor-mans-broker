package org.openforis.rmb.spring

import org.openforis.rmb.MessageQueue
import org.springframework.transaction.annotation.Transactional

class MessagePublishingService {
    private final MessageQueue<String> messageQueue

    MessagePublishingService(MessageQueue<String> messageQueue) {
        this.messageQueue = messageQueue
    }

    @Transactional
    void publish(String message) {
        messageQueue.publish(message)
    }
}
