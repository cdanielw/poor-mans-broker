package org.openforis.rmb.spring

import org.openforis.rmb.MessageHandler

import java.util.concurrent.CopyOnWriteArrayList

class MessageCollectingHandler implements MessageHandler<String> {
    final List<String> messages = new CopyOnWriteArrayList()

    void handle(String message) {
        messages << message
    }
}
