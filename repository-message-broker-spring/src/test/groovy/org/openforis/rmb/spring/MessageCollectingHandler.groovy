package org.openforis.rmb.spring

import org.openforis.rmb.MessageHandler

class MessageCollectingHandler implements MessageHandler<String> {
    final List<String> messages = []

    void handle(String message) {
        messages << message
    }
}
