package org.cobbzilla.util.mq.kestrel;

import org.cobbzilla.util.mq.MqConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
class SimpleConsumer implements MqConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);

    public final Object lock = new Object();

    public String firstMessageReceived = null;
    public String lastMessageReceived = null;

    @Override
    public void onMessage(Object message) throws Exception {

        LOG.info(getClass().getSimpleName()+": onMessage received message="+message);

        final String messageString = message.toString();

        if (firstMessageReceived == null) firstMessageReceived = messageString;
        lastMessageReceived = messageString;

        synchronized (lock) {
            lock.notify();
        }
    }

}
