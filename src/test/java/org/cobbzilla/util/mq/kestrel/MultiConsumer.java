package org.cobbzilla.util.mq.kestrel;

import org.cobbzilla.util.mq.MqConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class MultiConsumer implements MqConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MultiConsumer.class);

    public static final Object lock = new Object();
    public AtomicInteger messageCounter;

    private final int id;
    private final int limit;

    public volatile int instanceMessageCount = 0;

    public MultiConsumer (int id, int limit, AtomicInteger messageCounter) {
        this.id = id;
        this.limit = limit;
        this.messageCounter = messageCounter;
    }

    @Override
    public void onMessage(Object message) throws Exception {
        instanceMessageCount++;
        final int totalReceived = messageCounter.incrementAndGet();
        LOG.info("onMessage("+id+"): received "+message+" (total now="+totalReceived+")");
        if (totalReceived >= limit) {
            synchronized (lock) {
                LOG.info("onMessage("+id+"): all messages received, notifying lock");
                lock.notify();
            }
        }
    }
}
