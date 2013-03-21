package org.cobbzilla.util.mq.kestrel;

import org.cobbzilla.util.mq.MqProducer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class KestrelAckIT extends KestrelBasicIT {

    private static final Logger LOG = LoggerFactory.getLogger(KestrelAckIT.class);

    @Test
    public void dummy () {}

    @Test
    public void testAckWorks () throws Exception {

        AtomicInteger messageCounter = new AtomicInteger(0);
        final int messageCount = 5;
        MultiConsumer consumer = new MultiConsumer(1, messageCount, messageCounter);

        // Put 5 messages on the queue
        MqProducer producer = getProducer();
        for (int i=0; i< messageCount; i++) {
            String message = randomToken();
            producer.send(message);
        }

        // Register a consumer
        final KestrelClient client = getClient();
        client.registerConsumer(consumer, queueName, errorQueueName);

        while (consumer.instanceMessageCount < messageCount) {
            synchronized (MultiConsumer.lock) {
                MultiConsumer.lock.wait(100);
            }
        }

        // Got all the messages, kill all the clients
        shutdownClients();

        // Send one more message with a fresh producer
        String lastMessage = randomToken();
        LOG.info("lastMessage="+lastMessage);
        getProducer().send(lastMessage);

        // Now register a fresh listener, there should be ONLY the lastMessage on the queue
        final SimpleConsumer simpleConsumer = new SimpleConsumer();
        getClient().registerConsumer(simpleConsumer, queueName, errorQueueName);

        while (simpleConsumer.lastMessageReceived == null) {
            synchronized (simpleConsumer.lock) {
                simpleConsumer.lock.wait(WAIT_TIME);
            }
        }

        Thread.sleep(250); // just to see if anything else comes in on the queue (shouldn't be anything)

        assertEquals(lastMessage, simpleConsumer.firstMessageReceived);
        assertEquals(lastMessage, simpleConsumer.lastMessageReceived);
    }

}
