package org.cobbzilla.util.mq.kestrel;

import org.apache.commons.lang3.RandomStringUtils;
import org.cobbzilla.util.mq.MqClient;
import org.cobbzilla.util.mq.MqClientFactory;
import org.cobbzilla.util.mq.MqProducer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.cobbzilla.util.mq.kestrel.ControllableConsumer.Command.*;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class KestrelBasicIT {

    private static final Logger LOG = LoggerFactory.getLogger(KestrelBasicIT.class);

    public static final String TEST_KESTREL_HOST = "kestrel:22133";
    public static final int WAIT_TIME = 100;

    protected MqClientFactory clientFactory = new MqClientFactory();
    protected List<MqClient> clients = new ArrayList<>();

    protected String queueName;
    protected String errorQueueName;

    public static String randomToken() {
        return RandomStringUtils.randomAlphabetic(4).toLowerCase()+System.currentTimeMillis();
    }

    @Before
    public void setUp () throws Exception {
        queueName = "q_"+randomToken();
        errorQueueName = queueName+"_error";
    }

    protected KestrelClient getClient() {
        Properties clientProperties = new Properties();
        clientProperties.setProperty(KestrelClient.PROP_KESTREL_HOSTS, TEST_KESTREL_HOST);
        final KestrelClient client = (KestrelClient) clientFactory.createClient(KestrelClient.class.getCanonicalName(), clientProperties);
        clients.add(client);
        return client;
    }

    protected MqProducer getProducer() {
        return getClient().getProducer(queueName);
    }

    @After
    public void tearDown () throws Exception {
        LOG.info("tearDown: deleting queues and shutting down clients");
        getClient().deleteQueue(queueName);
        getClient().deleteQueue(errorQueueName);
        Thread.sleep(500);
        shutdownClients();
    }

    public void shutdownClients() throws IOException {
        for (MqClient client : clients) {
            client.shutdown();
        }
        clients.clear();
    }

    //    @Test
    public void testSimpleMessages () throws Exception {

        SimpleConsumer consumer = new SimpleConsumer();
        getClient().registerConsumer(consumer, queueName, errorQueueName);

        MqProducer producer = getProducer();
        for (int i=0; i<5; i++) {
            consumer.lastMessageReceived = null;
            assertNull(consumer.lastMessageReceived);

            String message = randomToken();
            producer.send(message);

            while (consumer.lastMessageReceived == null) {
                synchronized (consumer.lock) {
                    consumer.lock.wait(100);
                }
            }

            assertEquals(message, consumer.lastMessageReceived);
        }
    }

//    @Test
    public void testErrorMessage () throws Exception {

        String message = randomToken();
        getProducer().send(message);

        BadConsumer badConsumer = new BadConsumer();
        getClient().registerConsumer(badConsumer, queueName, errorQueueName);

        while (badConsumer.lastMessageReceived == null) {
            synchronized (badConsumer.lock) {
                badConsumer.lock.wait(WAIT_TIME);
            }
        }
        assertEquals(message, badConsumer.lastMessageReceived);

        // register a new consumer on the error queue only, they should get the message
        SimpleConsumer goodConsumer = new SimpleConsumer();
        getClient().registerConsumer(goodConsumer, errorQueueName, null);
        while (goodConsumer.lastMessageReceived == null) {
            synchronized (goodConsumer.lock) {
                goodConsumer.lock.wait(WAIT_TIME);
            }
        }
        assertEquals(message, goodConsumer.lastMessageReceived);
    }

//    @Test
    public void test2Consumers () throws Exception {

        String message1 = randomToken();
        String message2 = randomToken();

        final MqProducer producer = getProducer();
        producer.send(message1);
        getProducer().send(message2);

        // first consumer - read 1 message from the queue
        final KestrelClient client1 = getClient();
        final ControllableConsumer receiver1 = new ControllableConsumer(client1, queueName);
        receiver1.start();
        awaitCommand(receiver1, OPEN);

        // should get the first message
        assertEquals(message1, receiver1.getMostRecentMessage());

        // second consumer - read another message from the queue
        final KestrelClient client2 = getClient();
        final ControllableConsumer receiver2 = new ControllableConsumer(client2, queueName);
        receiver2.start();
        awaitCommand(receiver2, OPEN);

        // should get the second message
        assertEquals(message2, receiver2.getMostRecentMessage());

        // now ABORT the first message
        awaitCommand(receiver1, ABORT);
        client1.shutdown();

        // and ACK the second message
        awaitCommand(receiver2, CLOSE);

        // ask the second receiver to get another message -- should get the first one (that was aborted)
        awaitCommand(receiver2, OPEN);

        // receiver 2 should now get the first message, which was ABORTED by receiver1
        assertEquals(message1, receiver2.getMostRecentMessage());

        receiver1.addCommand(STOP);
        receiver2.addCommand(STOP);
    }

    private void awaitCommand(ControllableConsumer consumer, ControllableConsumer.Command command) throws InterruptedException {
        consumer.addCommand(command);
        while (consumer.getMostRecentCommand() != command) {
            synchronized (consumer.lock) {
                consumer.lock.wait(WAIT_TIME);
            }
            Thread.yield();
        }
    }

//    @Test
    public void testMultipleReceivers () throws Exception {
        int numConsumers = 2;
        int messageMultiple = 5;
        final int totalMessages = messageMultiple * numConsumers;

        MqProducer producer = getProducer();
        for (int i=0; i<totalMessages; i++) {
            producer.send("message"+i+"_"+randomToken());
        }

        AtomicInteger messageCounter = new AtomicInteger(0);
        for (int i=0; i<numConsumers; i++) {
            final MultiConsumer consumer = new MultiConsumer(i, totalMessages, messageCounter);
            getClient().registerConsumer(consumer, queueName, null);
        }
        while (messageCounter.get() < totalMessages) {
            synchronized (MultiConsumer.lock) {
                MultiConsumer.lock.wait(100);
            }
        }
        assertEquals(totalMessages, messageCounter.get());
    }

    private class BadConsumer extends SimpleConsumer {

        @Override
        public void onMessage(Object message) throws Exception {
            super.onMessage(message);
            throw new IllegalStateException("just throwing this to see the message move to the error queue");
        }
    }

}
