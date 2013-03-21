package org.cobbzilla.util.mq;

import net.rubyeye.xmemcached.exception.MemcachedException;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public interface MqClient {

    public void init(Properties properties) throws IOException;

    public MqProducer getProducer(String queueName);

    public void registerConsumer(MqConsumer callback, String queueName, String errorQueueName);

    public void deleteQueue (String queueName) throws InterruptedException, MemcachedException, TimeoutException;

    /**
     * Stops all registered consumers
     */
    public void shutdown() throws IOException;
}
