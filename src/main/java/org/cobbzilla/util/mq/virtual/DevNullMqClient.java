package org.cobbzilla.util.mq.virtual;

import lombok.extern.slf4j.Slf4j;
import net.rubyeye.xmemcached.exception.MemcachedException;
import org.cobbzilla.util.mq.MqClient;
import org.cobbzilla.util.mq.MqConsumer;
import org.cobbzilla.util.mq.MqProducer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

@Slf4j
public class DevNullMqClient implements MqClient {

    private final DevNullMqProducer producer = new DevNullMqProducer();

    @Override public void init(Properties properties) throws IOException {}

    @Override public MqProducer getProducer(String queueName) { return producer; }

    @Override
    public void registerConsumer(MqConsumer callback, String queueName, String errorQueueName) {
        log.info("registerConsumer("+callback+", "+queueName+", "+errorQueueName+")");
    }

    @Override
    public void flushAllQueues() throws InterruptedException, MemcachedException, TimeoutException {
        log.info("flushAllQueues()");
    }

    @Override
    public void deleteQueue(String queueName) throws InterruptedException, MemcachedException, TimeoutException {
        log.info("deleteQueue("+queueName+")");
    }

    @Override
    public void shutdown() throws IOException {
        log.info("shutdown()");
    }
}
