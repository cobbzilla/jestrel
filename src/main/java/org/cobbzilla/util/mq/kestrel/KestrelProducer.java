package org.cobbzilla.util.mq.kestrel;

import org.cobbzilla.util.mq.MqProducer;
import net.rubyeye.xmemcached.exception.MemcachedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class KestrelProducer implements MqProducer {

    private static final Logger LOG = LoggerFactory.getLogger(KestrelProducer.class);

    private KestrelClient client;
    private String queueName;

    public KestrelProducer(KestrelClient client, String queueName) {
        this.client = client;
        this.queueName = queueName;
    }

    @Override
    public void send(Object message) throws IOException, InterruptedException {
        boolean sent = false;
        int i = 0;
        while (!sent) {
            i++;
            final String msgString = message.toString();
            final int len = msgString.length();
            final String msg = len > 100 ? msgString.substring(0, 100)+"..." : msgString;
            try {
                LOG.info("send: sending to " + queueName + " (try #" + i + "): " + msg);
                client.set(queueName, message);
                LOG.info("send: send succeeded to "+queueName+" (on try #"+i+")");
                sent = true;

            } catch (InterruptedException e) {
                LOG.warn("send: interrupted while trying to send message ("+ msg +") to queue "+queueName);
                throw e;

            } catch (TimeoutException e) {
                LOG.warn("send: timed out sending message ("+ msg +") to queue "+queueName+", retrying");
                Thread.yield();
                Thread.sleep(50);

            } catch (MemcachedException e) {
                LOG.warn("send: error sending message ("+ msg +") to queue "+queueName+", retrying: "+e, e);
                Thread.yield();
                Thread.sleep(50);
            }
        }
    }

}
