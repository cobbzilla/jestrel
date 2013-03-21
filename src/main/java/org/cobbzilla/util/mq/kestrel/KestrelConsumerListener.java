package org.cobbzilla.util.mq.kestrel;

import org.cobbzilla.util.mq.MqConsumer;
import net.rubyeye.xmemcached.exception.MemcachedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
class KestrelConsumerListener implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(KestrelConsumerListener.class);

    private static final long READ_TIMEOUT = 500;

    private KestrelClient client;
    private MqConsumer callback;
    private String queueName;
    private String errorQueueName;

    private volatile boolean alive = true;

    public KestrelConsumerListener(KestrelClient client, MqConsumer callback, String queueName, String errorQueueName) {
        this.client = client;
        this.callback = callback;
        this.queueName = queueName;
        this.errorQueueName = errorQueueName;
    }

    public void stop () {
        LOG.info("stop called on listener: "+this);
        alive = false;
    }

    @Override
    public void run() {
        try {
            while (alive) {
                try {
                    LOG.debug("runloop: getting from "+queueName+" with timeout="+READ_TIMEOUT);
                    Object message = client.get(queueName, KestrelClient.KPARAM_OPEN, READ_TIMEOUT);
                    if (message == null) {
                        LOG.debug("runloop: got null from " + queueName + ", sleeping for 100ms and trying again");
                        Thread.sleep(100);
                        continue;
                    }
                    doCallback(message);

                } catch (InterruptedException e) {
                    // noop, just try again
                    LOG.info("runloop: interrupted while reading from kestrel");
                    if (notAlive()) return;

                } catch (TimeoutException e) {
                    // noop, yield and retry
                    if (notAlive()) return;
                    LOG.debug("runloop: timed out, yielding and continuing");
                    Thread.yield();
//                    deepSleep(100);
                    continue;

                } catch (MemcachedException e) {
                    String message = "runloop: Error talking to Kestrel: "+e;
                    LOG.error(message, e);
                    if (notAlive()) return;
                    // sleep for a bit so we don't spin if Kestrel is down
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e1) {
                        if (notAlive()) return;
                    }
                }
            }
        } finally {
            LOG.info("runloop: exiting");
        }
    }

    private boolean notAlive() {
        if (!alive || Thread.currentThread().isInterrupted()) {
            LOG.info("runloop: interrupted and/or not alive, exiting");
            return true;
        }
//        LOG.info("runloop: alive and not interrupted");
        return false;
    }

    private void doCallback(Object message) {
        try {
            // happy case - parse the json, invoke the callback, ack the message
            final String msgString = message.toString();
            final int len = msgString.length();
            final String prefix = len > 100 ? msgString.substring(0, 100) + "..." : msgString;
            LOG.info("doCallback: sending message ("+prefix+") to worker");
            callback.onMessage(message);
            LOG.debug("doCallback: ACKing message ("+prefix+")...");
            client.ack(queueName, 200);
            LOG.debug("doCallback: ACKed message OK ("+prefix+")...");

        } catch (Exception e) {
            // message handler failed, put this message on the error queue if one is configured
            if (errorQueueName != null) {
                try {
                    LOG.error("doCallback: Callback threw an exception, putting message onto errorQueue ("+errorQueueName+"): "+e, e);
                    client.set(errorQueueName, message);
                    client.ack(queueName, 200);
                    LOG.info("doCallback: Message put onto errorQueue and ACKED on regular queue (we should not see it again)");

                } catch (Exception fatal) {
                    String msg = "doCallback: Error putting message onto error queue ("+fatal+"). Aborting read to leave message on original queue";
                    client.abort(queueName, 200);
                    LOG.error(msg, fatal);
                }
            } else {
                LOG.error("doCallback: Callback threw and exception and there is no errorQueue configured. Aborting read to leave message on original queue: "+e, e);
                client.abort(queueName, 200);
            }
        }
    }
}
