package org.cobbzilla.util.mq.kestrel;

import org.cobbzilla.util.mq.MqClient;
import org.cobbzilla.util.mq.MqConsumer;
import org.cobbzilla.util.mq.MqProducer;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.KestrelCommandFactory;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class KestrelClient implements MqClient {

    public static final String KPARAM_OPEN = "/open";
    public static final String KPARAM_CLOSE = "/close";
    public static final String KPARAM_ABORT = "/abort";
    private static final Logger LOG = LoggerFactory.getLogger(KestrelClient.class);

    public static final String PROP_KESTREL_HOSTS = "kestrelHosts";
    public static final String PROP_RECONNECT_INTERVAL_IN_MINUTES = "kestrelReconnectIntervalInMinutes";
    public static final String PROP_KESTREL_CONNECTIONS = "kestrelConnectionPoolSize";

    private Properties initProperties;

    protected volatile MemcachedClient client;

    private KestrelConsumerListener listener = null;
    private volatile Thread listenerThread = null;

    private long reconnectIntervalMillis = 5 * 60 * 1000; // 5 minutes

    /** for debugging */
    public void setReconnectIntervalMillis(long reconnectIntervalMillis) {
        this.reconnectIntervalMillis = reconnectIntervalMillis;
    }

    private volatile long lastConnect;

    @Override
    public synchronized void init(Properties properties) throws IOException {

        LOG.debug("init: KestrelClient initializing...");
        this.initProperties = properties;

        int connectionPoolSize = 1;
        try {
            connectionPoolSize = Integer.parseInt(properties.getProperty(PROP_KESTREL_CONNECTIONS));
        } catch (Exception e) {
            LOG.warn(PROP_KESTREL_CONNECTIONS+" undefined, using default of "+connectionPoolSize);
        }

        final List<InetSocketAddress> memcachedHosts = getMemcachedHosts(properties.getProperty(PROP_KESTREL_HOSTS));

        final String hosts = properties.getProperty(PROP_KESTREL_HOSTS);
        final MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(hosts));
        builder.setCommandFactory(new KestrelCommandFactory());
        builder.setConnectionPoolSize(connectionPoolSize);
        client = builder.build();
        client.setPrimitiveAsString(true);

        final String reconnectIntervalString = properties.getProperty(PROP_RECONNECT_INTERVAL_IN_MINUTES);
        if (reconnectIntervalString != null) {
            reconnectIntervalMillis = 60 * 1000 * Long.parseLong(reconnectIntervalString);
        }
        lastConnect = System.currentTimeMillis();
        LOG.info("init: KestrelClient fully initialized with hosts="+memcachedHosts+", reconnecting every "+(reconnectIntervalMillis/1000/60)+" minutes");
    }

    private List<InetSocketAddress> getMemcachedHosts(String value) {
        List<InetSocketAddress> addresses = new ArrayList<>();
        String[] hostPortPairs = value.split(" ");
        for (String pair : hostPortPairs) {
            final int colonPos = pair.indexOf(":");
            final String host = pair.substring(0, colonPos);
            final int port = Integer.parseInt(pair.substring(colonPos+1));
            addresses.add(new InetSocketAddress(host, port));
        }
        return addresses;
    }

    @Override
    public MqProducer getProducer(String queueName) {
        return new KestrelProducer(this, queueName);
    }

    private static AtomicInteger threadCount = new AtomicInteger(0);
    @Override
    public synchronized void registerConsumer(MqConsumer callback, String queueName, String errorQueueName) {
        if (listenerThread != null) {
            throw new IllegalStateException("No more than one listener per client");
        }
        listener = new KestrelConsumerListener(this, callback, queueName, errorQueueName);
        listenerThread = new Thread(listener);
        listenerThread.setDaemon(true);
        listenerThread.setName("kestrel-consumer-" + threadCount.getAndIncrement());
        listenerThread.start();
    }

    @Override
    public void flushAllQueues() throws InterruptedException, MemcachedException, TimeoutException {
        client.flushAll();
    }

    @Override
    public void deleteQueue(String queueName) throws InterruptedException, TimeoutException {
        try {
            client.delete(queueName);
        } catch (MemcachedException e) {
            // noop - bug in XMemcached considers "DELETED" response invalid, but it's actually correct.
        }
    }

    @Override
    public void shutdown() throws IOException {

        // Fair warning...
        LOG.debug("shutdown: telling listener threads to stop...");
        if (listenerThread != null) {
            listener.stop();
            listenerThread.interrupt();
        }

        // finally, stop the client
        LOG.debug("shutdown: trying to shutdown the memcache client");
        shutdownMemcacheClient();

        LOG.debug("shutdown: client successfully and fully shutdown");
    }

    private synchronized void shutdownMemcacheClient() throws IOException {
        if (client != null) {
            LOG.debug("shutdownMemcacheClient: trying to shutdown the memcache client");
            client.shutdown();
            LOG.debug("shutdownMemcacheClient: memcache client stopped");
        } else {
            LOG.warn("shutdownMemcacheClient: memcache client already stopped");
        }
    }

    public synchronized Object get (String queue, String options, long timeout) throws InterruptedException, TimeoutException, MemcachedException {
        checkReconnect();
        return get_internal(queue, options, timeout);
    }

    private synchronized Object get_internal(String queue, String options, long timeout) throws TimeoutException, InterruptedException, MemcachedException {
        return client.get(queue + options + "/t=" + timeout, timeout + 100);
    }

    public synchronized void set (String queueName, Object message) throws InterruptedException, TimeoutException, MemcachedException {
        client.set(queueName, 0, message);
    }

    public synchronized void ack (String queueName, long timeout) {
        internal_ack(queueName, KPARAM_CLOSE, timeout);
    }

    public synchronized void abort (String queueName, long timeout) {
        internal_ack(queueName, KPARAM_ABORT, timeout);
    }

    private void internal_ack (String queueName, String ackType, long timeout) {
        long now = System.currentTimeMillis();
        int numTries = 0;
        int maxTries = 10;
        long backoff = 250;
        while (numTries < maxTries) {
            try {
                get_internal(queueName, ackType, timeout);
                LOG.debug("internal_ack(" + now + "," + ackType + ","+numTries+"): ack succeeded");
                return;

            } catch (Exception e) {
                String message2 = "internal_ack("+now+","+ackType+","+numTries+"): Couldn't ack an existing open read on queue "+queueName+": "+e;
                LOG.error(message2, e);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e1) {
                    final String msg = "Interrupted while trying to ack, bailing out";
                    LOG.warn(msg);
                    throw new IllegalStateException(msg, e1);
                }
                numTries++;
                backoff *= 2;
            }
        }
    }

    private void checkReconnect() {
        final long now = System.currentTimeMillis();
        if (isTimeToReconnect(now)) {
            lastConnect = now;
            LOG.info("checkReconnect: time to reconnect, shutting down and reinitializing...");
            try {
                synchronized (this) {
                    shutdownMemcacheClient();
                    init(initProperties);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reconnecting: "+e);
            }
        }
    }

    private boolean isTimeToReconnect(long now) {
        return now - lastConnect > reconnectIntervalMillis;
    }

}
