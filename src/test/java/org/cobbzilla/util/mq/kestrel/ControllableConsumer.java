package org.cobbzilla.util.mq.kestrel;

import net.rubyeye.xmemcached.exception.MemcachedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class ControllableConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ControllableConsumer.class);

    public static final int TIMEOUT = 1000;

    private static final AtomicInteger threadCounter = new AtomicInteger(1);
    public final Object lock = new Object();

    private KestrelClient client;
    private String queueName;
    private Thread thread = new Thread(this);

    public ControllableConsumer(KestrelClient client, String queueName) {
        this.client = client;
        this.queueName = queueName;
        thread.setName(ControllableConsumer.class.getSimpleName()+"-"+threadCounter.getAndIncrement());
        thread.setDaemon(true);
    }

    private volatile Command mostRecentCommand = null;
    public Command getMostRecentCommand() { return mostRecentCommand; }

    enum Command {
        STOP, OPEN, CLOSE, ABORT
    }

    private final List<Command> commands = new ArrayList<>();
    public void addCommand (Command command) {
        synchronized (commands) {
            commands.add(command);
            commands.notify();
        }
    }

    public void start () { thread.start(); }

    private List<String> messages = new ArrayList<>();
    public String getMostRecentMessage () {
        return messages.isEmpty() ? null : messages.get(0);
    }

    @Override
    public void run() {
        while (true) {

            List<Command> todo = getCommands();

            for (Command command : todo) {
                switch (command) {
                    case STOP:
                        LOG.info("STOP: exiting");
                        return;

                    case OPEN:
                        try {
                            LOG.info("OPEN: reading from kestrel...");
                            Object message = client.get(queueName, KestrelClient.KPARAM_OPEN, TIMEOUT);
                            if (message == null) {
                                LOG.info("OPEN: got a null read, continuing");
                                continue;
                            }
                            LOG.info("OPEN: received from kestrel: "+message);
                            messages.add(0, message.toString());

                        } catch (InterruptedException e) {
                            LOG.info("OPEN: interrupted while reading, exiting");
                            return;

                        } catch (MemcachedException e) {
                            LOG.error("OPEN: error reading from kestrel: " + e, e);
                            return;

                        } catch (TimeoutException e) {
                            LOG.warn("OPEN: timed out reading from kestrel, continuing");
                            continue;
                        }
                        break;

                    case CLOSE:
                        LOG.info("CLOSE: closing open read");
                        client.ack(queueName, TIMEOUT);
                        break;

                    case ABORT:
                        LOG.info("ABORT: aborting open read");
                        client.abort(queueName, TIMEOUT);
                        break;

                    default:
                        throw new IllegalArgumentException("Unrecognized command: "+command);
                }
                mostRecentCommand = command;
            }

            synchronized (lock) {
                lock.notify();
            }
        }
    }

    private List<Command> getCommands() {
        List<Command> todo = null;

        while (todo == null) {
            synchronized (commands) {
                try {
                    commands.wait(100);
                    if (commands.size() == 0) continue;

                } catch (InterruptedException e) {
                    LOG.info("Interrupted while waiting for commands, exiting");
                    return null;
                }
                todo = new ArrayList<>(commands);
                commands.clear();
            }
        }

        return todo;
    }
}
