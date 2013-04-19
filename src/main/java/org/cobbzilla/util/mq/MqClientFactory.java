package org.cobbzilla.util.mq;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
@Slf4j
@NoArgsConstructor @AllArgsConstructor
public class MqClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MqClientFactory.class);

    @Getter @Setter private String mqClassName;
    @Getter @Setter private Properties properties;

    private final List<MqClient> clients = new ArrayList<>();

    public MqClient createClient () {
        final MqClient client = createClient(mqClassName, properties);
        clients.add(client);
        return client;
    }

    public void shutdown () {
        for (MqClient client : clients) {
            try {
                client.shutdown();
            } catch (Exception e) {
                log.warn("Error shutting down client ("+client+"): "+e);
            }
        }
    }

    public MqClient createClient (String mqClassName, Properties properties) {
        Class<? extends MqClient> mqClass;
        try {
            mqClass = (Class<? extends MqClient>) Class.forName(mqClassName);
        } catch (Exception e) {
            throw new IllegalArgumentException("Couldn't load client class: "+mqClassName);
        }
        return createClient(mqClass, properties);
    }

    public MqClient createClient (Class<? extends MqClient> mqClass, Properties properties) {
        MqClient client;
        try {
            client = mqClass.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't create client ("+mqClass.getCanonicalName()+"): "+e, e);
        }
        try {
            client.init(properties);
        } catch (Exception e) {
            throw new IllegalArgumentException("Couldn't initialize client ("+mqClass.getCanonicalName()+"): "+e, e);
        }
        return client;
    }

}
