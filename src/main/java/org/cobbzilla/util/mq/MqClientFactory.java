package org.cobbzilla.util.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class MqClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MqClientFactory.class);

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
