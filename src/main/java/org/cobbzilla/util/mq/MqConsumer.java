package org.cobbzilla.util.mq;

/**
 * (c) Copyright 2013 Jonathan Cobb
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public interface MqConsumer {

    public void onMessage(Object message) throws Exception;

}
