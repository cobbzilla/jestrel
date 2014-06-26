package org.cobbzilla.util.mq.virtual;

import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.util.mq.MqProducer;

import java.io.IOException;

@Slf4j
public class DevNullMqProducer implements MqProducer {

    @Override public void send(Object thing) throws IOException, InterruptedException {
        log.info("send("+thing+")");
    }

}
