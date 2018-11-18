package org.graylog2.outputs;

import org.graylog2.plugin.Message;
import org.graylog2.plugin.outputs.MessageOutput;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaOutput implements MessageOutput {

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void write(Message message) throws Exception {

    }

    @Override
    public void write(List<Message> messages) throws Exception {

    }

    @Override
    public void stop() {

    }
}
