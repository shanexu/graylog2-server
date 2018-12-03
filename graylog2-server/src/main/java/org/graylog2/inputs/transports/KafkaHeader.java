package org.graylog2.inputs.transports;

import org.apache.kafka.common.header.Header;

public class KafkaHeader implements Header {
    private String key;
    private byte[] value;

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public byte[] value() {
        return this.value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }
}
