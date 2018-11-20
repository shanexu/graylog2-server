package org.graylog2.outputs;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.assistedinject.Assisted;
import org.apache.kafka.clients.producer.*;
import org.graylog2.gelfclient.GelfMessage;
import org.graylog2.gelfclient.GelfMessageBuilder;
import org.graylog2.gelfclient.GelfMessageLevel;
import org.graylog2.gelfclient.transport.GelfTransport;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.graylog2.plugin.streams.Stream;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaOutput implements MessageOutput {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOutput.class);

    public static final String CK_BOOTSTRAP = "bootstrap";
    public static final String CK_VERSION = "kafka_version";
    public static final String CK_TOPIC = "topic";
//    public static final String CK_ACKS = "acks";
//    public static final String CK_RETRIES = "retries";

    private final JsonFactory jsonFactory;
    private final Producer<byte[], byte[]> producer;
    private final String topic;

    @Inject
    public KafkaOutput(@Assisted Configuration configuration) throws MessageOutputConfigurationException {
        jsonFactory = new JsonFactory();
        topic = configuration.getString("topic");

        Properties props = new Properties();
        props.put("bootstrap.servers", configuration.getString(CK_BOOTSTRAP));
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "snappy");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(props);
        this.isRunning.set(true);
    }

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void write(final Message message) throws Exception {
        GelfMessage gelfMessage = toGELFMessage(message);
        byte[] bytes = toJson(gelfMessage);
        producer.send(new ProducerRecord<>(topic, bytes), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOG.warn("send message failed", exception);
                }
            }
        });
    }

    @Override
    public void write(final List<Message> messages) throws Exception {
        for (Message message : messages) {
            write(message);
        }
    }

    protected GelfMessage toGELFMessage(final Message message) {
        final DateTime timestamp;
        final Object fieldTimeStamp = message.getField(Message.FIELD_TIMESTAMP);
        if (fieldTimeStamp instanceof DateTime) {
            timestamp = (DateTime) fieldTimeStamp;
        } else {
            timestamp = Tools.nowUTC();
        }

        final GelfMessageLevel messageLevel = extractLevel(message.getField(Message.FIELD_LEVEL));
        final String fullMessage = (String) message.getField(Message.FIELD_FULL_MESSAGE);
        final String forwarder = GelfOutput.class.getCanonicalName();

        final GelfMessageBuilder builder = new GelfMessageBuilder(message.getMessage(), message.getSource())
                .timestamp(timestamp.getMillis() / 1000.0d)
                .additionalField("_forwarder", forwarder)
                .additionalFields(message.getFields());

        if (messageLevel != null) {
            builder.level(messageLevel);
        }

        if (fullMessage != null) {
            builder.fullMessage(fullMessage);
        }

        return builder.build();
    }

    @Nullable
    private GelfMessageLevel extractLevel(Object rawLevel) {
        GelfMessageLevel level;
        if (rawLevel instanceof Number) {
            final int numericLevel = ((Number) rawLevel).intValue();
            level = extractLevel(numericLevel);
        } else if (rawLevel instanceof String) {
            Integer numericLevel;
            try {
                numericLevel = Integer.parseInt((String) rawLevel);
            } catch (NumberFormatException e) {
                LOG.debug("Invalid message level " + rawLevel, e);
                numericLevel = null;
            }

            if (numericLevel == null) {
                level = null;
            } else {
                level = extractLevel(numericLevel);
            }
        } else {
            LOG.debug("Invalid message level {}", rawLevel);
            level = null;
        }

        return level;
    }

    @Nullable
    private GelfMessageLevel extractLevel(int numericLevel) {
        GelfMessageLevel level;
        try {
            level = GelfMessageLevel.fromNumericLevel(numericLevel);
        } catch (IllegalArgumentException e) {
            LOG.debug("Invalid numeric message level " + numericLevel, e);
            level = null;
        }
        return level;
    }

    private byte[] toJson(final GelfMessage message) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (final JsonGenerator jg = jsonFactory.createGenerator(out, JsonEncoding.UTF8)) {
            jg.writeStartObject();

            jg.writeStringField("version", message.getVersion().toString());
            jg.writeNumberField("timestamp", message.getTimestamp());
            jg.writeStringField("host", message.getHost());
            jg.writeStringField("short_message", message.getMessage());
            if (message.getLevel() != null) {
                jg.writeNumberField("level", message.getLevel().getNumericLevel());
            }

            if(null != message.getFullMessage()) {
                jg.writeStringField("full_message", message.getFullMessage());
            }

            for (Map.Entry<String, Object> field : message.getAdditionalFields().entrySet()) {
                final String realKey = field.getKey().startsWith("_") ? field.getKey() : ("_" + field.getKey());

                if (field.getValue() instanceof Number) {
                    // Let Jackson figure out how to write Number values.
                    jg.writeObjectField(realKey, field.getValue());
                } else if (field.getValue() == null) {
                    jg.writeNullField(realKey);
                } else {
                    jg.writeStringField(realKey, field.getValue().toString());
                }
            }

            jg.writeEndObject();
        }

        return out.toByteArray();
    }

    @Override
    public void stop() {
        LOG.debug("Stopping {}", producer.getClass().getName());
        try {
            producer.close(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Error stopping " + producer.getClass().getName(), e);
        }
        isRunning.set(false);
    }

    public interface Factory extends MessageOutput.Factory<KafkaOutput> {
        @Override
        KafkaOutput create(Stream stream, Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest configurationRequest = new ConfigurationRequest();
            configurationRequest.addField(new TextField(CK_BOOTSTRAP, "bootstrap servers", "", "bootstrap servers", ConfigurationField.Optional.NOT_OPTIONAL));
            configurationRequest.addField(new TextField(CK_VERSION, "version", "", "version", ConfigurationField.Optional.NOT_OPTIONAL));
            configurationRequest.addField(new TextField(CK_TOPIC, "topic", "", "topic", ConfigurationField.Optional.NOT_OPTIONAL));
            return configurationRequest;
        }
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("GELF Kafka Output", false, "", "An output sending GELF over kafka");
        }
    }
}
