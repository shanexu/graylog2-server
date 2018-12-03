package org.graylog2.inputs.codecs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.inputs.transports.KafkaHeader;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.annotations.Codec;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.AbstractCodec;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.journal.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Codec(name = "homer-pure", displayName = "Homer Pure")
public class HomerPureCodec extends AbstractCodec {

    private static final Logger logger = LoggerFactory.getLogger(HomerPureCodec.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @AssistedInject
    protected HomerPureCodec(@Assisted Configuration configuration) {
        super(configuration);
    }

    @Nullable
    @Override
    public Message decode(@Nonnull RawMessage raw) {
        Message message = new Message(new String(raw.getPayload(), StandardCharsets.UTF_8), null, raw.getTimestamp());
        byte[] payload2 = raw.getPayload2();
        if (payload2 != null && payload2.length > 0) {
            try {
                KafkaHeader[] kafkaHeaders = objectMapper.readValue(payload2, KafkaHeader[].class);
                for (KafkaHeader kh : kafkaHeaders) {
                    String key = kh.key();
                    byte[] value = kh.value();
                    switch (key) {
                        case "inode":
                            message.addField("inode", Longs.fromByteArray(value));
                            break;
                        case "device":
                            message.addField("device", Longs.fromByteArray(value));
                            break;
                        case "offset":
                            message.addField("offset", Longs.fromByteArray(value));
                            break;
                        case "length":
                            message.addField("length", Longs.fromByteArray(value));
                            break;
                        case "hostname":
                            message.setSource(new String(value, StandardCharsets.UTF_8));
                            break;
                        case "alias":
                            message.addField("alias", new String(value, StandardCharsets.UTF_8));
                            break;
                        case "app":
                            message.addField("app", new String(value, StandardCharsets.UTF_8));
                            break;
                        case "env":
                            message.addField("env", new String(value, StandardCharsets.UTF_8));
                            break;
                        case "cluster":
                            message.addField("cluster", new String(value, StandardCharsets.UTF_8));
                            break;
                    }
                }
            } catch (IOException e) {
                logger.warn("read kafkaHeaders failed with error {}", e);
            }
        }
        return message;
    }

    @Nullable
    @Override
    public CodecAggregator getAggregator() {
        return null;
    }

    @FactoryClass
    public interface Factory extends AbstractCodec.Factory<HomerPureCodec> {
        @Override
        HomerPureCodec create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends AbstractCodec.Descriptor {
        @Inject
        public Descriptor() {
            super(HomerPureCodec.class.getAnnotation(Codec.class).displayName());
        }
    }
}
