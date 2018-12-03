package org.graylog2.inputs.raw.kafka;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.inputs.codecs.HomerPureCodec;
import org.graylog2.inputs.transports.KafkaTransport;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.Codec;
import org.graylog2.plugin.inputs.transports.Transport;

import javax.inject.Inject;

public class HomerPureKafkaInput extends MessageInput {
    private static final String NAME = "Homer Pure Kafka";

    @AssistedInject
    public HomerPureKafkaInput(@Assisted Configuration configuration,
                               MetricRegistry metricRegistry,
                               KafkaTransport.Factory transport,
                               HomerPureCodec.Factory codec,
                               LocalMetricRegistry localRegistry,
                               Config config,
                               Descriptor descriptor, ServerStatus serverStatus) {
        this(metricRegistry,
                configuration,
                transport.create(configuration),
                codec.create(configuration),
                localRegistry,
                config,
                descriptor, serverStatus);
    }

    protected HomerPureKafkaInput(MetricRegistry metricRegistry,
                                  Configuration configuration,
                                  KafkaTransport transport,
                                  HomerPureCodec codec,
                                  LocalMetricRegistry localRegistry,
                                  MessageInput.Config config,
                                  MessageInput.Descriptor descriptor, ServerStatus serverStatus) {
        super(metricRegistry, configuration, transport, localRegistry, codec, config, descriptor, serverStatus);
    }

    @FactoryClass
    public interface Factory extends MessageInput.Factory<HomerPureKafkaInput> {
        @Override
        HomerPureKafkaInput create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "");
        }
    }

    @ConfigClass
    public static class Config extends MessageInput.Config {
        @Inject
        public Config(KafkaTransport.Factory transport, HomerPureCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}
