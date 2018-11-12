/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.inputs.transports;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import kafka0_9_0_1.consumer.Consumer;
import kafka0_9_0_1.consumer.ConsumerConfig;
import kafka0_9_0_1.consumer.ConsumerIterator;
import kafka0_9_0_1.consumer.ConsumerTimeoutException;
import kafka0_9_0_1.consumer.KafkaStream;
import kafka0_9_0_1.consumer.TopicFilter;
import kafka0_9_0_1.consumer.Whitelist;
import kafka0_9_0_1.javaapi.consumer.ConsumerConnector;
import kafka0_9_0_1.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.DropdownField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.ThrottleableTransport;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.graylog2.plugin.lifecycles.Lifecycle;
import org.graylog2.plugin.system.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static com.codahale.metrics.MetricRegistry.name;

public class KafkaTransport extends ThrottleableTransport {
    public static final String GROUP_ID = "graylog2";
    public static final String CK_FETCH_MIN_BYTES = "fetch_min_bytes";
    public static final String CK_FETCH_WAIT_MAX = "fetch_wait_max";
    public static final String CK_ZOOKEEPER = "zookeeper";
    public static final String CK_BOOTSTRAP = "bootstrap";
    public static final String CK_VERSION = "kafka_version";
    public static final String CK_TOPIC_FILTER = "topic_filter";
    public static final String CK_THREADS = "threads";
    public static final String CK_OFFSET_RESET = "offset_reset";

    // See https://kafka.apache.org/090/documentation.html for available values for "auto.offset.reset".
    private static final Map<String, String> OFFSET_RESET_VALUES = ImmutableMap.of(
            "largest", "Automatically reset the offset to the largest offset",
            "smallest", "Automatically reset the offset to the smallest offset"
    );

    private static final String DEFAULT_OFFSET_RESET = "largest";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransport.class);

    private final Configuration configuration;
    private final MetricRegistry localRegistry;
    private final NodeId nodeId;
    private final EventBus serverEventBus;
    private final ServerStatus serverStatus;
    private final ScheduledExecutorService scheduler;
    private final MetricRegistry metricRegistry;
    private final AtomicLong totalBytesRead = new AtomicLong(0);
    private final AtomicLong lastSecBytesRead = new AtomicLong(0);
    private final AtomicLong lastSecBytesReadTmp = new AtomicLong(0);

    private volatile boolean stopped = false;
    private volatile boolean paused = true;
    private volatile CountDownLatch pausedLatch = new CountDownLatch(1);

    private CountDownLatch stopLatch;
    private ConsumerConnector cc;
    private kafka0_8_2_2.javaapi.consumer.ConsumerConnector cc0_8_2_2;

    @AssistedInject
    public KafkaTransport(@Assisted Configuration configuration,
                          LocalMetricRegistry localRegistry,
                          NodeId nodeId,
                          EventBus serverEventBus,
                          ServerStatus serverStatus,
                          @Named("daemonScheduler") ScheduledExecutorService scheduler) {
        super(serverEventBus, configuration);
        this.configuration = configuration;
        this.localRegistry = localRegistry;
        this.nodeId = nodeId;
        this.serverEventBus = serverEventBus;
        this.serverStatus = serverStatus;
        this.scheduler = scheduler;
        this.metricRegistry = localRegistry;

        localRegistry.register("read_bytes_1sec", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastSecBytesRead.get();
            }
        });
        localRegistry.register("written_bytes_1sec", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return 0L;
            }
        });
        localRegistry.register("read_bytes_total", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalBytesRead.get();
            }
        });
        localRegistry.register("written_bytes_total", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return 0L;
            }
        });
    }

    @Subscribe
    public void lifecycleStateChange(Lifecycle lifecycle) {
        LOG.debug("Lifecycle changed to {}", lifecycle);
        switch (lifecycle) {
            case PAUSED:
            case FAILED:
            case HALTING:
                pausedLatch = new CountDownLatch(1);
                paused = true;
                break;
            default:
                paused = false;
                pausedLatch.countDown();
                break;
        }
    }

    @Override
    public void setMessageAggregator(CodecAggregator ignored) {
    }

    @Override
    public void doLaunch(final MessageInput input) throws MisfireException {
        serverStatus.awaitRunning(new Runnable() {
            @Override
            public void run() {
                lifecycleStateChange(Lifecycle.RUNNING);
            }
        });

        // listen for lifecycle changes
        serverEventBus.register(this);

        final Properties props = new Properties();

        props.put("group.id", GROUP_ID);
        props.put("enable.auto.commit", "true");
        props.put("client.id", "gl2-" + nodeId + "-" + input.getId());

        props.put("fetch.min.bytes", String.valueOf(configuration.getInt(CK_FETCH_MIN_BYTES)));
        props.put("fetch.wait.max.ms", String.valueOf(configuration.getInt(CK_FETCH_WAIT_MAX)));
        props.put("zookeeper.connect", configuration.getString(CK_ZOOKEEPER));
        String bootstrapServers = configuration.getString(CK_BOOTSTRAP);
        if (bootstrapServers != null && bootstrapServers.length() != 0) {
            props.put("bootstrap.servers", bootstrapServers);
        }
        props.put("auto.offset.reset", configuration.getString(CK_OFFSET_RESET, DEFAULT_OFFSET_RESET));
        // Default auto commit interval is 60 seconds. Reduce to 1 second to minimize message duplication
        // if something breaks.
        props.put("auto.commit.interval.ms", "1000");
        // Set a consumer timeout to avoid blocking on the consumer iterator.
        props.put("consumer.timeout.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        final int numThreads = configuration.getInt(CK_THREADS);
        String kafkaVersion = configuration.getString(CK_VERSION);

        if (kafkaVersion == null) {
            kafkaVersion = "0.8";
        }

        if (kafkaVersion.startsWith("0.8")) {
            LOG.warn("kafka 0.8.2.2");
            final kafka0_8_2_2.consumer.ConsumerConfig consumerConfig0_8_2_2 = new kafka0_8_2_2.consumer.ConsumerConfig(props);
            cc0_8_2_2 = kafka0_8_2_2.consumer.Consumer.createJavaConsumerConnector(consumerConfig0_8_2_2);
            final kafka0_8_2_2.consumer.TopicFilter filter = new kafka0_8_2_2.consumer.Whitelist(configuration.getString(CK_TOPIC_FILTER));
            final List<kafka0_8_2_2.consumer.KafkaStream<byte[], byte[]>> streams = cc0_8_2_2.createMessageStreamsByFilter(filter, numThreads);

            final ExecutorService executor = executorService(numThreads);

            // this is being used during shutdown to first stop all submitted jobs before committing the offsets back to zookeeper
            // and then shutting down the connection.
            // this is to avoid yanking away the connection from the consumer runnables
            stopLatch = new CountDownLatch(streams.size());

            for (final kafka0_8_2_2.consumer.KafkaStream<byte[], byte[]> stream : streams) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        final kafka0_8_2_2.consumer.ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
                        boolean retry;

                        do {
                            retry = false;

                            try {
                                // we have to use hasNext() here instead foreach, because next() marks the message as processed immediately
                                // noinspection WhileLoopReplaceableByForEach
                                while (consumerIterator.hasNext()) {
                                    if (paused) {
                                        // we try not to spin here, so we wait until the lifecycle goes back to running.
                                        LOG.debug(
                                            "Message processing is paused, blocking until message processing is turned back on.");
                                        Uninterruptibles.awaitUninterruptibly(pausedLatch);
                                    }
                                    // check for being stopped before actually getting the message, otherwise we could end up losing that message
                                    if (stopped) {
                                        break;
                                    }
                                    if (isThrottled()) {
                                        blockUntilUnthrottled();
                                    }

                                    // process the message, this will immediately mark the message as having been processed. this gets tricky
                                    // if we get an exception about processing it down below.
                                    final kafka0_8_2_2.message.MessageAndMetadata<byte[], byte[]> message = consumerIterator.next();

                                    final byte[] bytes = message.message();

                                    // it is possible that the message is null
                                    if (bytes == null) {
                                        continue;
                                    }

                                    totalBytesRead.addAndGet(bytes.length);
                                    lastSecBytesReadTmp.addAndGet(bytes.length);

                                    final RawMessage rawMessage = new RawMessage(bytes);

                                    // TODO implement throttling
                                    input.processRawMessage(rawMessage);
                                }
                            } catch (kafka0_8_2_2.consumer.ConsumerTimeoutException e) {
                                // Happens when there is nothing to consume, retry to check again.
                                retry = true;
                            } catch (Exception e) {
                                LOG.error("Kafka consumer error, stopping consumer thread.", e);
                            }
                        } while (retry && !stopped);
                        // explicitly commit our offsets when stopping.
                        // this might trigger a couple of times, but it won't hurt
                        cc0_8_2_2.commitOffsets();
                        stopLatch.countDown();
                    }
                });
            }
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    lastSecBytesRead.set(lastSecBytesReadTmp.getAndSet(0));
                }
            }, 1, 1, TimeUnit.SECONDS);
        } else if (bootstrapServers == null || bootstrapServers.length() == 0) {
            LOG.warn("kafka 0.9.0.1");
            final ConsumerConfig consumerConfig = new ConsumerConfig(props);
            cc = Consumer.createJavaConsumerConnector(consumerConfig);
            final TopicFilter filter = new Whitelist(configuration.getString(CK_TOPIC_FILTER));
            final List<KafkaStream<byte[], byte[]>> streams = cc.createMessageStreamsByFilter(filter, numThreads);

            final ExecutorService executor = executorService(numThreads);

            // this is being used during shutdown to first stop all submitted jobs before committing the offsets back to zookeeper
            // and then shutting down the connection.
            // this is to avoid yanking away the connection from the consumer runnables
            stopLatch = new CountDownLatch(streams.size());

            for (final KafkaStream<byte[], byte[]> stream : streams) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        final ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
                        boolean retry;

                        do {
                            retry = false;

                            try {
                                // we have to use hasNext() here instead foreach, because next() marks the message as processed immediately
                                // noinspection WhileLoopReplaceableByForEach
                                while (consumerIterator.hasNext()) {
                                    if (paused) {
                                        // we try not to spin here, so we wait until the lifecycle goes back to running.
                                        LOG.debug(
                                            "Message processing is paused, blocking until message processing is turned back on.");
                                        Uninterruptibles.awaitUninterruptibly(pausedLatch);
                                    }
                                    // check for being stopped before actually getting the message, otherwise we could end up losing that message
                                    if (stopped) {
                                        break;
                                    }
                                    if (isThrottled()) {
                                        blockUntilUnthrottled();
                                    }

                                    // process the message, this will immediately mark the message as having been processed. this gets tricky
                                    // if we get an exception about processing it down below.
                                    final MessageAndMetadata<byte[], byte[]> message = consumerIterator.next();

                                    final byte[] bytes = message.message();

                                    // it is possible that the message is null
                                    if (bytes == null) {
                                        continue;
                                    }

                                    totalBytesRead.addAndGet(bytes.length);
                                    lastSecBytesReadTmp.addAndGet(bytes.length);

                                    final RawMessage rawMessage = new RawMessage(bytes);

                                    // TODO implement throttling
                                    input.processRawMessage(rawMessage);
                                }
                            } catch (ConsumerTimeoutException e) {
                                // Happens when there is nothing to consume, retry to check again.
                                retry = true;
                            } catch (Exception e) {
                                LOG.error("Kafka consumer error, stopping consumer thread.", e);
                            }
                        } while (retry && !stopped);
                        // explicitly commit our offsets when stopping.
                        // this might trigger a couple of times, but it won't hurt
                        cc.commitOffsets();
                        stopLatch.countDown();
                    }
                });
            }
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    lastSecBytesRead.set(lastSecBytesReadTmp.getAndSet(0));
                }
            }, 1, 1, TimeUnit.SECONDS);
        } else {
            String autoOffsetReset = props.getProperty("auto.offset.reset", DEFAULT_OFFSET_RESET);
            if (autoOffsetReset.equals("largest")) {
                autoOffsetReset = "latest";
            } else if (autoOffsetReset.equals("smallest")) {
                autoOffsetReset = "earliest";
            } else {
                autoOffsetReset = "latest";
            }
            props.setProperty("auto.offset.reset", autoOffsetReset);
            LOG.warn("bootstrap.servers={}",props.get("bootstrap.servers"));
            LOG.warn("kafka 0.11.0.2");
            stopLatch = new CountDownLatch(numThreads);
            final ExecutorService executor = executorService(numThreads);
            for (int i = 0; i < numThreads; i++) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
                        Pattern pattern = Pattern.compile(Objects.requireNonNull(configuration.getString(CK_TOPIC_FILTER)));
                        consumer.subscribe(pattern, new ConsumerRebalanceListener() {
                            @Override
                            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                                LOG.info("partitions revoked {}", partitions);
                            }

                            @Override
                            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                                LOG.info("partitions assigned {}", partitions);
                            }
                        });
                        do {
                            if (paused) {
                                // we try not to spin here, so we wait until the lifecycle goes back to running.
                                LOG.debug(
                                    "Message processing is paused, blocking until message processing is turned back on.");
                                Uninterruptibles.awaitUninterruptibly(pausedLatch);
                            }
                            try {
                                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(1000);
                                if (consumerRecords == null || consumerRecords.isEmpty()) {
                                    continue;
                                }
                                for (ConsumerRecord<byte[], byte[]> message : consumerRecords) {
                                    if (paused) {
                                        // we try not to spin here, so we wait until the lifecycle goes back to running.
                                        LOG.debug(
                                            "Message processing is paused, blocking until message processing is turned back on.");
                                        Uninterruptibles.awaitUninterruptibly(pausedLatch);
                                    }
                                    final byte[] bytes = message.value();
                                    if (bytes == null) {
                                        continue;
                                    }
                                    totalBytesRead.addAndGet(bytes.length);
                                    lastSecBytesReadTmp.addAndGet(bytes.length);

                                    final RawMessage rawMessage = new RawMessage(bytes);

                                    // TODO implement throttling
                                    input.processRawMessage(rawMessage);
                                }
                            } catch (Exception e) {
                                LOG.error("Kafka consumer error, stopping consumer thread.", e);
                            }
                        } while (!stopped);
                        LOG.warn("stop consumer");
                        consumer.commitSync();
                        consumer.close();
                        stopLatch.countDown();
                    }
                });
            }
        }
    }

    private ExecutorService executorService(int numThreads) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("kafka-transport-%d").build();
        return new InstrumentedExecutorService(
                Executors.newFixedThreadPool(numThreads, threadFactory),
                metricRegistry,
                name(this.getClass(), "executor-service"));
    }

    @Override
    public void doStop() {
        stopped = true;

        serverEventBus.unregister(this);

        if (stopLatch != null) {
            try {
                // unpause the processors if they are blocked. this will cause them to see that we are stopping, even if they were paused.
                if (pausedLatch != null && pausedLatch.getCount() > 0) {
                    pausedLatch.countDown();
                }
                final boolean allStoppedOrderly = stopLatch.await(5, TimeUnit.SECONDS);
                stopLatch = null;
                if (!allStoppedOrderly) {
                    // timed out
                    LOG.info(
                            "Stopping Kafka input timed out (waited 5 seconds for consumer threads to stop). Forcefully closing connection now. " +
                                    "This is usually harmless when stopping the input.");
                }
            } catch (InterruptedException e) {
                LOG.debug("Interrupted while waiting to stop input.");
            }
        }
        if (cc0_8_2_2 != null) {
            cc0_8_2_2.shutdown();
            cc0_8_2_2 = null;
        }
        if (cc != null) {
            cc.shutdown();
            cc = null;
        }
    }

    @Override
    public MetricSet getMetricSet() {
        return localRegistry;
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<KafkaTransport> {
        @Override
        KafkaTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends ThrottleableTransport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest cr = super.getRequestedConfiguration();

            cr.addField(new TextField(
                    CK_ZOOKEEPER,
                    "ZooKeeper address",
                    "127.0.0.1:2181",
                    "Host and port of the ZooKeeper that is managing your Kafka cluster.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new TextField(
                CK_BOOTSTRAP,
                "Bootstrap servers",
                null,
                "Bootstrap servers",
                ConfigurationField.Optional.OPTIONAL));

            cr.addField(new TextField(
                CK_VERSION,
                "Kafak version",
                "0.8.2",
                "Kafka version",
                ConfigurationField.Optional.OPTIONAL));

            cr.addField(new TextField(
                    CK_TOPIC_FILTER,
                    "Topic filter regex",
                    "^your-topic$",
                    "Every topic that matches this regular expression will be consumed.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_FETCH_MIN_BYTES,
                    "Fetch minimum bytes",
                    5,
                    "Wait for a message batch to reach at least this size or the configured maximum wait time before fetching.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_FETCH_WAIT_MAX,
                    "Fetch maximum wait time (ms)",
                    100,
                    "Wait for this time or the configured minimum size of a message batch before fetching.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_THREADS,
                    "Processor threads",
                    2,
                    "Number of processor threads to spawn. Use one thread per Kafka topic partition.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new DropdownField(
                    CK_OFFSET_RESET,
                    "Auto offset reset",
                    DEFAULT_OFFSET_RESET,
                    OFFSET_RESET_VALUES,
                    "What to do when there is no initial offset in ZooKeeper or if an offset is out of range",
                    ConfigurationField.Optional.OPTIONAL));

            return cr;
        }
    }
}
