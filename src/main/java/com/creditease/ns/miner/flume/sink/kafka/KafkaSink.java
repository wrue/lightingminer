package com.creditease.ns.miner.flume.sink.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.ns.miner.flume.constant.CommonConstants;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

public class KafkaSink extends AbstractSink implements Configurable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
	private final Properties kafkaProps = new Properties();
	private KafkaProducer<String, byte[]> producer;
	private String topic;
	private int batchSize;
	private boolean useAvroEventFormat;
	private String partitionHeader = null;
	private Integer staticPartitionId = null;
	private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer = Optional.absent();
	private Optional<ByteArrayOutputStream> tempOutStream = Optional.absent();
	private BinaryEncoder encoder = null;

	public String getTopic() {
		return this.topic;
	}

	public int getBatchSize() {
		return this.batchSize;
	}

	@Override
	public Sink.Status process() throws EventDeliveryException {
		Sink.Status result = Sink.Status.READY;
		Channel channel = getChannel();
		Transaction transaction = null;
		Event event = null;
		String eventTopic = null;
		String eventKey = null;
		try {
			long processedEvents = 0L;

			transaction = channel.getTransaction();
			transaction.begin();

			for (; processedEvents < this.batchSize; processedEvents += 1L) {
				event = channel.take();
				if (event == null) {
					if (processedEvents == 0L) {
						result = Sink.Status.BACKOFF;
					}
					break;
				}
				Map<String, String> headers = event.getHeaders();

				String ip = headers.get(CommonConstants.EVENT_HEADERS_IP_KEY);
				String filename = headers.get(CommonConstants.EVENT_HEADERS_WHOLEPATH_KEY);
				String logId = headers.get(CommonConstants.EVENT_HEADERS_LOGID_KEY);
				String lineRange = headers.get(CommonConstants.EVENT_HEADERS_LINERANGE_KEY);

				StringBuffer bf = new StringBuffer(logId);
				bf.append(CommonConstants.REGEX_SPILTTER).append(ip).append(CommonConstants.REGEX_SPILTTER).append(filename)
						.append(CommonConstants.REGEX_SPILTTER).append(lineRange);

				eventTopic = headers.get("topic");
				if (eventTopic == null) {
					eventTopic = this.topic;
				}
				eventKey = bf.toString();

				Integer partitionId = null;
				try {
					if (this.staticPartitionId != null) {
						partitionId = this.staticPartitionId;
					}
					if (this.partitionHeader != null) {
						String headerVal = event.getHeaders().get(this.partitionHeader);
						if (headerVal != null) {
							partitionId = Integer.valueOf(Integer.parseInt(headerVal));
						}
					}
					ProducerRecord<String, byte[]> record;
					if (partitionId != null) {
						record = new ProducerRecord<String, byte[]>(eventTopic, partitionId, eventKey, serializeEvent(event,
								this.useAvroEventFormat));
					} else {
						record = new ProducerRecord<String, byte[]>(eventTopic, eventKey, serializeEvent(event,
								this.useAvroEventFormat));
					}
					this.producer.send(record);
				} catch (NumberFormatException ex) {
					throw new EventDeliveryException("Non integer partition id specified", ex);
				} catch (Exception ex) {
					throw new EventDeliveryException("Could not send event", ex);
				}
			}
			this.producer.flush();
			transaction.commit();
		} catch (Exception ex) {
			String errorMsg = "Failed to publish events";
			logger.error("Failed to publish events", ex);
			result = Sink.Status.BACKOFF;
			if (transaction != null) {
				try {
					transaction.rollback();
				} catch (Exception e) {
					logger.error("Transaction rollback failed", e);
					throw Throwables.propagate(e);
				}
			}
			throw new EventDeliveryException(errorMsg, ex);
		} finally {
			if (transaction != null) {
				transaction.close();
			}
		}
		return result;
	}

	@Override
	public synchronized void start() {
		this.producer = new KafkaProducer<String, byte[]>(this.kafkaProps);
		super.start();
	}

	@Override
	public synchronized void stop() {
		this.producer.close();
		super.stop();
	}

	@Override
	public void configure(Context context) {
		translateOldProps(context);

		String topicStr = context.getString("kafka.topic");
		if ((topicStr == null) || (topicStr.isEmpty())) {
			topicStr = "default-flume-topic";
			logger.warn("Topic was not specified. Using {} as the topic.", topicStr);
		} else {
			logger.info("Using the static topic {}. This may be overridden by event headers", topicStr);
		}
		this.topic = topicStr;

		this.batchSize = context.getInteger("flumeBatchSize", Integer.valueOf(100)).intValue();
		if (logger.isDebugEnabled()) {
			logger.debug("Using batch size: {}", Integer.valueOf(this.batchSize));
		}
		this.useAvroEventFormat = context.getBoolean("useFlumeEventFormat", Boolean.valueOf(false)).booleanValue();

		this.partitionHeader = context.getString("partitionIdHeader");
		this.staticPartitionId = context.getInteger("defaultPartitionId");
		if (logger.isDebugEnabled()) {
			logger.debug("useFlumeEventFormat set to: {}", Boolean.valueOf(this.useAvroEventFormat));
		}

		String bootStrapServers = context.getString("kafka.bootstrap.servers");
		if ((bootStrapServers == null) || (bootStrapServers.isEmpty())) {
			throw new ConfigurationException("Bootstrap Servers must be specified");
		}
		setProducerProps(context, bootStrapServers);
		if (logger.isDebugEnabled()) {
			logger.debug("Kafka producer properties: {}", this.kafkaProps);
		}
	}

	private void translateOldProps(Context ctx) {
		if (!ctx.containsKey("kafka.topic")) {
			ctx.put("kafka.topic", ctx.getString("topic"));
			logger.warn("{} is deprecated. Please use the parameter {}", "topic", "kafka.topic");
		}
		if (!ctx.containsKey("kafka.bootstrap.servers")) {
			String brokerList = ctx.getString("brokerList");
			if ((brokerList == null) || (brokerList.isEmpty())) {
				throw new ConfigurationException("Bootstrap Servers must be specified");
			}
			ctx.put("kafka.bootstrap.servers", brokerList);
			logger.warn("{} is deprecated. Please use the parameter {}", "brokerList", "kafka.bootstrap.servers");
		}
		if (!ctx.containsKey("flumeBatchSize")) {
			String oldBatchSize = ctx.getString("batchSize");
			if ((oldBatchSize != null) && (!oldBatchSize.isEmpty())) {
				ctx.put("flumeBatchSize", oldBatchSize);
				logger.warn("{} is deprecated. Please use the parameter {}", "batchSize", "flumeBatchSize");
			}
		}
		if (!ctx.containsKey("kafka.producer.acks")) {
			String requiredKey = ctx.getString("requiredAcks");
			if ((requiredKey != null) && (!requiredKey.isEmpty())) {
				ctx.put("kafka.producer.acks", requiredKey);
				logger.warn("{} is deprecated. Please use the parameter {}", "requiredAcks", "kafka.producer.acks");
			}
		}
		if (ctx.containsKey("key.serializer.class")) {
			logger
					.warn(
							"{} is deprecated. Flume now uses the latest Kafka producer which implements a different interface for serializers. Please use the parameter {}",
							"key.serializer.class", "kafka.producer.key.serializer");
		}
		if (ctx.containsKey("serializer.class")) {
			logger
					.warn(
							"{} is deprecated. Flume now uses the latest Kafka producer which implements a different interface for serializers. Please use the parameter {}",
							"serializer.class", "kafka.producer.value.serializer");
		}
	}

	private void setProducerProps(Context context, String bootStrapServers) {
		this.kafkaProps.put("acks", "1");

		this.kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		this.kafkaProps.putAll(context.getSubProperties("kafka.producer."));
		this.kafkaProps.put("bootstrap.servers", bootStrapServers);
	}

	protected Properties getKafkaProps() {
		return this.kafkaProps;
	}

	private byte[] serializeEvent(Event event, boolean useAvroEventFormat) throws IOException {
		byte[] bytes;
		if (useAvroEventFormat) {
			if (!this.tempOutStream.isPresent()) {
				this.tempOutStream = Optional.of(new ByteArrayOutputStream());
			}
			if (!this.writer.isPresent()) {
				this.writer = Optional.of(new SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class));
			}
			this.tempOutStream.get().reset();
			AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()), ByteBuffer.wrap(event.getBody()));
			this.encoder = EncoderFactory.get().directBinaryEncoder(this.tempOutStream.get(), this.encoder);
			this.writer.get().write(e, this.encoder);
			this.encoder.flush();
			bytes = this.tempOutStream.get().toByteArray();
		} else {
			bytes = event.getBody();
		}
		return bytes;
	}

	private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
		Map<CharSequence, CharSequence> charSeqMap = new HashMap<CharSequence, CharSequence>();
		for (Map.Entry<String, String> entry : stringMap.entrySet()) {
			charSeqMap.put(entry.getKey(), entry.getValue());
		}
		return charSeqMap;
	}
}
