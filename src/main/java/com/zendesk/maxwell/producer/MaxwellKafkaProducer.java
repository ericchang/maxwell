package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.producer.partitioners.MaxwellKafkaPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Properties;

class KafkaCallback implements Callback {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKafkaProducer.class);
	private final MaxwellContext context;
	private final RowMap rowMap;
	private final byte[] value;
	private final String key;

	public KafkaCallback(RowMap r, MaxwellContext c, String key, byte[] value) {
		this.context = c;
		this.rowMap= r;
		this.key = key;
		this.value = value;
	}

	@Override
	public void onCompletion(RecordMetadata md, Exception e) {
		if ( e != null ) {
			e.printStackTrace();
		} else {
			try {
				if ( LOGGER.isDebugEnabled()) {
					LOGGER.debug("->  key:" + key + ", partition:" +md.partition() + ", offset:" + md.offset());
					LOGGER.debug("   " + new String(this.value, Charset.forName("UTF-8")));
					LOGGER.debug("   " + rowMap.getPosition());
					LOGGER.debug("");
				}
				if ( rowMap.isTXCommit() ) {
					context.setPosition(rowMap.getPosition());
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
}

public class MaxwellKafkaProducer extends AbstractProducer {
	static final Object KAFKA_DEFAULTS[] = {
		"compression.type", "gzip",
		"metadata.fetch.timeout.ms", 5000
	};
	private final KafkaProducer<String, byte[]> kafka;
	private String topic;
	private final int numPartitions;
	private final MaxwellKafkaPartitioner partitioner;

	public MaxwellKafkaProducer(MaxwellContext context, Properties kafkaProperties, String kafkaTopic, Format format) {
		super(context, EnumSet.of(Format.JSON, Format.AVRO_JSON), format);

		this.topic = kafkaTopic;
		if ( this.topic == null ) {
			this.topic = "maxwell";
		}

		this.setDefaults(kafkaProperties);
		this.kafka = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new ByteArraySerializer());
		this.numPartitions = kafka.partitionsFor(topic).size(); //returns 1 for new topics

		String hash = context.getConfig().kafkaPartitionHash;
		String partitionKey = context.getConfig().kafkaPartitionKey;
		this.partitioner = new MaxwellKafkaPartitioner(hash, partitionKey);
	}

	@Override
	public void push(RowMap r) throws Exception {
		String key = r.pkToJson();
		byte[] rowSerialized = serialize(r, topic);
		ProducerRecord<String, byte[]> record =
				new ProducerRecord<>(topic, this.partitioner.kafkaPartition(r, this.numPartitions), r.pkToJson(), rowSerialized);

		kafka.send(record, new KafkaCallback(r, this.context, key, rowSerialized));
	}

	private void setDefaults(Properties p) {
		for(int i=0 ; i < KAFKA_DEFAULTS.length; i += 2) {
			String key = (String) KAFKA_DEFAULTS[i];
			Object val = KAFKA_DEFAULTS[i + 1];

			if ( !p.containsKey(key)) {
				p.put(key, val);
			}
		}
	}
}
