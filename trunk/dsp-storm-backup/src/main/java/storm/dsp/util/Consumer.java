/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.dsp.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import storm.kafka.KafkaProperties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

/**
 * This is a simple kafka consumer for consuming messages from a kafka broker
 * 
 * @author Tony WANG
 * 
 */
public class Consumer extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;
	private static int i = 0;
	private static long pTime;

	public Consumer(String topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zk.connect", DSPPropertyUtil.KAFKA_BROKER_URL + ":2181");
		props.put("groupid", KafkaProperties.groupId);
		props.put("zk.sessiontimeout.ms", "2000");
		props.put("zk.synctime.ms", "200");
		props.put("autocommit.interval.ms", "1000");
		props.put("zk.connectiontimeout.ms", "15000");
		// props.put("queuedchunks.max", "100");
		// props.put("socket.buffersize", Integer.toString(64 * 1024));
		// props.put("fetch.size", Integer.toString(300 * 1024));
		return new ConsumerConfig(props);

	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaMessageStream<Message>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaMessageStream<Message> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<Message> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println(getMessage(it.next()));
			// it.next();
			// i++;
			// if (i % 10000 == 0) {
			// System.out.println(i);
			// long cTime = System.currentTimeMillis();
			// System.out.println(cTime - pTime);
			// pTime = cTime;
			// }
		}
	}

	public static String getMessage(Message message) {
		ByteBuffer buffer = message.payload();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}

	public static void main(String[] args) {
		Consumer consumerThread = new Consumer("alarm1");
		consumerThread.start();
	}
}
