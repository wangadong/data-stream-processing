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

import net.sf.json.JSONObject;

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
public class DemoConsumer extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;
	private static int i = 0;
	private static long pTime;
	private static long totalDelay = 0;
	private static long count = 0;
	private static long currentDelay = 0;
	private static long currentCount = 0;

	public DemoConsumer(String topic) {
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
			Message message = it.next();

			Map<String, Object> jsonMap = JSONObject
					.fromObject(getMessage(message));
			// it's a fact type alarm or an equipment alarm
			if (jsonMap.containsKey("FactTypeAlarm")) {
				System.out.println(jsonMap.values());
			} else {
				long delay = System.currentTimeMillis()
						- (Long) Long.parseLong((String) jsonMap
								.get("acquisitionTime"));
				totalDelay += delay;
				count++;
				currentDelay += delay;
				currentCount++;
				// TODO Here is just an alarm with all information about this
				// alarm, but not alarm detail
			}
			if (count % 10000 == 0 && count != 0) {
				System.out.println("Average delay time: " + totalDelay / count
						+ "ms" + ",alarm count: " + count
						+ ";Current delay time: " + currentDelay / currentCount
						+ "ms");
				currentDelay = currentCount = 0;
			}
		}
	}

	public static String getMessage(Message message) {
		ByteBuffer buffer = message.payload();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}

	public static void main(String[] args) {
		DemoConsumer consumerThread = new DemoConsumer("alarm");
		consumerThread.start();
	}
}
