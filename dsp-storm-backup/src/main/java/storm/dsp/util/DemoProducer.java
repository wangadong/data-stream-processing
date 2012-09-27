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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import backtype.storm.tuple.Values;

import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

/**
 * A sample producer to produce messages and send to kafka broker This could be
 * used to simulate FEP system
 * 
 * @author wangadong
 * 
 */
public class DemoProducer {

	final static private DemoProducer _instance = new DemoProducer();
	private static kafka.javaapi.producer.Producer<Integer, String> producer;
	private final Properties props = new Properties();
	static int count;
	int messagePerSecond;
	static int messageToSend;
	private static long precedentTime;

	public DemoProducer() {
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect", DSPPropertyUtil.KAFKA_BROKER_URL + ":2181");
		props.put("zk.connectiontimeout.ms", "20000");
		props.put("producer.type", "async");
		// props.put("buffer.size", "10240000");
		props.put("connect.timeout.ms", "10000");
		// props.put("socket.timeout.ms", "30000");
		// props.put("reconnect.interval", "30000");
		// props.put("max.message.size", "10000000");
		// props.put("zk.read.num.retries", "3");
		// props.put("queue.time", "5000");
		props.put("queue.size", "5000000");
		// props.put("batch.size", "2000");
		producer = new kafka.javaapi.producer.Producer<Integer, String>(
				new ProducerConfig(props));
	}

	public static DemoProducer getInstance() {
		return _instance;
	}

	public static void sendBySingleRequest(String message, String topic) {
		producer.send(new ProducerData<Integer, String>(topic, message));
	}

	public static void main(String[] args) throws InterruptedException {

		count = 0;
		final int messagePerSecond = 3000;
		while (count < 300000) {
			String json;
			if (count % 10 == 0) {
				json = "{\"point_name\":\"dciPOW-HOSTAT\",\"description\":\"35kV\",\"type\":\"DI1\",\"value\":2,\"acquisitionTime\":\""
						+ System.currentTimeMillis() + "\"}";
			} else {
				json = "{\"point_name\":\"dciPOW-HOSTAT\",\"description\":\"35kV\",\"type\":\"DI1\",\"value\":0,\"acquisitionTime\":\""
						+ System.currentTimeMillis() + "\"}";
			}
			DemoProducer.sendBySingleRequest(json, DSPPropertyUtil.KAFKA_TOPIC);

			count++;
			// harmonize sending
			if (messageToSend-- <= 0) {
				try {
					Thread.sleep(1);
					messageToSend = (int) ((System.currentTimeMillis() - precedentTime)
							* messagePerSecond / 1000);
					precedentTime = System.currentTimeMillis();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		Thread.sleep(10000);
		producer.close();
	}

	public void close() {
		producer.close();
	}

}
