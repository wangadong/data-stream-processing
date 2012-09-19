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

import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
/**
 * A sample producer to produce messages and send to kafka broker
 * This could be used to simulate FEP system
 * @author wangadong
 *
 */
public class Producer {
	final static private Producer _instance = new Producer();
	private static kafka.javaapi.producer.Producer<Integer, String> producer;
	private final Properties props = new Properties();

	public Producer() {
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect", DSPPropertyUtil.KAFKA_BROKER_URL+":2181");
		props.put("zk.connectiontimeout.ms", "15000");
		props.put("producer.type", "async");
		// props.put("buffer.size", "10240000");
		// props.put("connect.timeout.ms", "5000");
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

	public static Producer getInstance() {
		return _instance;
	}

	public static void sendBySingleRequest(String message, String topic) {
		producer.send(new ProducerData<Integer, String>(topic, message));
	}

	public static void main(String[] args) throws InterruptedException {
		Producer.sendBySingleRequest("topic", "message");
	}

	public void close() {
		producer.close();
	}

}
