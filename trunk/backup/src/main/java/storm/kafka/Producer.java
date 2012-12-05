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
package storm.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class Producer {
	private final kafka.javaapi.producer.Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();
	Random _rand;

	public Producer(String topic) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect", "192.168.0.188:2181");
		props.put("zk.connectiontimeout.ms", "15000");
		props.put("producer.type", "async");
//		props.put("buffer.size", "10240000");
////		props.put("connect.timeout.ms", "5000");
////		props.put("socket.timeout.ms", "30000");
////		props.put("reconnect.interval", "30000");
//		props.put("max.message.size", "10000000");
////		props.put("zk.read.num.retries", "3");
////		props.put("queue.time", "5000");
		props.put("queue.size", "5000000");
////		props.put("batch.size", "2000");
		// Use random partitioner. Don't need the key type. Just set it to
		// Integer.
		// The message is of type String.
		producer = new kafka.javaapi.producer.Producer<Integer, String>(
				new ProducerConfig(props));
		this.topic = topic;
		this._rand = new Random();
	}

	public void sendBySingleList(int i) {
		int messageNo = 1;
		List<ProducerData<Integer, String>> list = new ArrayList<ProducerData<Integer, String>>();
		while (messageNo <= i) {
			Vector<String> msgVector = new Vector<String>();
			String json = "{\"name\":\"dciPOW-SPRCHR\",\"description\":\""
					+ messageNo
					+ "\",\"type\":\"AI\",\"range\":{\"max\":100,\"min\":0},\"jitter\":0.01,\"value\":10,\"acquisition time\":\"/Date(1343899543000)/\"}";
			msgVector.add(json);
			String json1 = "{\"name\":\"dciPOW-CABTNOM2\",\"description\":\""
					+ messageNo
					+ "\",\"type\":\"DI1\",\"range\":{\"max\":100,\"min\":0},\"jitter\":0.01,\"value\":2,\"acquisition time\":\"/Date(1343899543323)/\"}";
			msgVector.add(json1);
			String ranMessage = msgVector.get(_rand.nextInt(msgVector.size()));
			String messageStr = new String("Message_" + messageNo);
			list.add(new ProducerData<Integer, String>(topic, ranMessage));
			messageNo++;
			if ((messageNo - 1) % 100 == 0) {
				// System.out.println(messageNo);
				// System.out.println(json);
			}

		}
		producer.send(list);
		System.out.println("finished");
	}
	public void sendBySingleRequest(int i) {
		int messageNo = 1;
		while (messageNo <= i) {
			Vector<String> msgVector = new Vector<String>();
			String json = "{\"name\":\"dciPOW-SPRCHR\",\"description\":\""
					+ messageNo
					+ "\",\"type\":\"AI\",\"range\":{\"max\":100,\"min\":0},\"jitter\":0.01,\"value\":10,\"acquisition time\":\"/Date(1343899543000)/\"}";
			msgVector.add(json);
			String json1 = "{\"name\":\"dciPOW-CABTNOM2\",\"description\":\""
					+ messageNo
					+ "\",\"type\":\"DI1\",\"range\":{\"max\":100,\"min\":0},\"jitter\":0.01,\"value\":2,\"acquisition time\":\"/Date(1343899543323)/\"}";
			msgVector.add(json1);
			String ranMessage = msgVector.get(_rand.nextInt(msgVector.size()));
			String messageStr = new String("Message_" + messageNo);
			producer.send(new ProducerData<Integer, String>(topic, ranMessage));
			messageNo++;
			if ((messageNo - 1) % 100 == 0) {
				 System.out.println(messageNo);
				// System.out.println(json);
			}
			
		}
		System.out.println("finished");
	}

	public static void main(String[] args) throws InterruptedException {
		Producer producerThread = new Producer("test11");
		producerThread.sendBySingleList(30000);
//		producerThread.sendBySingleRequest(100000);

		producerThread.close();
	}

	public void close() {
		// TODO Auto-generated method stub
		producer.close();
	}

}
