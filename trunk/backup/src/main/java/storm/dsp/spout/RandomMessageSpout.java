package storm.dsp.spout;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import storm.dsp.util.DSPPropertyUtil;

import net.sf.json.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * a Kafka Spout simulator Send random message to storm topology to do some
 * functional tests
 * 
 * @author Tony WANG
 * 
 */
public class RandomMessageSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	Random _rand;
	int count;
	int messagePerSecond;
	int messageToSend;
	private static long precedentTime;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
		count = 0;

	}

	@Override
	public void nextTuple() {
		if (count == 5) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Vector<String> msgVector = new Vector<String>();
		switch (DSPPropertyUtil.TEST_CASE) {
		// Test Case 1:Processing Performance and Scalability
		// 1 Spout,1 DispatchBolt,6 ProcessingBolt,1 AlarmBolt
		// 10000 msgs/sec
		// 2 times test
		// 300000 msgs before adding new node:Average Delay Time, completeness
		// 300000 msgs after adding new node:Average Delay Time,completeness
		// 1 alarm message every 10 message
		case (1):
			DSPPropertyUtil.setDELAY_COUNT_NUM(10);
			messagePerSecond = DSPPropertyUtil.MESSAGE_PER_SECOND;
			if (count < DSPPropertyUtil.MESSAGE_TOTAL) {
				String json;
				if (count % 10 == 0) {
					json = "{\"point_name\":\"deiCQ615PSCADAACP1_AC-DCPOWIN2\",\"description\":\"交直流屏(AC)直流屏1#交流电源投入\",\"type\":\"DI1/SOE1\",\"value\":10,\"acquisitionTime\":\""
							+ System.currentTimeMillis() + "\"}";
				} else {
					json = "{\"point_name\":\"deiCQ615PSCADAACP1_AC-DCPOWIN2\",\"description\":\"交直流屏(AC)直流屏1#交流电源投入\",\"type\":\"DI1/SOE1\",\"value\":0,\"acquisitionTime\":\""
							+ System.currentTimeMillis() + "\"}";
				}
//				System.out.println(json);
				_collector.emit(new Values(json));
				count++;
				//
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
			break;
		// Test Case 2:Fault-tolerant
		// random new message type
		// add new fact type from GUI to match new type message
		case (2):
			DSPPropertyUtil.setDELAY_COUNT_NUM(1000);
			messagePerSecond = DSPPropertyUtil.MESSAGE_PER_SECOND;
			if (count < DSPPropertyUtil.MESSAGE_TOTAL) {
				String json;
				if (count % 10000 == 0) {
					json = "{\"point_name\":\"deiCQ615PSCADAACP1_AC-DCPOWIN2\",\"description\":\"交直流屏(AC)直流屏1#交流电源投入\",\"type\":\"DI1/SOE1\",\"value\":0,\"acquisitionTime\":\""
							+ System.currentTimeMillis()
							+ "\",\"optional1\":\"new field\"}";
				} else {
					json = "{\"point_name\":\"deiCQ615PSCADAACP1_AC-DCPOWIN2\",\"description\":\"交直流屏(AC)直流屏1#交流电源投入\",\"type\":\"DI1/SOE1\",\"value\":10,\"acquisitionTime\":\""
							+ System.currentTimeMillis() + "\"}";
				}
				_collector.emit(new Values(json));
				count++;
				//
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
			break;
		// Test Case 3:Dynamic Rule Changing
		// rule change during 300 seconds
		case (3):
			messagePerSecond = DSPPropertyUtil.MESSAGE_PER_SECOND;
			if (count < DSPPropertyUtil.MESSAGE_TOTAL) {
				String json;
				if (count % 10 == 0) {
					json = "{\"point_name\":\"deiCQ615PSCADAACP1_AC-DCPOWIN2\",\"description\":\"交直流屏(AC)直流屏1#交流电源投入\",\"type\":\"DI1/SOE1\",\"value\":0,\"acquisitionTime\":\""
							+ System.currentTimeMillis() + "\"}";
				} else {
					json = "{\"point_name\":\"deiCQ615PSCADAACP1_AC-DCPOWIN2\",\"description\":\"交直流屏(AC)直流屏1#交流电源投入\",\"type\":\"DI1/SOE1\",\"value\":10,\"acquisitionTime\":\""
							+ System.currentTimeMillis() + "\"}";
				}
				_collector.emit(new Values(json));
				count++;
				//
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
			break;
		default:

		}
		// Utils.sleep(100);

		// String ranMessage = msgVector.get(_rand.nextInt(msgVector.size()));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

	public static void main(String[] args) {
		String json = "{\"name\":\"dciPOW-SPRCHR\",\"description\":\"35kV\",\"type\":\"AI\",\"range\":{\"max\":100,\"min\":0},\"jitter\":0.01,\"value\":10,\"acquisition time\":\"/Date(1343899543000)/\"}";
		Map map = JSONObject.fromObject(json);
		Iterator it = map.keySet().iterator();
		while (it.hasNext())
			System.out.println(it.next());
	}
}