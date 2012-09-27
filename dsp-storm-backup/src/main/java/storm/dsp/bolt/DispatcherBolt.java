package storm.dsp.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import storm.dsp.util.DSPPropertyUtil;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This class is used for dispatching messages got from Spout, and do something
 * before sending to {@link ProcessingBolt}
 * 
 * @author Tony WANG
 * 
 */
public class DispatcherBolt extends BaseBasicBolt {
	// this list is used when the messages will be packed before sending to next
	// Bolt
	ArrayList<String> msgList = new ArrayList<String>();
	static long pTime = System.currentTimeMillis();
	private int i = 0;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String message = (String) tuple.getValue(0);
		// send message list when reach the MIN_FETCH_SIZE size, if not reached, it will be
		// send every MIN_FETCH_INTERVAL milliseconds

		msgList.add(message);
		long cTime = System.currentTimeMillis();
		if (cTime - pTime >= DSPPropertyUtil.MIN_FETCH_INTERVAL) {
			collector.emit(new Values(new ArrayList<String>(msgList)));
			msgList.clear();
			pTime = cTime;
		} else if (msgList.size() >= DSPPropertyUtil.MIN_FETCH_SIZE) {
			collector.emit(new Values(new ArrayList<String>(msgList)));
			msgList.clear();
		}

		// collector.emit(new Values(message));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
