package storm.dsp.bolt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import storm.dsp.util.DSPPropertyUtil;
import storm.dsp.util.Producer;

import net.sf.json.JSONObject;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * This storm bolt is used to collect alarms sent by {@link ProcessingBolt} then
 * do something, by example, send to NewSCADA system
 * 
 * @author Tony WANG
 * 
 */
public class AlarmBolt extends BaseBasicBolt {
	private static long totalDelay = 0;
	private static long count = 0;
	private static long currentDelay = 0;
	private static long currentCount = 0;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// Map<String, Object> jsonMap = JSONObject.fromObject((String) tuple
		// .getValue(0));
		ArrayList<String> alarmList = (ArrayList<String>) tuple.getValue(0);
		Iterator<String> it = alarmList.iterator();
		while (it.hasNext()) {
			String alarm = it.next();
			// System.out.println(alarm);
			Map<String, Object> jsonMap = JSONObject.fromObject(alarm);
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
			}
			if (count % DSPPropertyUtil.getDELAY_COUNT_NUM() == 0 && count != 0) {
				System.out.println("Average delay time: " + totalDelay / count
						+ "ms" + ",alarm count: " + count
						+ ";Current delay time: " + currentDelay / currentCount
						+ "ms");
				currentDelay = currentCount = 0;
			}
			if (DSPPropertyUtil.ALARM_TO_KAFKA)
				Producer.sendBySingleRequest(alarm, "alarm");
			else
				// System.out.println(alarm);
				;
			jsonMap.clear();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("messageList"));
	}
}
