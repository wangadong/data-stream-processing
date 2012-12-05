package storm.dsp.bolt;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import clojure.main;

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
	PrintStream sockOut = null;
	Socket s;
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// Map<String, Object> jsonMap = JSONObject.fromObject((String) tuple
		// .getValue(0));
		// This is an alarm list sent by processingbolt
		ArrayList<String> alarmList = (ArrayList<String>) tuple.getValue(0);
		Iterator<String> it = alarmList.iterator();
		while (it.hasNext()) {
			String alarm = it.next();
			// System.out.println(alarm);
			// System.out.println(alarm);
			Map<String, Object> jsonMap = JSONObject.fromObject(alarm);
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
			if (count % DSPPropertyUtil.getDELAY_COUNT_NUM() == 0 && count != 0) {
				System.out.println("Average delay time: " + totalDelay / count
						+ "ms" + ",alarm count: " + count
						+ ";Current delay time: " + currentDelay / currentCount
						+ "ms");
				currentDelay = currentCount = 0;
			}
			if (DSPPropertyUtil.ALARM_TO_KAFKA) {
				
				try {
					s = new Socket("127.0.0.1", 8686);
					sockOut = new PrintStream(s.getOutputStream());
					sockOut.println(alarm);
					s.close();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
//					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
//					e.printStackTrace();
				}
			}
			jsonMap.clear();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("messageList"));
	}
	public static void main(String[] args) {
		PrintStream sockOut = null;
		Socket s;
		try {
			s = new Socket("127.0.0.1", 8686);
			sockOut = new PrintStream(s.getOutputStream());
			sockOut.println("{1111111111111111111111}");
			sockOut.println("{1111111111111111111111}");
			sockOut.println("{1111111111111111111111}");
			sockOut.println("{1111111111111111111111}");
			sockOut.println("{1111111111111111111111}");
			s.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
