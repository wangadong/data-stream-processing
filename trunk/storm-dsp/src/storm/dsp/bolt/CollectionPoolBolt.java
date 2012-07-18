package storm.dsp.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CollectionPoolBolt extends BaseRichBolt {
	OutputCollector _collector;
	static ArrayList msgList = new ArrayList();

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		Message message = (Message) tuple.getValue(0);
		msgList.add(message);

		// System.out.println("one message");
		if (msgList.size() == 10) {
			_collector.emit(new Values(new ArrayList(msgList)));
			_collector.ack(tuple);
			msgList.clear();
			// System.out.println("output!");
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("messageList"));
	}

}
