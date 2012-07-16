package storm.dsp.bolt;

import java.util.Map;
import java.util.Vector;

import storm.dsp.bolt.Message;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CollectionPoolBolt extends BaseRichBolt {
	OutputCollector _collector;
	static Vector<Message> msgVector = new Vector<>();

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		Message message = (Message) tuple.getValue(0);

		msgVector.add(message);
		// System.out.println("one message");
		if (msgVector.size() > 10) {
			System.out.println(msgVector);
			_collector.emit(tuple, new Values(msgVector));
			msgVector.clear();
			// System.out.println("output!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("collection"));
	}

}
