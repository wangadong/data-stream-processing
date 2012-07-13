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

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		Message message = (Message) tuple.getValue(0);
		Vector<Message> msgVector = new Vector<>();
		msgVector.add(message);

		_collector.emit(new Values(msgVector));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("collection"));
	}

}
