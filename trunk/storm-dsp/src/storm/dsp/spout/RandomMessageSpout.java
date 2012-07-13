package storm.dsp.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import storm.dsp.ExclamationTopology.Message;



public class RandomMessageSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;    
    

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        Vector<Message> msgVector = new Vector<>();
        Message message = new Message();
        message.setMessage("Hello World");
        message.setStatus(Message.HELLO);
        msgVector.add(message);
        message.setMessage("Goodbye");
        message.setStatus(Message.GOODBYE);
        msgVector.add(message);
        Message sentence = msgVector.get(_rand.nextInt(msgVector.size()));
        _collector.emit(new Values(sentence));
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
    
}