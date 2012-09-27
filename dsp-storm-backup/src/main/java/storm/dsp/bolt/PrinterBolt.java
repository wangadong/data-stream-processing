package storm.dsp.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


/**
 * Just for testing
 * Print all the tuples incoming
 * 
 * @author Tony WANG
 *
 */
public class PrinterBolt extends BaseBasicBolt {

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
}
