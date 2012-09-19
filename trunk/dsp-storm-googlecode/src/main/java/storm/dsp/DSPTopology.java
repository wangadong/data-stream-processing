package storm.dsp;

import java.io.File;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.config__init;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import storm.dsp.bolt.AlarmBolt;
import storm.dsp.bolt.DispatcherBolt;
import storm.dsp.bolt.ProcessingBolt;
import storm.dsp.spout.RandomMessageSpout;
import storm.dsp.util.DSPPropertyUtil;

/**
 * Storm topology for Data Stream Processing System This is a simulate topology
 * with a simulated data source {@link RandomMessageSpout} Properties is loaded
 * by {@link DSPPropertyUtil} class from config.xml
 * 
 * @author Tony WANG
 */
public class DSPTopology {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		// Set a test storm Spout with a simulated message spout
		builder.setSpout("source", new RandomMessageSpout(),
				DSPPropertyUtil.SPOUT_NUMBER);
		// Set data flow bolts for DSP
		builder.setBolt("collection", new DispatcherBolt(),
				DSPPropertyUtil.DISPATCHER_BOLT_NUMBER).shuffleGrouping(
				"source");
		builder.setBolt("processing", new ProcessingBolt(),
				DSPPropertyUtil.PROCESSING_BOLT_NUMBER).shuffleGrouping(
				"collection");
		builder.setBolt("alarm", new AlarmBolt(),
				DSPPropertyUtil.ALARM_BOLT_NUMBER)
				.shuffleGrouping("processing");
		Config conf = new Config();
		conf.setDebug(DSPPropertyUtil.STORM_DEBUG);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(DSPPropertyUtil.WORKER_NUM);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(DSPPropertyUtil.TOPOLOGY_NAME, conf,
					builder.createTopology());
			Utils.sleep(1000000);
			cluster.killTopology(DSPPropertyUtil.TOPOLOGY_NAME);
			cluster.shutdown();
		}
	}
}
