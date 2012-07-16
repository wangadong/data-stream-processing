package storm.dsp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import storm.dsp.bolt.CollectionPoolBolt;
import storm.dsp.bolt.ProcessingBolt;
import storm.dsp.spout.RandomMessageSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class DSPTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("source", new RandomMessageSpout(), 2);
		builder.setBolt("collection", new CollectionPoolBolt(), 1)
				.shuffleGrouping("source");
		builder.setBolt("processing", new ProcessingBolt(), 1).shuffleGrouping(
				"collection");
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

}
