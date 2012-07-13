package storm.dsp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.DecisionTableConfiguration;
import org.drools.builder.DecisionTableInputType;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderErrors;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.io.ResourceFactory;
import org.drools.logger.KnowledgeRuntimeLogger;
import org.drools.logger.KnowledgeRuntimeLoggerFactory;
import org.drools.runtime.StatefulKnowledgeSession;

import storm.dsp.bolt.ExclamationBolt;
import storm.dsp.bolt.Message;
import storm.dsp.bolt.CollectionPoolBolt;
import storm.dsp.spout.RandomMessageSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("source", new RandomMessageSpout(), 2);
		builder.setBolt("collection", new CollectionPoolBolt(), 1)
				.shuffleGrouping("source");
		builder.setBolt("exclaim1", new ExclamationBolt(), 5).shuffleGrouping(
				"collection");
		Config conf = new Config();
		conf.setDebug(false);

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
