package storm.dsp;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.utils.Utils;

import storm.dsp.bolt.AlarmBolt;
import storm.dsp.bolt.DispatcherBolt;
import storm.dsp.bolt.ProcessingBolt;
import storm.kafka.KafkaConfig;
import storm.kafka.StringScheme;
import storm.kafka.TransactionalKafkaSpout;

/**
 * This is a Storm topology used KafkaTransacionalSpout as a data source
 * Still have issues, so KafkaSpout is recommended
 */
public class KafkaTransactionalTopology {

	public static void main(String[] args) throws Exception {
		List<String> hosts = new ArrayList<String>();
		hosts.add("192.168.0.188");
		KafkaConfig kafkaConf = KafkaConfig.fromHostStrings(hosts, 3, "test5");
		kafkaConf.scheme = new StringScheme();
		kafkaConf.forceStartOffsetTime(-2);
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder(
				"id", "spout", new TransactionalKafkaSpout(kafkaConf), 1);
		builder.setBolt("collection", new DispatcherBolt(), 1)
				.shuffleGrouping("spout");
		builder.setBolt("processing", new ProcessingBolt(),6).shuffleGrouping(
				"collection");
		builder.setBolt("alarm", new AlarmBolt(), 1).shuffleGrouping(
				"processing");
		// builder.setBolt("printer", new
		// PrinterBolt()).shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(5);
			StormSubmitter.submitTopology(args[0], conf,
					builder.buildTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka-test", conf, builder.buildTopology());
			Utils.sleep(1000000);
			cluster.killTopology("kafka-test");
			cluster.shutdown();
		}
	}
}
