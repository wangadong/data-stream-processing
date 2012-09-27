package storm.dsp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.dsp.bolt.AlarmBolt;
import storm.dsp.bolt.DispatcherBolt;
import storm.dsp.bolt.ProcessingBolt;
import storm.dsp.util.DSPPropertyUtil;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

/**
 * Storm Topology for Data Stream Processing System
 * A Kafka data source is required.
 * @author Tony WANG
 */
public class KafkaTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		List<String> hosts = new ArrayList<String>();
		// set Kafka Broker URL
		hosts.add(DSPPropertyUtil.KAFKA_BROKER_URL);
		SpoutConfig spoutConf = SpoutConfig.fromHostStrings(hosts,
				DSPPropertyUtil.KAFKA_PARTITION_PER_HOST, DSPPropertyUtil.KAFKA_TOPIC,
				DSPPropertyUtil.KAFKA_ZKROOT, DSPPropertyUtil.KAFKA_ID);
		// configuration for KafkaSpout
		spoutConf.scheme = new StringScheme();
		spoutConf
				.forceStartOffsetTime(DSPPropertyUtil.KAFKA_FORCE_START_OFFSET_TIME);

		// spoutConf.zkServers = new ArrayList<String>() {
		// {
		// add("192.168.0.188");
		// }
		// };
		// spoutConf.zkPort = 2181;

		builder.setSpout("spout", new KafkaSpout(spoutConf),
				DSPPropertyUtil.SPOUT_NUMBER);
		builder.setBolt("collection", new DispatcherBolt(),
				DSPPropertyUtil.DISPATCHER_BOLT_NUMBER)
				.shuffleGrouping("spout");
		builder.setBolt("processing", new ProcessingBolt(),
				DSPPropertyUtil.PROCESSING_BOLT_NUMBER).shuffleGrouping(
				"collection");
		builder.setBolt("alarm", new AlarmBolt(),
				DSPPropertyUtil.ALARM_BOLT_NUMBER)
				.shuffleGrouping("processing");
		// builder.setBolt("printer", new
		// PrinterBolt()).shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(DSPPropertyUtil.STORM_DEBUG);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(DSPPropertyUtil.WORKER_NUM);
			StormSubmitter.submitTopology(DSPPropertyUtil.TOPOLOGY_NAME, conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka-test", conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.killTopology("kafka-test");
			cluster.shutdown();
		}
	}
}
