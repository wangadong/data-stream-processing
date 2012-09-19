package storm.kafka;

import java.io.Serializable;
import java.util.List;


public class SpoutConfig extends KafkaConfig implements Serializable {
    public List<String> zkServers = null;
    public Integer zkPort = null;
    public String zkRoot = null;
    public String id = null;
    public long stateUpdateIntervalMs = 2000;
    
    public SpoutConfig(List<HostPort> hosts, int partitionsPerHost, String topic, String zkRoot, String id) {
        super(hosts, partitionsPerHost, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
    
    public static SpoutConfig fromHostStrings(List<String> hosts, int partitionsPerHost, String topic, String zkRoot, String id) {
        return new SpoutConfig(KafkaConfig.convertHosts(hosts), partitionsPerHost, topic, zkRoot, id);
    }
}
