package storm.dsp.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import storm.dsp.bolt.ProcessingBolt;
import storm.dsp.spout.RandomMessageSpout;

/**
 * Properties Util class for DSP topology
 * 
 * @author Tony WANG
 * 
 */
/**
 * @author wangadong
 * 
 */

public class DSPPropertyUtil {
	// use a singleton pattern to create a properties util class
	final static private DSPPropertyUtil _instance = new DSPPropertyUtil();
	private static Properties props;
	public static String TOPOLOGY_NAME;
	/**
	 * The Knowledge package name in Guvnor Library This is used to get all the
	 * FactTypes from the package
	 */
	public static String KNOWLEDGE_PACKAGE_NAME;
	public static String KNOWLEDGE_SERVER_IP;
	/**
	 * Spout number for DSP topology, usually 1 Spout
	 */
	public static int SPOUT_NUMBER;
	/**
	 * DispatcherBolt number for DSP topology, usually 1
	 */
	public static int DISPATCHER_BOLT_NUMBER;
	/**
	 * ProcessingBolt number for DSP topology, based on the scale of input data
	 * It defines the parallel processing number
	 */
	public static int PROCESSING_BOLT_NUMBER;
	/**
	 * AlarmBolt number for DSP topology,basically 1
	 */
	public static int ALARM_BOLT_NUMBER;
	/**
	 * Workers number for DSP topology Worker is kind of container, it contains
	 * several Bolt or Spout One supervisor is able to contain 4 Workers as
	 * default, set by storm.yaml
	 */
	public static int WORKER_NUM;
	/**
	 * Kafka Server Address
	 */
	public static String KAFKA_BROKER_URL;
	public static int KAFKA_PARTITION_PER_HOST;
	/**
	 * Kafka Topic for current topology
	 */
	public static String KAFKA_TOPIC;
	/**
	 * Don't know what it's used for..just ignore it
	 */
	public static String KAFKA_ZKROOT;
	public static String KAFKA_ID;
	/**
	 * It is a config of KafkaSpout, used to define where to start consuming
	 * from Topic
	 */
	public static int KAFKA_FORCE_START_OFFSET_TIME;
	/**
	 * True if you want output all the message record
	 */
	public static boolean STORM_DEBUG;
	/**
	 * Just for Test Case
	 */
	public static int TEST_CASE;
	/**
	 * For Test Case, the number of message per sec sent by
	 * {@link RandomMessageSpout}
	 */
	public static int MESSAGE_PER_SECOND;
	/**
	 * The minimum interval for dispatching the incoming messages to
	 * {@link ProcessingBolt}, measured by millisecond This should be defined
	 * with MIN_FETCH_SIZE together to fit the incoming messages
	 */
	public static int MIN_FETCH_INTERVAL;
	/**
	 * The minimum list size for dispatching message to {@link ProcessingBolt},
	 * if the number of incoming message per MESSAGE_PER_SECOND is higher than
	 * this size, it will be sent immediately
	 * 
	 */
	public static int MIN_FETCH_SIZE;

	/**
	 * If True, the alarm will sent to a Kafka Broker with topic "alarm"
	 */
	public static boolean ALARM_TO_KAFKA;
	/**
	 * For test case, Total message number for one test
	 */
	public static int MESSAGE_TOTAL;
	/**
	 * For test case, It's used to set the count frequency
	 */
	public static int DELAY_COUNT_NUM;

	private DSPPropertyUtil() {
		if (ClassLoader.getSystemResourceAsStream("config.xml") != null)
			loadXMLConfig();
	}

	public static DSPPropertyUtil getInstance() {
		return _instance;
	}

	public static void getSysInfo() {

		Properties properties = new Properties();

		properties = System.getProperties();
		properties.list(System.out);
	}

	/**
	 * Load XML configuation file with a filepath Not available for cluster
	 * deployment
	 * 
	 * @param filepath
	 *            the config.xml file path
	 */
	public static void loadXMLConfig(String url) {
		try {
			props = new Properties();
			// load properties XML file from classpath
			InputStream file = new URL(url).openStream();
			props.load(file);
			TOPOLOGY_NAME = props.getProperty("TOPOLOGY_NAME");
			KNOWLEDGE_PACKAGE_NAME = props
					.getProperty("KNOWLEDGE_PACKAGE_NAME");
			KNOWLEDGE_SERVER_IP = props.getProperty("KNOWLEDGE_SERVER_IP");
			SPOUT_NUMBER = Integer.parseInt(props.getProperty("SPOUT_NUMBER"));
			DISPATCHER_BOLT_NUMBER = Integer.parseInt(props
					.getProperty("DISPATCHER_BOLT_NUMBER"));
			PROCESSING_BOLT_NUMBER = Integer.parseInt(props
					.getProperty("PROCESSING_BOLT_NUMBER"));
			ALARM_BOLT_NUMBER = Integer.parseInt(props
					.getProperty("ALARM_BOLT_NUMBER"));
			WORKER_NUM = Integer.parseInt(props.getProperty("WORKER_NUM"));
			KAFKA_BROKER_URL = props.getProperty("KAFKA_BROKER_URL");
			KAFKA_PARTITION_PER_HOST = Integer.parseInt(props
					.getProperty("KAFKA_PARTITION_PER_HOST"));
			KAFKA_TOPIC = props.getProperty("KAFKA_TOPIC");
			KAFKA_ZKROOT = props.getProperty("KAFKA_ZKROOT");
			KAFKA_ID = props.getProperty("KAFKA_ID");
			KAFKA_FORCE_START_OFFSET_TIME = Integer.parseInt(props
					.getProperty("KAFKA_FORCE_START_OFFSET_TIME"));
			STORM_DEBUG = Boolean
					.parseBoolean(props.getProperty("STORM_DEBUG"));
			ALARM_TO_KAFKA = Boolean.parseBoolean(props
					.getProperty("ALARM_TO_KAFKA"));
			TEST_CASE = Integer.parseInt(props.getProperty("TEST_CASE"));
			MESSAGE_TOTAL = Integer
					.parseInt(props.getProperty("MESSAGE_TOTAL"));

			MESSAGE_PER_SECOND = Integer.parseInt(props
					.getProperty("MESSAGE_PER_SECOND"));
			MIN_FETCH_INTERVAL = Integer.parseInt(props
					.getProperty("MIN_FETCH_INTERVAL"));
			MIN_FETCH_SIZE = Integer.parseInt(props
					.getProperty("MIN_FETCH_SIZE"));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Load XML configuration file from classpath
	 */
	public static void loadXMLConfig() {
		try {
			props = new Properties();
			// load properties XML file from classpath
			props.loadFromXML(ClassLoader
					.getSystemResourceAsStream("config.xml"));
			TOPOLOGY_NAME = props.getProperty("TOPOLOGY_NAME");
			KNOWLEDGE_PACKAGE_NAME = props
					.getProperty("KNOWLEDGE_PACKAGE_NAME");
			KNOWLEDGE_SERVER_IP = props.getProperty("KNOWLEDGE_SERVER_IP");
			SPOUT_NUMBER = Integer.parseInt(props.getProperty("SPOUT_NUMBER"));
			DISPATCHER_BOLT_NUMBER = Integer.parseInt(props
					.getProperty("DISPATCHER_BOLT_NUMBER"));
			PROCESSING_BOLT_NUMBER = Integer.parseInt(props
					.getProperty("PROCESSING_BOLT_NUMBER"));
			ALARM_BOLT_NUMBER = Integer.parseInt(props
					.getProperty("ALARM_BOLT_NUMBER"));
			WORKER_NUM = Integer.parseInt(props.getProperty("WORKER_NUM"));
			KAFKA_BROKER_URL = props.getProperty("KAFKA_BROKER_URL");
			KAFKA_PARTITION_PER_HOST = Integer.parseInt(props
					.getProperty("KAFKA_PARTITION_PER_HOST"));
			KAFKA_TOPIC = props.getProperty("KAFKA_TOPIC");
			KAFKA_ZKROOT = props.getProperty("KAFKA_ZKROOT");
			KAFKA_ID = props.getProperty("KAFKA_ID");
			KAFKA_FORCE_START_OFFSET_TIME = Integer.parseInt(props
					.getProperty("KAFKA_FORCE_START_OFFSET_TIME"));
			STORM_DEBUG = Boolean
					.parseBoolean(props.getProperty("STORM_DEBUG"));
			ALARM_TO_KAFKA = Boolean.parseBoolean(props
					.getProperty("ALARM_TO_KAFKA"));
			TEST_CASE = Integer.parseInt(props.getProperty("TEST_CASE"));
			MESSAGE_TOTAL = Integer
					.parseInt(props.getProperty("MESSAGE_TOTAL"));

			MESSAGE_PER_SECOND = Integer.parseInt(props
					.getProperty("MESSAGE_PER_SECOND"));
			MIN_FETCH_INTERVAL = Integer.parseInt(props
					.getProperty("MIN_FETCH_INTERVAL"));
			MIN_FETCH_SIZE = Integer.parseInt(props
					.getProperty("MIN_FETCH_SIZE"));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * list all the properties of DSP system
	 */
	public static void getDSPPropInfo() {
		props.list(System.out);
	}

	/**
	 * used to create a config.xml file
	 * 
	 * @throws IOException
	 */
	public static void writePropsToXML() throws IOException {
		Properties prop = new Properties();
		prop.setProperty("TOPOLOGY_NAME", "test");
		prop.setProperty("KNOWLEDGE_PACKAGE_NAME", "DSP");
		prop.setProperty("KNOWLEDGE_SERVER_IP", "192.168.0.107");
		prop.setProperty("SPOUT_NUMBER", "1");
		prop.setProperty("DISPATCHER_BOLT_NUMBER", "1");
		prop.setProperty("PROCESSING_BOLT_NUMBER", "4");
		prop.setProperty("ALARM_BOLT_NUMBER", "1");
		prop.setProperty("WORKER_NUM", "7");
		prop.setProperty("KAFKA_BROKER_URL", "192.168.0.107");
		prop.setProperty("KAFKA_PARTITION_PER_HOST", "1");
		prop.setProperty("KAFKA_TOPIC", "test");
		prop.setProperty("KAFKA_ZKROOT", "/home/dsp/dsp/zookeeper-3.3.6");
		prop.setProperty("KAFKA_ID", "id");
		prop.setProperty("KAFKA_FORCE_START_OFFSET_TIME", "-2");
		prop.setProperty("STORM_DEBUG", "False");
		prop.setProperty("TEST_CASE", "1");
		prop.setProperty("MESSAGE_TOTAL", "500000");
		prop.setProperty("MESSAGE_PER_SECOND", "1000");
		prop.setProperty("MIN_FETCH_SIZE", "200");
		prop.setProperty("MIN_FETCH_INTERVAL", "50");
		prop.setProperty("ALARM_TO_KAFKA", "True");

		FileOutputStream fos = new FileOutputStream(
				"src/main/resources/config.xml");
		prop.storeToXML(fos, "config");
		fos.close();
	}

	public static long getDELAY_COUNT_NUM() {
		if (DELAY_COUNT_NUM > 0)
			return DELAY_COUNT_NUM;
		else
			setDELAY_COUNT_NUM(10);
		return DELAY_COUNT_NUM;
	}

	public static void setDELAY_COUNT_NUM(int dELAY_COUNT_NUM) {
		DELAY_COUNT_NUM = dELAY_COUNT_NUM;
	}

	public static String getTOPOLOGY_NAME() {
		return TOPOLOGY_NAME;
	}

	public static void setTOPOLOGY_NAME(String tOPOLOGY_NAME) {
		TOPOLOGY_NAME = tOPOLOGY_NAME;
	}

	public static String getKNOWLEDGE_PACKAGE_NAME() {
		return KNOWLEDGE_PACKAGE_NAME;
	}

	public static void setKNOWLEDGE_PACKAGE_NAME(String kNOWLEDGE_PACKAGE_NAME) {
		KNOWLEDGE_PACKAGE_NAME = kNOWLEDGE_PACKAGE_NAME;
	}

	public static int getSPOUT_NUMBER() {
		return SPOUT_NUMBER;
	}

	public static void setSPOUT_NUMBER(int sPOUT_NUMBER) {
		SPOUT_NUMBER = sPOUT_NUMBER;
	}

	public static int getDISPATCHER_BOLT_NUMBER() {
		return DISPATCHER_BOLT_NUMBER;
	}

	public static void setDISPATCHER_BOLT_NUMBER(int dISPATCHER_BOLT_NUMBER) {
		DISPATCHER_BOLT_NUMBER = dISPATCHER_BOLT_NUMBER;
	}

	public static int getPROCESSING_BOLT_NUMBER() {
		return PROCESSING_BOLT_NUMBER;
	}

	public static void setPROCESSING_BOLT_NUMBER(int pROCESSING_BOLT_NUMBER) {
		PROCESSING_BOLT_NUMBER = pROCESSING_BOLT_NUMBER;
	}

	public static int getALARM_BOLT_NUMBER() {
		return ALARM_BOLT_NUMBER;
	}

	public static void setALARM_BOLT_NUMBER(int aLARM_BOLT_NUMBER) {
		ALARM_BOLT_NUMBER = aLARM_BOLT_NUMBER;
	}

	public static int getWORKER_NUM() {
		return WORKER_NUM;
	}

	public static void setWORKER_NUM(int wORKER_NUM) {
		WORKER_NUM = wORKER_NUM;
	}

	public static String getKAFKA_BROKER_URL() {
		return KAFKA_BROKER_URL;
	}

	public static void setKAFKA_BROKER_URL(String kAFKA_BROKER_URL) {
		KAFKA_BROKER_URL = kAFKA_BROKER_URL;
	}

	public static int getKAFKA_PARTITION_PER_HOST() {
		return KAFKA_PARTITION_PER_HOST;
	}

	public static void setKAFKA_PARTITION_PER_HOST(int kAFKA_PARTITION_PER_HOST) {
		KAFKA_PARTITION_PER_HOST = kAFKA_PARTITION_PER_HOST;
	}

	public static String getKAFKA_TOPIC() {
		return KAFKA_TOPIC;
	}

	public static void setKAFKA_TOPIC(String kAFKA_TOPIC) {
		KAFKA_TOPIC = kAFKA_TOPIC;
	}

	public static String getKAFKA_ZKROOT() {
		return KAFKA_ZKROOT;
	}

	public static void setKAFKA_ZKROOT(String kAFKA_ZKROOT) {
		KAFKA_ZKROOT = kAFKA_ZKROOT;
	}

	public static String getKAFKA_ID() {
		return KAFKA_ID;
	}

	public static void setKAFKA_ID(String kAFKA_ID) {
		KAFKA_ID = kAFKA_ID;
	}

	public static int getKAFKA_FORCE_START_OFFSET_TIME() {
		return KAFKA_FORCE_START_OFFSET_TIME;
	}

	public static void setKAFKA_FORCE_START_OFFSET_TIME(
			int kAFKA_FORCE_START_OFFSET_TIME) {
		KAFKA_FORCE_START_OFFSET_TIME = kAFKA_FORCE_START_OFFSET_TIME;
	}

	public static boolean isSTORM_DEBUG() {
		return STORM_DEBUG;
	}

	public static void setSTORM_DEBUG(boolean sTORM_DEBUG) {
		STORM_DEBUG = sTORM_DEBUG;
	}

	public static int getTEST_CASE() {
		return TEST_CASE;
	}

	public static void setTEST_CASE(int tEST_CASE) {
		TEST_CASE = tEST_CASE;
	}

	public static int getMESSAGE_PER_SECOND() {
		return MESSAGE_PER_SECOND;
	}

	public static void setMESSAGE_PER_SECOND(int mESSAGE_PER_SECOND) {
		MESSAGE_PER_SECOND = mESSAGE_PER_SECOND;
	}

	public static int getMIN_FETCH_INTERVAL() {
		return MIN_FETCH_INTERVAL;
	}

	public static void setMIN_FETCH_INTERVAL(int mIN_FETCH_INTERVAL) {
		MIN_FETCH_INTERVAL = mIN_FETCH_INTERVAL;
	}

	public static int getMIN_FETCH_SIZE() {
		return MIN_FETCH_SIZE;
	}

	public static void setMIN_FETCH_SIZE(int mIN_FETCH_SIZE) {
		MIN_FETCH_SIZE = mIN_FETCH_SIZE;
	}

	public static boolean isALARM_TO_KAFKA() {
		return ALARM_TO_KAFKA;
	}

	public static void setALARM_TO_KAFKA(boolean aLARM_TO_KAFKA) {
		ALARM_TO_KAFKA = aLARM_TO_KAFKA;
	}

	public static int getMESSAGE_TOTAL() {
		return MESSAGE_TOTAL;
	}

	public static void setMESSAGE_TOTAL(int mESSAGE_TOTAL) {
		MESSAGE_TOTAL = mESSAGE_TOTAL;
	}

	public static void main(String args[]) throws Exception {
		DSPPropertyUtil.writePropsToXML();
		if (ClassLoader.getSystemResourceAsStream("config.xml") != null)
			getDSPPropInfo();
	}
}
