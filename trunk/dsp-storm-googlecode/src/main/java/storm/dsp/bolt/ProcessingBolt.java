package storm.dsp.bolt;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.impl.Log4JLogger;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.agent.KnowledgeAgent;
import org.drools.agent.KnowledgeAgentConfiguration;
import org.drools.agent.KnowledgeAgentFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.decisiontable.ExternalSpreadsheetCompiler;
import org.drools.definition.type.FactType;
import org.drools.io.Resource;
import org.drools.io.ResourceChangeScannerConfiguration;
import org.drools.io.ResourceFactory;
import org.drools.io.impl.ByteArrayResource;
import org.drools.logger.KnowledgeRuntimeLogger;
import org.drools.logger.KnowledgeRuntimeLoggerFactory;
import org.drools.runtime.StatefulKnowledgeSession;
import org.slf4j.impl.Log4jLoggerFactory;

import storm.dsp.util.DSPPropertyUtil;

//import storm.dsp.POJO.Message;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This is a Storm Bolt for processing the input data. Drools Rule Engine is
 * used in this bolt.
 * KnowledgeAgent is used to monitor the knowledge base and if rules have been changed, it will update automatically
 * 
 * 
 * @author Tony WANG
 * 
 */
public class ProcessingBolt extends BaseBasicBolt {

	// initialize a KnowledgeAgent to manage remote knowledge base
	static KnowledgeAgent kagent = createKnowledgeAgent();
	// fact type for Drools
	private static Object message;
	// a global ArrayList used by Drools Engine
	List<String> list = new ArrayList<String>();

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// String json = (String) tuple.getValue(0);
		List<String> msgList = (ArrayList<String>) tuple.getValue(0);
		Iterator<String> msgIt = msgList.iterator();
		// load up the knowledge base
		KnowledgeBase kbase = kagent.getKnowledgeBase();
		try {
			// GET A KSESSION
			StatefulKnowledgeSession ksession = kbase
					.newStatefulKnowledgeSession();
			// KnowledgeRuntimeLogger logger = KnowledgeRuntimeLoggerFactory
			// .newFileLogger(ksession, "test");

			ksession.setGlobal("list", list);
			// go !
			// get all the FactTypes from KnowledgeBase to match the incoming
			// data
			Collection<FactType> factTypes = kbase.getKnowledgePackage(
					DSPPropertyUtil.KNOWLEDGE_PACKAGE_NAME).getFactTypes();
			while (msgIt.hasNext()) {
				// turn the incoming data into related fact type which could be
				// recognized by Drools Engine
				Map<String, Object> jsonMap = JSONObject.fromObject(msgIt
						.next());
				// if no FactType is suitable, send an alarm to Administrator
				// TODO determine the right method to send to Administrator
				if (!setFactTypeFromMap(message, jsonMap, factTypes))
					list.add("{\"FactTypeAlarm\":\""+"Message with KEYS:" + jsonMap.keySet().toString()
							+ " is not contained in FactTypes\"}");
				// insert converted message
				ksession.insert(message);
			}
			// process
			ksession.fireAllRules();
			// logger.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (!list.isEmpty())
			collector.emit(new Values(new ArrayList<String>(list)));
		// after sending the alarm list to next Bolt, clear the list for next
		// use
		list.clear();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("alarm"));
	}

	/**
	 * create a KnowledgeAgent with a remote Guvnor knowledge base
	 * 
	 * @return a Knowledge Agent
	 */
	private static KnowledgeAgent createKnowledgeAgent() {
		// when use changeset.xml
		// please include username/pwd in your ChangeSet, see below:
		/*
		 * <add> <resource source=
		 * 'http://localhost:8081/jboss-brms/org.drools.guvnor.Guvnor/package/defaultPackage/LATEST'
		 * type='PKG' basicAuthentication='enabled' username='admin'
		 * password='admin'/> </add>
		 */
		// create a new KnowledgeAgent
		KnowledgeAgent kagent = KnowledgeAgentFactory
				.newKnowledgeAgent("GuvnorAgent");
		// set the changeset.xml as Resource source
		Resource changeset = ResourceFactory.newInputStreamResource(ClassLoader
				.getSystemResourceAsStream("ChangeSet.xml"));
		kagent.applyChangeSet(changeset);

		// set auto scanning
		ResourceChangeScannerConfiguration sconf = ResourceFactory
				.getResourceChangeScannerService()
				.newResourceChangeScannerConfiguration();
		// Set the disk scanning interval to 10s, default is 60s.
		sconf.setProperty("drools.resource.scanner.interval", "10");
		ResourceFactory.getResourceChangeScannerService().configure(sconf);
		// start the auto scanning service
		ResourceFactory.getResourceChangeNotifierService().start();
		ResourceFactory.getResourceChangeScannerService().start();
		return kagent;
	}

	/**
	 * This method is used for converting json map into related FactType defined
	 * in knowledge base
	 * 
	 * @param bean
	 *            the fact to set the values of the fields on
	 * @param json
	 *            input json Map
	 * @param factTypes
	 *            all factTypes pre-defined in knowledge base
	 * @return true if found related factType false if a new data model with no
	 *         related facttype define in knowledge base
	 */
	private static boolean setFactTypeFromMap(Object bean, Map json,
			Collection factTypes) {
		Iterator<FactType> it = factTypes.iterator();
		FactType ft = it.next();
		while (it.hasNext()) {
			try {
				ft = it.next();
				message = ft.newInstance();
				ft.setFromMap(message, json);
			} catch (Exception e) {
				continue;
			}
			return true;
		}
		return false;

	}

	/**
	 * Test method
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		String json1 = "{\"point_name\":\"dciPOW-HOSTAT\",\"description\":\"35kV\",\"type\":\"DI1\",\"max\":100,\"min\":0,\"jitter\":0.01,\"value\":2,\"acquisitionTime\":\"/Date(1343899543323)/\",\"optional1\":\"Message\"}";
		Map json = JSONObject.fromObject(json1);
		List<String> list = new ArrayList<String>();
		int i = 0;
		// dispose
		while (i < 100000) {
			KnowledgeBase kbase = kagent.getKnowledgeBase();
			Collection factTypes = kbase.getKnowledgePackage("DSP")
					.getFactTypes();
			if (!setFactTypeFromMap(message, json, factTypes))
				list.add("Message with KEYS:" + json.keySet().toString()
						+ " is not contained in FactTypes");
			// FactType ft = kbase.getFactType("DSP", "newMessage");
			StatefulKnowledgeSession ksession = kbase
					.newStatefulKnowledgeSession();
			ksession.setGlobal("list", list);
			ksession.insert(message);
			ksession.fireAllRules();
			System.out.println(list.get(list.size() - 1));
			Thread.sleep(500);
		}
	}

}
