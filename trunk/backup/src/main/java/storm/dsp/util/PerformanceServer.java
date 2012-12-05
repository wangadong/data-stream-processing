package storm.dsp.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import net.sf.json.JSONObject;

/**
 * a TCP Socket Spout to receive Message From FEP simulation
 * 
 * @author Tony WANG
 * 
 */
public class PerformanceServer {
	static ServerSocket servSock;
	static Socket clntSock;
	static StringBuffer strbuffer;
	static String lastString;
	static LinkedList<String> queue = new LinkedList<String>();

	private static long totalDelay = 0;
	private static long count = 0;
	private static long currentDelay = 0;
	private static long currentCount = 0;

	public static void getMessageByTCPStream(InputStream in) throws IOException {
		BufferedReader bf = new BufferedReader(new InputStreamReader(in,
				"UTF-8"));
		byte[] buffer = new byte[10000];
		in.read(buffer);
		strbuffer = new StringBuffer();
		if (lastString != null)
			strbuffer.append(lastString);
		String message = new String(buffer, "utf-8");
		strbuffer.append(message);
		String[] strArray = strbuffer.toString().split("}");
		for (int i = 0; i < strArray.length - 1; i++) {
			String str = strArray[i];
			queue.add(str + "}");
			// System.out.println(str + "}");
		}
		lastString = strArray[strArray.length - 1];
	}

	public static void main(String[] args) throws IOException {

		servSock = new ServerSocket(8686);
		Socket clntSock = servSock.accept();
		SocketAddress clientAddress = clntSock.getRemoteSocketAddress();
		System.out.println("Handling client at " + clientAddress);
		DataInputStream in = new DataInputStream(clntSock.getInputStream());
		while (true) {
			try {

				getMessageByTCPStream(in);
				for (String message : queue) {
//					System.out.println(message);
					benchmark(message);
				}

				// System.out.println("\n\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// clntSock.close();
	}

	public static void benchmark(String message) {
		// Map<String, Object> jsonMap = JSONObject.fromObject((String) tuple
		// .getValue(0));
		// System.out.println(alarm);
		Map<String, Object> jsonMap = JSONObject.fromObject(message);
		if (jsonMap.containsKey("FactTypeAlarm")) {
			System.out.println(jsonMap.values());
		} else {
			try {
				long delay = System.currentTimeMillis()
						- (Long) Long.parseLong((String) jsonMap
								.get("acquisitionTime"));
				totalDelay += delay;
				count++;
				currentDelay += delay;
				currentCount++;
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		}
		if (count % DSPPropertyUtil.getDELAY_COUNT_NUM() == 0 && count != 0
				&& currentCount != 0) {
			String delay = "Average delay time: " + totalDelay / count + "ms"
					+ ",alarm count: " + count + ";Current delay time: "
					+ currentDelay / currentCount + "ms";
			currentDelay = currentCount = 0;
			// if (DSPPropertyUtil.ALARM_TO_KAFKA)
			// Producer.sendBySingleRequest("alarm1", delay);
			// else
			System.out.println(delay);
			// System.out.println(alarm);

		}
	}
}
