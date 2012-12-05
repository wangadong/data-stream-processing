package storm.dsp.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Map;

import net.sf.json.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;

/**
 * a TCP Socket Spout to receive Message From FEP simulation
 * 
 * @author Tony WANG
 * 
 */
public class TCPMessageServer {
	SpoutOutputCollector _collector;
	static ServerSocket servSock;
	static Socket clntSock;
	static StringBuffer strbuffer;
	static String lastString;
	static LinkedList<String> queue = new LinkedList<String>();

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

		servSock = new ServerSocket(1024);
		Socket clntSock = servSock.accept();
		SocketAddress clientAddress = clntSock.getRemoteSocketAddress();
		System.out.println("Handling client at " + clientAddress);
		DataInputStream in = new DataInputStream(clntSock.getInputStream());
		while (true) {
			try {

				getMessageByTCPStream(in);
				String message = queue.pop();
				Map<String, Object> jsonMap = JSONObject.fromObject(message);
				String acquisitionTime = "" + System.currentTimeMillis() + "";
				jsonMap.remove("acquisition time");
				jsonMap.put("acquisitionTime", acquisitionTime);

				System.out.println(jsonMap);

				Producer.getInstance().sendBySingleRequest(
						jsonMap.toString(),"topic");
				// System.out.println("\n\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// clntSock.close();
	}
}