package storm.dsp.util;

import java.net.*;
import java.io.*;

public class GenericTcpClient{
	static String HOSTNAME;
	static String LOCAL_ADDR;
	static int PORT = 8686;
	
	public static void main( String[] args ){
		try {
			InetAddress localAddr = InetAddress.getLocalHost();
			LOCAL_ADDR = localAddr.getHostAddress();
			HOSTNAME = localAddr.getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		PrintStream sockOut = null;
		try {
			Socket s = new Socket( HOSTNAME, 8686 );
			sockOut = new PrintStream( s.getOutputStream() );
			String msgJSON = "{\"Point Name\":\"deiCQ615PSCADALAF2_A06_7-CBSTA\",\"type\":\"DI1/SOE1\",\"Station\":\"CQ615\",\"Subsystem\":\"PSCADA\",\"value\":0,\"acquisition time\":\"Date(1352977537621)\"}";
			sockOut.println( msgJSON );
			s.close();
		} catch ( SocketException e ) {
			System.err.println( "Socket error: " + e );
		} catch ( UnknownHostException e ) {
			System.err.println( "Invalid host!" );
		} catch ( IOException e ) {
			System.err.println( "I/O error : " + e );
		}
		
		
	}
}