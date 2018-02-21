package lightingminer;

import java.net.URL;

import org.apache.flume.node.Application;

public class TestLogSinkFromApplication {
	public static void main(String[] args) 
	{
		URL url = Thread.currentThread().getContextClassLoader().getResource("logdest.conf");
		String confPath = url.getFile();
		String[] argms = new String[]{"--conf-file", confPath, "--name", "logdstagent" };
		
		
		Application.main(argms);
		
	}
}
