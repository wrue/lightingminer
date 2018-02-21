package lightingminer;

import java.net.URL;

import org.apache.flume.node.Application;

public class TestLogSourceFromApplication {
	public static void main(String[] args) 
	{
		URL url = Thread.currentThread().getContextClassLoader().getResource("logsource.conf");
		String confPath = url.getFile();
		String[] argms = new String[]{"--conf-file", confPath, "--name", "logsrcagent" };
		
		
		Application.main(argms);
		
	}
}
