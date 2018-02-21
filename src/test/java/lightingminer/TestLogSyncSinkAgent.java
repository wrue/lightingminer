package lightingminer;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.creditease.ns.miner.util.StagedInstall;

public class TestLogSyncSinkAgent {

	private Properties agentProps;

	@Before
	public void setup() throws Exception {

		
		System.setProperty(StagedInstall.PROP_PATH_TO_DIST_TARBALL, "/Users/liuyou/work/apache-flume-1.6.0-bin.tar.gz");
		
		agentProps = new Properties();
		String channelName = "mem-01";

		String srcName = "avrosource";

		agentProps.put(String.format("agent.sources.%s.type", srcName),
				"avro");
		agentProps.put(String.format("agent.sources.%s.port", srcName),
				"8888");
		agentProps.put(String.format("agent.sources.%s.bind", srcName),
				"0.0.0.0");
		
		
		
		agentProps.put("agent.channels."+channelName+".type", "MEMORY");
		agentProps.put("agent.channels."+channelName+".capacity", String.valueOf(100000));


		String sinkName = "logsyncsink";
		agentProps.put("agent.sinks."+sinkName+".channel", channelName);
		agentProps.put("agent.sinks."+sinkName+".type", "LogSyncSink");
		agentProps.put("agent.sinks."+sinkName+".sink.logDir", "/Users/liuyou/Downloads/logtestsink");
	

		agentProps.put("agent.sources", srcName);
		agentProps.put("agent.channels", channelName);
		agentProps.put("agent.sinks", sinkName);
	}

	@Test
	public void testRunAgent() throws Exception
	{
		 StagedInstall.getInstance().startAgent("agent", agentProps);
	}

}
