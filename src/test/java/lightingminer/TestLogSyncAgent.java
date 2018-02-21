package lightingminer;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.creditease.ns.miner.util.StagedInstall;
import com.google.common.collect.Lists;

public class TestLogSyncAgent {

	private Properties agentProps;

	@Before
	public void setup() throws Exception {

		
		System.setProperty(StagedInstall.PROP_PATH_TO_DIST_TARBALL, "/Users/liuyou/work/apache-flume-1.6.0-bin.tar.gz");
		
		agentProps = new Properties();
		String channelName = "mem-01";

		String srcName = "logsyncsource";

		agentProps.put(String.format("agent.sources.%s.type", srcName),
				"LogSyncSource");
		agentProps.put(String.format("agent.sources.%s.watchedDir", srcName),
				"/Users/liuyou/Downloads/logtestsource");
		agentProps.put(String.format("agent.sources.%s.channels", srcName),
				channelName);
		
		
		
		agentProps.put("agent.channels."+channelName+".type", "MEMORY");
		agentProps.put("agent.channels."+channelName+".capacity", String.valueOf(100000));


		String sinkName = "avrotestsink";
		agentProps.put("agent.sinks."+sinkName+".channel", channelName);
		agentProps.put("agent.sinks."+sinkName+".type", "avro");
		agentProps.put("agent.sinks."+sinkName+".sink.hostname", "127.0.0.1");
		agentProps.put("agent.sinks."+sinkName+".sink.port", "8888");
		agentProps.put("agent.sinks."+sinkName+".sink.batch-size", "2");
		agentProps.put("agent.sinks."+sinkName+".sink.connect-timeout", "2000");
		agentProps.put("agent.sinks."+sinkName+".sink.request-timeout", "2000");

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
