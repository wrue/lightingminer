package lightingminer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.AvroSink;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.ns.miner.flume.source.LogSyncSource;

public class TestLogSyncSource {
	private static final Logger logger = LoggerFactory
			.getLogger(TestLogSyncSource.class);

	private int selectedPort;
	private LogSyncSource source;
	private Channel channel;
	private InetAddress localhost;
	private AvroSink sink;

	@Before
	public void setUp() throws UnknownHostException {
		localhost = InetAddress.getByName("127.0.0.1");
		source = new LogSyncSource();
		channel = new MemoryChannel();
		Context context = new Context();

		Configurables.configure(channel, new Context());

		List<Channel> channels = new ArrayList<Channel>();
		channels.add(channel);

		ChannelSelector rcs = new ReplicatingChannelSelector();
		rcs.setChannels(channels);

		source.setChannelProcessor(new ChannelProcessor(rcs));
		
		sink = new AvroSink();
		sink.setChannel(channel);
		//source config
		context.put("watchedDir", "/Users/liuyou/Downloads/logtestsource");
		context.put("watchedFilePattern", "flow_*.log");
		
	    context.put("hostname", "127.0.0.1");
	    context.put("port", String.valueOf(8888));
	    context.put("batch-size", String.valueOf(2));
	    context.put("connect-timeout", String.valueOf(2000L));
	    context.put("request-timeout", String.valueOf(3000L));
//	    if (compressionType.equals("deflate")) {
//	      context.put("compression-type", compressionType);
//	      context.put("compression-level", Integer.toString(compressionLevel));
//	    }
	    
	    Configurables.configure(source, context);
	    Configurables.configure(channel, context);
	    Configurables.configure(sink, context);
	}
	
	@Test
	public void testSource()
	{
		 source.start();
		 sink.start();
		 
	}
}
