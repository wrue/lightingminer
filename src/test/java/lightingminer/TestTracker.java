package lightingminer;

import com.creditease.ns.miner.flume.sink.config.Tracker;

public class TestTracker {
	public static void main(String[] args) 
	{
		String wholePath = "C:\\CrawlProgram\\Log\\BankflowPlugin\\201610\\20161009\\flow_Debit_BOB.log";
		
		Tracker tracker = new Tracker();
		
		tracker.setStorePath("/crawlog%3");
		String path = tracker.getRealStoreFileName("10.170.173.65", wholePath);
		
		
		System.out.println("path:"+path);
		
		
	}
}
