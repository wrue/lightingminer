package com.creditease.ns.miner.flume.sink.config.perform;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;

import com.creditease.ns.collie.context.InterpretationContext;
import com.creditease.ns.collie.perform.Performer;
import com.creditease.ns.miner.flume.sink.config.ConfigContext;
import com.creditease.ns.miner.flume.sink.config.Tracker;

public class TrackerPerformer implements Performer{
	private static final Logger logger = LoggerFactory
			.getLogger("logSyncSink");
	@Override
	public void begin(InterpretationContext ic, String name,
			Attributes attributes) throws Exception {
		Tracker tracker = new Tracker();
		
		String trackerName = attributes.getValue("name");
		
		if (!StringUtils.isEmpty(trackerName)) 
		{
			tracker.setName(trackerName);
		}
		else
		{
			throw new Exception("必须指定Tracker的名称");
		}
		
		String storePath = attributes.getValue("storePath");
		
		if (!StringUtils.isEmpty(storePath)) 
		{
			tracker.setStorePath(storePath);
		}
		
		ic.pushObject(tracker);
		logger.info("#解析tracker元素 {} {}#",trackerName,storePath);
	}

	@Override
	public void body(InterpretationContext ic, String body) throws Exception {
		
	}

	@Override
	public void end(InterpretationContext ic, String name) throws Exception {
		Tracker tracker = (Tracker)ic.popObject();
		ConfigContext.getInstance().registerTracker(tracker.getName(), tracker);
		logger.info("#解析tracker元素并注册Tracker完毕 {}#",ConfigContext.getInstance().getTrackerCache());
	}

}
