package com.creditease.ns.miner.flume.sink.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigContext {
	
	private Map<String,Tracker> trackerCache = new ConcurrentHashMap<String, Tracker>();
	
	private static  ConfigContext instance = new ConfigContext(); 
	
	private ConfigContext() { 
	} 
	
	public synchronized static ConfigContext getInstance() { 
		if(instance == null)
		{
			instance = new ConfigContext();
		}
		
		return instance; 
	} 

	public Map<String, Tracker> getTrackerCache() {
		return trackerCache;
	}

	public void setTrackerCache(Map<String, Tracker> trackerCache) {
		this.trackerCache = trackerCache;
	}
	
	
	public Tracker getTracker(String name)
	{
		return this.trackerCache.get(name);
	}
	
	public void registerTracker(String name,Tracker tracker)
	{
		this.trackerCache.put(name, tracker);
	}
	
}
