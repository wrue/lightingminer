package com.creditease.ns.miner.flume.sink.config;

public interface RollingPolicy {
	
	 void rollover() throws Exception;
	 
	  CompressionMode getCompressionMode();
	  
	  void setArcPattern(String arcPattern);
}
