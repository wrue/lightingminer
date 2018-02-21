package com.creditease.ns.miner.flume.help;

import java.io.File;


public interface WatcherDecider {
	public boolean decide(File file);
}
