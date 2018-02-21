package com.creditease.ns.miner.flume.help;

import java.io.File;
import java.io.IOException;

import com.creditease.ns.miner.util.Utils;

public class DirWatcherDecider extends AbstractWatcherDecider {

	private String dirNamePattern;
	
	public String getDirNamePattern() {
		return dirNamePattern;
	}

	public void setDirNamePattern(String dirNamePattern) {
		this.dirNamePattern = dirNamePattern;
	}

	@Override
	public boolean decide(File file) {
	
		if (!file.isDirectory()) 
		{
			return false;
		}
		
		try {
			boolean isMatch = Utils.doMatch(dirNamePattern.toCharArray(), 0, file.getCanonicalPath().toCharArray(), 0);
			return isMatch;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
