package com.creditease.ns.miner.flume.help;

import java.io.File;

import org.apache.commons.lang.StringUtils;

import com.creditease.ns.miner.util.Utils;


public class LogFileWatcherDecider extends AbstractWatcherDecider{

	private String logFilePattern;
	
	@Override
	public boolean decide(File file) {
		if (file.exists()) 
		{
			//首先判断文件名是否遵循模式再判断修改日期和当前日期是否在dateRange 之内
			if (isWatchedFile(file.getName())) 
			{
				return calFileTimeCondition(file);
			}
		}
		
		return false;
	}

	private  boolean isWatchedFile(String fileName) {
		if (StringUtils.isEmpty(logFilePattern)) 
		{
			return true;
		}
		
		boolean isMatch = Utils.doMatch(logFilePattern.toCharArray(), 0, fileName.toCharArray(), 0);
		return isMatch;
	}

	public String getLogFilePattern() {
		return logFilePattern;
	}

	public void setLogFilePattern(String logFilePattern) {
		this.logFilePattern = logFilePattern;
	}
	
}
