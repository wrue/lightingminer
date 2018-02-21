package com.creditease.ns.miner.flume.help;

import java.io.File;

public abstract class AbstractWatcherDecider implements WatcherDecider {

	//单位是天
	private int dateRange;
	

	public int getDateRange() {
		return dateRange;
	}

	public void setDateRange(int dateRange) {
		this.dateRange = dateRange;
	}

	protected long convertDateRangeToMs(int dateRange)
	{
		//天转成毫秒 注意要加l 否则int型可能会计算溢出或者出错
		long rangeTime = dateRange * (24 * 60 * 60 * 1000l);
		
		return	rangeTime;
	}
	
	protected boolean calFileTimeCondition(File file)
	{
		long modifiedTime = file.lastModified();
		long rangeLimitTime = convertDateRangeToMs(dateRange);
		long curTime = System.currentTimeMillis();
		long differTime = curTime - modifiedTime;
		if (differTime <= rangeLimitTime ) 
		{
			return true;
		}
		else
		{
			String path = null;
			try
			{
				path = file.getCanonicalPath();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		return false;
	}

	
}
