package com.creditease.ns.miner.flume.handler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.slf4j.Logger;

import com.creditease.ns.miner.flume.constant.CommonConstants;
import com.creditease.ns.miner.flume.source.LogSyncSource;
import com.creditease.ns.miner.util.Utils;

public class LogWriteHandler implements Runnable{

	private Path logPath;
	private volatile boolean stop;
	private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>();
	private BufferedWriter writer = null;
	private String logRealPath = null;
	private File logFile;
	private long startTime = System.currentTimeMillis();
	public static long LIFETIME = 30 * 3600 * 1000l;
	private Logger logger =  LogSyncSource.logger;
	private long curLineNum = 0;
	
	@Override
	public void run() {
		while ( !Thread.interrupted() && !stop)
		{
			Event event = null;
			try {
				event = eventQueue.take();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				return;
			}
			
			try {
				writeContent(event);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("#同步日志出现异常# {}",logRealPath,e);
				shutdownNow();
			}
		}
	}

	public void startHandle(Executor executor) throws Exception {
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile,true),"utf-8"));
			curLineNum = Utils.getTotalLines(logRealPath);
			logger.debug("#启动新的handler# {} {}",curLineNum,logRealPath);
		} catch (Exception e) {
			logger.error("#启动handler线程失败# {}",logPath.toString(),e);	
			throw e;
		}
		executor.execute(this);
		stop = false;
	}

	public Path getLogPath() {
		return logPath;
	}

	public void setLogPath(Path logPath) {
		this.logPath = logPath;
	}

	public String getLogRealPath() {
		return logRealPath;
	}

	public void setLogRealPath(String logRealPath) {
		this.logRealPath = logRealPath;
	}

	public File getLogFile() {
		return logFile;
	}

	public void setLogFile(File logFile) {
		this.logFile = logFile;
	}


	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	public void putEventQueue(Event event) {
		eventQueue.add(event);
	}
	
	private void writeContent(Event event) throws Exception{
		try {
			boolean isContinue = preWrite(event);
			if (!isContinue) 
			{
				logger.debug("#不符合写入条件,直接返回# {} {}",curLineNum,logRealPath);
				return;
			}
			writer.write(new String(event.getBody(),"utf-8"));
			writer.flush();
			LineRange range = getLineRange(event.getHeaders());
			curLineNum = range.endLineNum;
			logger.debug("#写入日志中...# {} {}",curLineNum,logRealPath);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("#写入日志异常,停止线程# {} {}",curLineNum,logRealPath,e);
			throw e;
		}
	}
	
	
	/*
		得到每个日志的行数，放入到对应的实例变量中

		当得到事件后分析事件的line范围

		如果当前文件的行数大于等于line的EndLineNum 则这个事件应该是重启后重新发的事件，
		我们简单丢弃，如果当前的文件行数小于startLineNum并且刚刚差值等于1 则继续写入日志
		如果差值不等于1 我们选择暂停同步，并打印错误日志警告

		如果当前的文件行数大于等于startLineNum并且小于EndLineNum 我们算出开始的偏移 当前文件行数-startLineNum +1 得到应该打印的行数 下标从0开始



		5 6 7 8
		当前文件是第6行 6-5 = 1 我们读取的偏移是第三行
		当前文件是第7行 7-5 = 2 我们读取的偏移是第四行
	*/
	private boolean preWrite(Event event) throws Exception
	{
		boolean isContinue = false;
		Map<String,String> headers = event.getHeaders();
		LineRange range = getLineRange(headers);
		
		if (range == null) 
		{
			logger.error("#出现非法的头部信息# {}",headers);
			throw new Exception("出现非法的event头信息");
		}
		
		long startLineNum = range.startLineNum;
		long endLineNum = range.endLineNum;
		
		if (curLineNum >= endLineNum) 
		{
			//什么都不做，只是为了减少后续的判断
			logger.debug("#传入的日志内容已经写入,略过当前事件# {} {} {} {}",logRealPath,curLineNum,startLineNum,endLineNum);
		}
		else if (curLineNum < startLineNum && ((startLineNum-curLineNum) == 1)) 
		{
			logger.debug("#传入的事件连续，可继续写入# {} {} {}",logRealPath,curLineNum,startLineNum);
			isContinue = true;
		}
		else if(curLineNum < startLineNum)
		{
			logger.error("#出现不连续的日志同步事件# {} {} {}",logRealPath,curLineNum,startLineNum);
			throw new Exception("出现不连续的日志同步事件，应该停止写入");
		}
		else if(curLineNum >= startLineNum && curLineNum < endLineNum)
		{
			logger.info("#出现当前日志卡在事件传入行范围之间的情况,抽出要写入的内容# {} {} {} {}",startLineNum,endLineNum,curLineNum,logRealPath);
			String[] filteredLines =  filterContent(tranportInLines(new String(event.getBody(),"utf-8")), (int)(curLineNum-startLineNum + 1));
			if(filteredLines != null && filteredLines.length > 0)
			{
				StringBuilder builder = new StringBuilder();
				for (int i = 0; i < filteredLines.length; i++) 
				{
					builder.append(filteredLines[i]+"\n");
				}
				event.setBody(builder.toString().getBytes("utf-8"));
				logger.debug("#抽取了出新的内容，并覆盖原来Event中的内容# {} {} {} {}",curLineNum,startLineNum,endLineNum,logRealPath);
				isContinue = true;
			}
		}
		else
		{
			
		}
		return isContinue;
		
	}
	
	private String[] tranportInLines(String content)
	{
		String[] lines = content.split("\n");
		return lines;
	}
	
	private String[] filterContent(String[]rawLines,int begin)
	{
		if (rawLines == null) 
		{
			return null;
		}
		
		if (begin > rawLines.length) 
		{
			return null;
		}
		String[] newLines = new String[rawLines.length-begin];
		System.arraycopy(rawLines, begin, newLines, 0, newLines.length);
		return newLines;
	}
	
	public LineRange getLineRange(Map<String,String> headers)
	{
		LineRange ranger = null;
		if (headers != null) 
		{
			ranger = new LineRange();
			String lineRange = headers.get(CommonConstants.EVENT_HEADERS_LINERANGE_KEY);
			if (!StringUtils.isEmpty(lineRange)) 
			{
				String[] ranges = lineRange.split(CommonConstants.LINERANGE_SEPERATOR);
				long startLineNum = Long.parseLong(ranges[0]);
				long endLineNum = Long.parseLong(ranges[1]);
				
				ranger.startLineNum = startLineNum;
				ranger.endLineNum = endLineNum;
			}
		}
		
		return ranger;
	}
	
	static class LineRange
	{
		public long startLineNum = -1;
		public long endLineNum = -1;
	}
	
	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	} 

	

	public BlockingQueue<Event> getEventQueue() {
		return eventQueue;
	}

	public void setEventQueue(BlockingQueue<Event> eventQueue) {
		this.eventQueue = eventQueue;
	}

	public Logger getLogger() {
		return logger;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public long getCurOffset() {
		return curLineNum;
	}

	public void setCurOffset(long curOffset) {
		this.curLineNum = curOffset;
	}

	

	public void shutdownNow() {
		this.stop =  true;
		this.eventQueue.clear();
		Thread.currentThread().interrupt();
		if (this.writer != null) 
		{
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				writer = null;
			}
		}
	}
	
}
