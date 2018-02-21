package com.creditease.ns.miner.flume.handler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;

import com.creditease.ns.miner.flume.constant.CommonConstants;
import com.creditease.ns.miner.flume.source.LogSyncSource;
import com.creditease.ns.miner.util.Utils;

public class LogReadHandler implements Runnable {

	private Path logPath;
	private volatile boolean stop;
	private BlockingQueue<WatchEvent<?>> eventQueue = new LinkedBlockingQueue<WatchEvent<?>>();
	private LineNumberReader reader = null;
	private String logRealPath = null;
	private File logFile;
	private long startTime = System.currentTimeMillis();
	public static long LIFETIME = 30 * 3600 * 1000l;
	private Logger logger = LogSyncSource.logger;
	private long curOffset = 0;
	private int readBatchSize = 10;
	private ChannelProcessor channelProcessor;
	private Map<String, String> eventHeaders = new HashMap<String, String>();
	public static int MAX_RETRY_TIMES = 3;
	public static int MAX_BATCH_LINE_NUM = 10000; // 批量读取改小一些，避免OOM
	private long startLineNum = 1;
	private long endLineNum = startLineNum;
	private String logId;

	@Override
	public void run() {
		while (!Thread.interrupted() && !stop) {
			WatchEvent<?> event = null;
			try {
				event = eventQueue.take();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				return;
			}
			if (event != null) {
				readContent();
			}
		}
	}

	public void startHandle(Executor executor) throws Exception {
		try {
			reader = new LineNumberReader(new InputStreamReader(new FileInputStream(logFile), "utf-8"));
			eventHeaders.put(CommonConstants.EVENT_HEADERS_WHOLEPATH_KEY, this.logRealPath);
			eventHeaders.put(CommonConstants.EVENT_HEADERS_FILENAME_KEY, this.logFile.getName());
			eventHeaders.put(CommonConstants.EVENT_HEADERS_LOGID_KEY, this.logId);
			String ip = Utils.getIp();
			eventHeaders.put(CommonConstants.EVENT_HEADERS_IP_KEY, ip);
			logger.error("#启动handler线程成功# {} {}", logPath.toString(), logId);
		} catch (FileNotFoundException e) {
			logger.error("#启动handler线程失败,没有发现文件# {} {}", logPath.toString(), logId, e);
			shutdownNow();
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

	public void putEventQueue(WatchEvent<?> event) {
		eventQueue.add(event);
	}

	// 可以加快读取速度 每次读取一个块儿
	private void readContent() {
		try {
			String line = null;
			StringBuilder lineBlock = new StringBuilder();
			long lineNum = 0;
			while ((line = reader.readLine()) != null) {

				lineNum++;
				lineBlock.append(line).append("\n");

				if ((lineNum % MAX_BATCH_LINE_NUM) == 0) {
					lineBlock = processEvent(lineBlock);
				}
			}

			if (lineBlock.length() > 0) {
				lineBlock = processEvent(lineBlock);
			}

		} catch (Exception e) {
			e.printStackTrace();
			// 事件处理出现问题了
		}

	}

	private StringBuilder processEvent(StringBuilder lineBlock) throws Exception {
		Map<String, String> headers = copyMap(eventHeaders);
		endLineNum = reader.getLineNumber();
		headers.put(CommonConstants.EVENT_HEADERS_LINERANGE_KEY, startLineNum + CommonConstants.LINERANGE_SEPERATOR
				+ endLineNum);
		Event event = EventBuilder.withBody(lineBlock.toString(), Charset.forName("utf-8"), headers);
		startLineNum = endLineNum + 1;
		try {
			// 写入channel
			channelProcessor.processEvent(event);
			return new StringBuilder();
		} catch (Exception e) {
			// 处理事件出现问题，为了程序健壮性，我们会恢复reader的位置到读取前的位置，重新读取
			boolean processSuc = false;
			for (int i = 0; i < MAX_RETRY_TIMES; i++) {
				try {
					logger.error("#尝试重新处理事件# {} {} {}", i, headers, logId);
					channelProcessor.processEvent(event);
					processSuc = true;
					break;
				} catch (Exception e1) {
					Thread.sleep(100l);
				}
			}
			if (!processSuc) {
				logger.error("#尝试重新处理事件次数达到最大限制,停止重试# {} {}", headers, logId);
				throw e;
			}

			return new StringBuilder();
		}
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public BlockingQueue<WatchEvent<?>> getEventQueue() {
		return eventQueue;
	}

	public void setEventQueue(BlockingQueue<WatchEvent<?>> eventQueue) {
		this.eventQueue = eventQueue;
	}

	public Logger getLogger() {
		return logger;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public long getCurOffset() {
		return curOffset;
	}

	public void setCurOffset(long curOffset) {
		this.curOffset = curOffset;
	}

	public int getReadBatchSize() {
		return readBatchSize;
	}

	public void setReadBatchSize(int readBatchSize) {
		this.readBatchSize = readBatchSize;
	}

	public ChannelProcessor getChannelProcessor() {
		return channelProcessor;
	}

	public void setChannelProcessor(ChannelProcessor channelProcessor) {
		this.channelProcessor = channelProcessor;
	}

	public void removeSelf(Map<String, LogReadHandler> handlerCache) {
		if (handlerCache != null) {
			handlerCache.remove(this.logRealPath);
		}
	}

	public Map<String, String> copyMap(Map<String, String> srcMap) {
		Map<String, String> dstMap = new HashMap<String, String>();
		for (Iterator iterator = srcMap.entrySet().iterator(); iterator.hasNext();) {
			Entry<String, String> entry = (Entry<String, String>) iterator.next();
			dstMap.put(entry.getKey(), entry.getValue());
		}

		return dstMap;
	}

	public void shutdownNow() {
		this.stop = true;
		this.eventQueue.clear();
		if (this.reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				reader = null;
			}
		}
	}

	public String getLogId() {
		return logId;
	}

	public void setLogId(String logId) {
		this.logId = logId;
	}

}
