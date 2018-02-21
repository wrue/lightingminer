package com.creditease.ns.miner.flume.sink;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.ns.miner.flume.constant.CommonConstants;
import com.creditease.ns.miner.flume.handler.LogReadHandler;
import com.creditease.ns.miner.flume.handler.LogWriteHandler;
import com.creditease.ns.miner.flume.help.ThreadHelp;
import com.creditease.ns.miner.flume.sink.config.ConfigContext;
import com.creditease.ns.miner.flume.sink.config.SyncLogConfigurator;
import com.creditease.ns.miner.flume.sink.config.Tracker;

public class LogSyncSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger("logSyncSink");
	private String logCollectionDir;
	private static final int defaultBatchSize = 100;
	private int batchSize = defaultBatchSize;
	private String logConf;
	private Map<String, LogWriteHandler> fileWriters = new ConcurrentHashMap<String, LogWriteHandler>();

	private ExecutorService handlerExecutor = Executors.newCachedThreadPool(new ThreadHelp.CustomThreadNameThreadFactory(
			"hand"));
	private ScheduledExecutorService cleanExecutor = Executors.newSingleThreadScheduledExecutor();

	private ConfigContext configContext = ConfigContext.getInstance();

	public static long RECOVER_LAST_FILE_TIME_RANGE = 8 * 60 * 60 * 1000l;

	@Override
	public Status process() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		Status result = Status.READY;

		try {
			transaction.begin();
			for (int i = 0; i < batchSize; i++) {
				event = channel.take();
				if (event != null) {
					Map<String, String> headers = event.getHeaders();
					String ip = headers.get(CommonConstants.EVENT_HEADERS_IP_KEY);
					String filename = headers.get(CommonConstants.EVENT_HEADERS_WHOLEPATH_KEY);
					String logId = headers.get(CommonConstants.EVENT_HEADERS_LOGID_KEY);
					Tracker tracker = configContext.getTracker(logId);

					if (tracker == null) {
						logger.error("出现错误，没有找到匹配的Tracker {} {}", logId, configContext.getTrackerCache());
					}

					String wholePath = tracker.getRealStoreFileName(ip, filename);
					String wholeDir = wholePath.substring(0, wholePath.lastIndexOf("/"));

					File logDir = new File(wholeDir);

					if (!logDir.exists()) {
						logDir.mkdirs();
					}

					File f = new File(wholePath);

					LogWriteHandler writer = fileWriters.get(wholePath);
					if (writer == null) {
						writer = new LogWriteHandler();
						writer.setLogPath(Paths.get(wholePath));
						writer.setLogRealPath(wholePath);
						writer.setLogFile(f);
						writer.startHandle(handlerExecutor);
						fileWriters.put(wholePath, writer);
					} else {
						if (writer.isStop()) {
							fileWriters.remove(wholePath);
						}
					}

					writer.putEventQueue(event);
					logger.debug("#放入传入的Event# {}", headers);

				} else {
					// No events found, request back-off semantics from runner
					logger.debug("#没有待处理的事件# {}", batchSize);
					result = Status.BACKOFF;
					break;
				}
			}
			transaction.commit();
			logger.debug("#事务提交成功#");
		} catch (Exception ex) {
			transaction.rollback();
			throw new EventDeliveryException("Failed to process transaction", ex);
		} finally {
			transaction.close();
		}

		return result;
	}

	@Override
	public synchronized void start() {

		logger.info("Starting {}...", this);

		CleanHandler cleanHandler = new CleanHandler();
		cleanHandler.handlerCache = (ConcurrentHashMap<String, LogWriteHandler>) fileWriters;
		cleanExecutor.scheduleWithFixedDelay(cleanHandler, 1000l, 30 * 60 * 1000l, TimeUnit.MILLISECONDS);

		super.start();

		logger.info("LogSyncSink {} started.", getName());
	}

	@Override
	public synchronized void stop() {
		logger.info("Stopping {}...", this);
		handlerExecutor.shutdownNow();
		for (Iterator iterator = fileWriters.keySet().iterator(); iterator.hasNext();) {
			String path = (String) iterator.next();
			LogWriteHandler logWriteHandler = fileWriters.get(path);
			if (logWriteHandler != null) {
				logWriteHandler.shutdownNow();
			}
			iterator.remove();

		}
		super.stop();

		logger.info("LogSyncSink {} stoped", getName());
	}

	@Override
	public void configure(Context context) {

		
		// logCollectionDir = context.getString("logDir");
		// if (StringUtils.isEmpty(logCollectionDir))
		// {
		// throw new RuntimeException("必须指定日志归集目录");
		// }

		this.batchSize = context.getInteger("batchSize", 100);

		this.logConf = context.getString("logConf");
		if (StringUtils.isEmpty(logConf)) {
			throw new RuntimeException("必须指定日志配置文件路径");
		}

		loadConf();
		logger.info("#读取配置文件完毕#");
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	private void loadConf() {
		// 解析loadConf xml形式
		URL url = Thread.currentThread().getContextClassLoader().getResource(this.logConf);
		SyncLogConfigurator syncLogConfigurator = new SyncLogConfigurator();
		try {
			syncLogConfigurator.doConfig(url.openStream());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	static class CleanHandler implements Runnable {
		public volatile boolean stop = false;
		public ConcurrentHashMap<String, LogWriteHandler> handlerCache;

		@Override
		public void run() {
			logger.debug("#handler清理线程开始执行#");
			// 这里从while改成了if 因为我们自己用的线程做无限循环定时sleep时 发现线程总是没有执行，故改成这样
			// 具体原因还需要进一步分析 很有可能是executor本身内部调用无限循环的线程工作出现的问题
			if (!Thread.interrupted() && !stop) {
				if (handlerCache != null) {
					// 迭代整个cache如果出现超时，则停止这个handler

					Set<String> filePaths = handlerCache.keySet();
					if (filePaths != null) {
						Iterator<String> iterator = filePaths.iterator();
						while (iterator.hasNext()) {
							String key = iterator.next();
							LogWriteHandler logHandler = handlerCache.get(key);

							if (logHandler == null) {
								logger.debug("#没有获取到loghandler {}#", key);
								iterator.remove();
								continue;
							}

							long now = System.currentTimeMillis();

							if ((now - logHandler.getStartTime()) >= LogWriteHandler.LIFETIME) {
								try {
									logHandler.shutdownNow();
								} catch (Exception e) {
									e.printStackTrace();
								} finally {
									iterator.remove();
									logger.debug("#移除过期handler {}#", logHandler.getLogPath().toString());
								}

							} else {
								logger.debug("#当前handler没有过期，暂时不移除# {} {} {} {}", now, logHandler.getStartTime(), (now - logHandler
										.getStartTime()), LogReadHandler.LIFETIME);
							}

						}
					}

				}

			}

			logger.debug("#handler清理线程结束执行#");
		}
	}

	public String getLogConf() {
		return logConf;
	}

	public void setLogConf(String logConf) {
		this.logConf = logConf;
	}

}
