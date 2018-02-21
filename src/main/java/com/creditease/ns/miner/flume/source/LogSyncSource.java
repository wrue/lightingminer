package com.creditease.ns.miner.flume.source;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creditease.ns.miner.flume.constant.LogWatchEvent;
import com.creditease.ns.miner.flume.handler.LogReadHandler;
import com.creditease.ns.miner.flume.help.DirWatcherDecider;
import com.creditease.ns.miner.flume.help.LogFileWatcherDecider;
import com.creditease.ns.miner.flume.help.ThreadHelp;
import com.creditease.ns.miner.flume.help.WatcherDecider;

public class LogSyncSource extends AbstractSource implements Configurable, EventDrivenSource {

	public static Logger logger = LoggerFactory.getLogger("WatcherSource");

	WatchService watcher = null;
	private Path watchedDir;
	private List<WatcherDecider> watcherDeciders = null;
	private List<WatcherDecider> dirWatchedDeciders = null;
	private String watchedFilePattern;
	private WatcherEventHandler eventHandler = new WatcherEventHandler();
	private ChannelProcessor channelProcessor = null;
	private String logId;
	private String watchedDirPattern;

	@Override
	public void start() {
		try {
			logger.info("LogSyncSource is starting");
			channelProcessor = getChannelProcessor();
			eventHandler.watchedDir = watchedDir;
			eventHandler.watcherDeciders = watcherDeciders;
			eventHandler.watchedFilePattern = watchedFilePattern;
			eventHandler.watcher = watcher;
			eventHandler.channelProcessor = channelProcessor;
			eventHandler.logId = this.logId;

			eventHandler.watchedDirPattern = watchedDirPattern;
			eventHandler.dirWatcherDeciders = dirWatchedDeciders;

			eventHandler.init();
			logger.debug("#EventHandler初始化完毕# {} {}", eventHandler.hashCode(), eventHandler.handlerCache);

			Thread logPeekThread = new Thread(eventHandler);
			logPeekThread.start();

			super.start();

			logger.info("LogSyncSource started");
		} catch (Exception e) {
			e.printStackTrace();
			channelProcessor.close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void stop() {
		eventHandler.stop();
		channelProcessor.close();
	}

	@Override
	public void configure(Context context) {
		logger.debug("#开始配置LogSyncSource...#");

		logId = context.getString("logId");

		if (StringUtils.isEmpty(logId)) {
			throw new RuntimeException("请为当前日志配置一个LogId");
		}

		String dir = context.getString("watchedDir");

		if (StringUtils.isEmpty(dir)) {
			throw new RuntimeException("请配置被观察的目录");
		}

		watchedDir = Paths.get(dir);

		if (watcher == null) {
			try {
				watcher = FileSystems.getDefault().newWatchService();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		watchedDirPattern = context.getString("watchedDirPattern");

		if ((dirWatchedDeciders == null) && !StringUtils.isEmpty(watchedDirPattern)) {
			dirWatchedDeciders = new ArrayList<WatcherDecider>();
			String[] filePatterns = watchedDirPattern.split(",");
			for (String filePattern : filePatterns) {
				DirWatcherDecider fileWatcherDecider = new DirWatcherDecider();
				fileWatcherDecider.setDirNamePattern(filePattern);
				dirWatchedDeciders.add(fileWatcherDecider);
			}
		}

		watchedFilePattern = context.getString("watchedFilePattern");

		if (watcherDeciders == null) {
			watcherDeciders = new ArrayList<WatcherDecider>();
			int dateRange = context.getInteger("restartCoverDay", 1);
			if (!StringUtils.isEmpty(watchedFilePattern)) {
				String[] filePatterns = watchedFilePattern.split(",");
				for (String filePattern : filePatterns) {
					LogFileWatcherDecider fileWatcherDecider = new LogFileWatcherDecider();
					fileWatcherDecider.setDateRange(dateRange);
					fileWatcherDecider.setLogFilePattern(filePattern);
					watcherDeciders.add(fileWatcherDecider);
				}
			} else {
				// 没有pattern 就添加一个decide即可
				LogFileWatcherDecider fileWatcherDecider = new LogFileWatcherDecider();
				fileWatcherDecider.setDateRange(dateRange);
				fileWatcherDecider.setLogFilePattern(null);
				watcherDeciders.add(fileWatcherDecider);
			}

		}

		logger.debug("#配置LogSyncSource完毕#");
	}

	static class WatcherEventHandler implements Runnable {
		public WatchService watcher = null;
		public Path watchedDir;
		public Map<WatchKey, Path> keys;
		public ExecutorService handlerExecutor = Executors
				.newCachedThreadPool(new ThreadHelp.CustomThreadNameThreadFactory("hand"));
		public List<WatcherDecider> watcherDeciders = null;
		public String watchedFilePattern;

		private volatile boolean run = true;
		public ChannelProcessor channelProcessor = null;

		private ConcurrentHashMap<String, Path> exceptionPaths = new ConcurrentHashMap<String, Path>();
		private ConcurrentHashMap<String, LogReadHandler> handlerCache = new ConcurrentHashMap<String, LogReadHandler>();
		private ScheduledExecutorService cleanExecutor = Executors.newSingleThreadScheduledExecutor();
		boolean inited;
		public String logId;

		public String watchedDirPattern;
		public List<WatcherDecider> dirWatcherDeciders = null;

		public void init() throws Exception {
			if (!inited) {
				if (keys == null) {
					keys = new ConcurrentHashMap<WatchKey, Path>();
				}
				registerAll(watchedDir);

				CleanHandler cleanHandler = new CleanHandler();
				cleanHandler.handlerCache = handlerCache;
				cleanExecutor.scheduleWithFixedDelay(cleanHandler, 1000l, 30 * 60 * 1000l, TimeUnit.MILLISECONDS);

				inited = true;
				logger.debug("#WatcherEventHandler初始化成功# {}", this.handlerCache);
			}
		}

		@Override
		public void run() {
			logger.debug("#eventhandler开始运行 {} {}#", this.hashCode(), this.handlerCache);
			while (!Thread.interrupted() && run) {
				WatchKey key;

				try {
					// wait for a key to be available
					key = watcher.take();

					Path dir = keys.get(key);

					if (dir == null) {
						dir = (Path) key.watchable();
					}

					for (WatchEvent<?> event : key.pollEvents()) {
						// 取得对应的事件处理器
						Path path = (Path) event.context();
						path = dir.resolve(path);

						Kind<?> kind = event.kind();

						if (kind == OVERFLOW) {
							logger.warn("#出现OVERFLOW事件，表明事件出现丢失#");
							continue;
						}

						if (path != null) {
							if (!path.toFile().exists()) {
								continue;
							}

							if (path.toFile().isDirectory()) {
								String fullPath = path.toFile().getAbsolutePath();

								if (fullPath == null) {
									continue;
								}

								if (kind == ENTRY_CREATE) {
									try {
										registerAll(path);
									} catch (Exception e) {
										// 当出现异常的时候 我们将path 放入一个待处理的map中，然后随后尝试注册，如果注册失败，就不移除，
										// 注册成功则移除
										logger.error("注册目录出现异常,需要重新注册 {}", fullPath, e);
										exceptionPaths.put(fullPath, path);
									}
									logger.debug("#运行期间发现了目录创建，添加到观察列表当中 {} {}#", path.toString(), exceptionPaths);
								} else {
									Path path2 = exceptionPaths.get(fullPath);

									if (path2 != null) {
										// 证明有出错的地方
										// 我们尝试重新注册
										try {
											registerAll(path2);
											exceptionPaths.remove(fullPath);
										} catch (Exception e) {
											// 当出现异常的时候 我们将path 放入一个待处理的map中，然后随后尝试注册，如果注册失败，就不移除，
											// 注册成功则移除
											logger.error("注册目录出现异常,等待重新注册 {} {}", fullPath, exceptionPaths, e);
										}
									}

								}
								continue;
							}

							// 可能创建了一些临时文件
							File f = path.toFile();
							String filePath = f.getCanonicalPath();
							String realPath = f.getAbsolutePath();
							LogReadHandler handler = this.handlerCache.get(filePath);
							if (handler == null) {
								if (kind == ENTRY_CREATE) {
									try {
										registerNewFile(path, false);
										handler = this.handlerCache.get(filePath);
									} catch (Exception e) {
										logger.error("注册文件失败 {}", filePath, e);
									}
								} else if (kind == ENTRY_DELETE) {
									// 暂时什么都不做
									logger.debug("#文件被删除了# {} {} {}", filePath, f.isDirectory(), realPath);
									continue;
								} else {
									logger.warn("#出现逻辑错误，当出现非创建事件时，缓存中确无对应的Entry,可能是注册WatchKey时有问题# {} {} {} {}", filePath, f
											.isDirectory(), realPath, handlerCache);

									boolean isMatch = false;

									isMatch = decideFile(f, watcherDeciders);

									if (!isMatch) {
										logger.info("#排除文件# {} {}", filePath, realPath);
										continue;
									}

									handler = this.handlerCache.get(filePath);
									if (handler == null) {
										logger.warn("#没有找到对应的handler, 重新注册此文件的handler# {} {}", filePath, realPath);
										registerNewFile(path, true);
									}
									continue;
								}
							}
							if (handler != null) {
								handler.putEventQueue(event);
							}
						} else {
							logger.warn("#文件不存在，却发出了对应的事件 {}#", event.kind().toString());
						}

					}

					boolean valid = key.reset();
					if (!valid) {
						logger.warn("#WatchKey Reset操作出现问题# {} {}", valid, key.toString());
						continue;
					}

				} catch (InterruptedException ex) {
					continue;
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}
		}

		private void register(Path dir) throws IOException {
			WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
			logger.debug("#注册监听目录# {} {}", dir.toFile().getCanonicalPath(), dir.toFile().getAbsolutePath());
		}

		private void registerAll(final Path start) throws Exception {
			Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
					try {
						if (!dir.toFile().isDirectory()) {
							return FileVisitResult.CONTINUE;
						}

						if (!StringUtils.isEmpty(watchedDirPattern) && (dirWatcherDeciders != null)) {
							boolean isMatch = false;

							isMatch = decideFile(dir.toFile(), dirWatcherDeciders);

							if (isMatch) {
								register(dir);
							} else {
								logger.info("#排除目录文件# {} {}", dir.toFile().getCanonicalPath(), watchedDirPattern);
							}
						} else {
							logger.debug("#没有配置目录过滤，指定父目录下的所有目录都需要监听#");
							register(dir);
						}
					} catch (Exception e) {
						logger.error("#读取目录失败 {}#", dir, e);
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

					final Path path = file;
					try {
						registerNewFile(file, true);
					} catch (Exception e) {
						logger.error("#读取文件失败 {}#", file, e);
					}

					return FileVisitResult.CONTINUE;
				}

			});
		}

		protected void registerNewFile(Path file, boolean isPutinCreateEvent) throws Exception {
			File f = file.toFile();
			String logRealPath = f.getCanonicalPath();
			String realPath = f.getAbsolutePath();
			boolean isMatch = false;

			isMatch = decideFile(f, watcherDeciders);

			// 文件名是否匹配
			if (isMatch) {
				if (!StringUtils.isEmpty(watchedDirPattern) && (dirWatcherDeciders != null)) {
					// 文件名符合，还需要判断目录是否符合
					File parentPath = f.getParentFile();

					if (parentPath == null) {
						logger.info("#发现文件名符合，但是目录不符合规则的文件# {}", logRealPath);
						isMatch = false;
					} else {
						boolean dirMatch = false;
						dirMatch = decideFile(parentPath, dirWatcherDeciders);

						if (!dirMatch) {
							logger.info("#发现不符合目录规则文件# {} {} {} {}", logRealPath, parentPath, watchedDirPattern, parentPath
									.isDirectory());
						}

						isMatch = (dirMatch && isMatch);
					}
				}

			}

			// 加目录判断是否匹配
			if (isMatch) {
				LogReadHandler logReadHandler = handlerCache.get(logRealPath);

				final Path path = file;

				if (logReadHandler == null) {
					logReadHandler = handleNewFile(file, f, logRealPath, realPath);

					if (isPutinCreateEvent) {
						logReadHandler.putEventQueue(new WatchEvent<Path>() {

							@Override
							public java.nio.file.WatchEvent.Kind<Path> kind() {
								return LogWatchEvent.AGENTRESTARTEVENT;
							}

							@Override
							public int count() {
								return 1;
							}

							@Override
							public Path context() {
								return path;
							}
						});
					}

					handlerCache.put(logRealPath, logReadHandler);
					logger.debug("#注册handler成功# {} {} {}", logRealPath, realPath, handlerCache);
				} else {
					logger.debug("#发现logHandler已经存在，可能是编程逻辑错误# {} {}", logRealPath, realPath);
				}
			} else {
				logger.debug("#排除文件# {} {} {}", logRealPath, realPath, watchedFilePattern);
			}
		}

		private boolean decideFile(File parentPath, List<WatcherDecider> watcherDeciders) {
			boolean isMatch = false;
			for (Iterator dirIterator = watcherDeciders.iterator(); dirIterator.hasNext();) {
				WatcherDecider dirWatcherDecider = (WatcherDecider) dirIterator.next();
				isMatch = dirWatcherDecider.decide(parentPath);
				if (isMatch) {
					break;
				}
			}
			return isMatch;
		}

		protected LogReadHandler handleNewFile(Path file, File f, String logRealPath, String realPath) throws Exception {
			LogReadHandler logReadHandler = new LogReadHandler();
			logReadHandler.setLogPath(file);
			logReadHandler.setLogRealPath(logRealPath);
			logReadHandler.setLogFile(f);
			logReadHandler.setChannelProcessor(channelProcessor);
			logReadHandler.setLogId(logId);
			try {
				logReadHandler.startHandle(handlerExecutor);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("#启动新的logHandler失败# {} {}", logRealPath, realPath, e);
				throw e;
			}

			logger.info("#启动新的logHandler# {} {}", logRealPath, realPath);
			return logReadHandler;
		}

		public void stop() {
			logger.info("#正在结束WatcherSource线程...#");
			this.run = false;
			handlerExecutor.shutdownNow();
			for (Iterator iterator = handlerCache.keySet().iterator(); iterator.hasNext();) {
				String path = (String) iterator.next();
				LogReadHandler logReadHandler = handlerCache.get(path);
				if (logReadHandler != null) {
					logReadHandler.shutdownNow();
				}

				iterator.remove();
			}

			logger.info("#结束WatcherSource线程#");
		}

	}

	static class CleanHandler implements Runnable {
		public volatile boolean stop = false;
		public ConcurrentHashMap<String, LogReadHandler> handlerCache;

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
							LogReadHandler logHandler = handlerCache.get(key);

							if (logHandler == null) {
								logger.debug("#没有获取到loghandler,准备移除 {}#", key);
								iterator.remove();
								continue;
							}

							long now = System.currentTimeMillis();

							if ((now - logHandler.getStartTime()) >= LogReadHandler.LIFETIME) {
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

				} else {
					logger.warn("#没有设置handlerCache,清理线程退出#");
					Thread.currentThread().interrupt();
					return;
				}

			}

			logger.debug("#handler清理线程结束执行#");
		}
	}

	public String getLogId() {
		return logId;
	}

	public void setLogId(String logId) {
		this.logId = logId;
	}
}
