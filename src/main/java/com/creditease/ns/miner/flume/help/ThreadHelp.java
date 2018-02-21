package com.creditease.ns.miner.flume.help;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.creditease.ns.miner.flume.handler.LogReadHandler;
import com.creditease.ns.miner.flume.handler.LogWriteHandler;

public class ThreadHelp {

	public static class CustomThreadNameThreadFactory implements ThreadFactory {
		static final AtomicInteger poolNumber = new AtomicInteger(1);
		final ThreadGroup group;
		final AtomicInteger threadNumber = new AtomicInteger(1);
		final String namePrefix;

		public CustomThreadNameThreadFactory(String threadNamePrefix) {
			SecurityManager s = System.getSecurityManager();
			group = (s != null)? s.getThreadGroup() :
				Thread.currentThread().getThreadGroup();
			namePrefix = threadNamePrefix + "-"+"-"+
					poolNumber.getAndIncrement() +
					"-t-";
		}

		public Thread newThread(Runnable r) {
			Thread t = new Thread(group, r,
					namePrefix + threadNumber.getAndIncrement(),
					0);
//			
//			if (r instanceof LogReadHandler) 
//			{
//				LogReadHandler logReadHandler = (LogReadHandler)r;
//				t.setName(t.getName() + "-" + (logReadHandler.getLogPath() != null ? logReadHandler.getLogPath().getFileName() : ""));
//			}
//			else if (r instanceof LogWriteHandler) 
//			{
//				LogWriteHandler logWriteHandler = (LogWriteHandler)r;
//				t.setName(t.getName() + "-" + (logWriteHandler.getLogPath() != null ? logWriteHandler.getLogPath().getFileName() : ""));
//			} 
//			
			t.setUncaughtExceptionHandler(new ThreadUncaughtExceptionHandler());
			if (t.isDaemon())
				t.setDaemon(false);
			if (t.getPriority() != Thread.NORM_PRIORITY)
				t.setPriority(Thread.NORM_PRIORITY);
			return t;
		}
	}

	public static class ThreadUncaughtExceptionHandler implements UncaughtExceptionHandler {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			System.out.println("[线程可能出错] [当前运行的线程:"+Thread.currentThread().getName()+"] [出错线程:"+t.getName()+"]");
			e.printStackTrace();
		}
	}
	
}
