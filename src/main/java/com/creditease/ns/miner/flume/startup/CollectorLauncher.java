package com.creditease.ns.miner.flume.startup;

import java.net.URL;

import org.apache.flume.node.Application;

public class CollectorLauncher {
	public static void main(String[] args) {

		String destConfig = "logdest.conf";

		if ((args != null) && (args.length >= 1)) {
			if ("kafka".equals(args[0])) {
				destConfig = "logdestkafka.conf";
			}
		}

		URL url = Thread.currentThread().getContextClassLoader().getResource(destConfig);
		String confPath = url.getFile();
		String[] argms = new String[] { "--conf-file", confPath, "--name", "logdstagent" };

		Application.main(argms);
	}
}
