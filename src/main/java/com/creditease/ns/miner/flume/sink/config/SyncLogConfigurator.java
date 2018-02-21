package com.creditease.ns.miner.flume.sink.config;


import com.creditease.ns.collie.config.AbstractConfigurator;
import com.creditease.ns.collie.element.ElementSelector;
import com.creditease.ns.collie.specs.SpecStore;
import com.creditease.ns.miner.flume.sink.config.perform.RollingPolicyPerformer;
import com.creditease.ns.miner.flume.sink.config.perform.TrackerPerformer;

public class SyncLogConfigurator extends AbstractConfigurator{

	@Override
	public void buildSpec(SpecStore specStore) {
		specStore.addSpec(new ElementSelector("*/tracker"), new TrackerPerformer());
		specStore.addSpec(new ElementSelector("tracker/rollingPolicy"), new RollingPolicyPerformer());
	}

}
