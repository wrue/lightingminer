package com.creditease.ns.miner.flume.sink.config.perform;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.Attributes;

import com.creditease.ns.collie.context.InterpretationContext;
import com.creditease.ns.collie.perform.Performer;
import com.creditease.ns.miner.flume.sink.config.RollingPolicy;
import com.creditease.ns.miner.flume.sink.config.Tracker;

public class RollingPolicyPerformer implements Performer{

	@Override
	public void begin(InterpretationContext ic, String name,
			Attributes attributes) throws Exception {
		Object o = ic.peekObject();
		if (!(o instanceof Tracker)) 
		{
			throw new Exception("必须为RollingPolicy指定对应的Tracker元素");
		}
		
		Tracker tracker = (Tracker) o;
		
		String className = attributes.getValue("class");
		if (StringUtils.isEmpty(className)) 
		{
			throw new Exception("必须为RollingPolicy指定一个class");
		}
		
		Class cl = Class.forName(className);
		RollingPolicy rollingPolicy = (RollingPolicy)cl.newInstance();
		
		String arc = attributes.getValue("arc");
		
		if (!StringUtils.isEmpty(arc)) 
		{
			rollingPolicy.setArcPattern(arc);
		}
	
		tracker.setRollingPolicy(rollingPolicy);
	}

	@Override
	public void body(InterpretationContext ic, String body) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void end(InterpretationContext ic, String name) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
