package lightingminer;

import com.creditease.ns.miner.util.Utils;

public class TestMatcher {
	public static void main(String[] args) 
	{
		String pattern = "/data/crawl/log/*";
		
		System.out.println(pattern);
		
		boolean isMatch = Utils.doMatch(pattern.toCharArray(), 0, "/data/crawl/log".toCharArray(), 0);
		
		System.out.println("ismatch:"+isMatch);
		
	}
}
