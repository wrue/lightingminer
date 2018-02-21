package com.creditease.ns.miner.flume.sink.config;

/**
 * 这个类中会分析传入的文件名来确定真正存储到路径时候需要保留多少层路径 从最后一个文件分隔符开始数
 * @author liuyang
 *2016年8月15日下午6:03:17
 */
public class Tracker {
	
	private String name;
	private String storePath;
	private RollingPolicy rollingPolicy;
	private String realPath;
	private String storeFileName;
	private int reservePathLevel = -1;
	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStorePath() {
		return storePath;
	}

	public void setStorePath(String storePath) {
		this.storePath = storePath;
	}

	public RollingPolicy getRollingPolicy() {
		return rollingPolicy;
	}

	public void setRollingPolicy(RollingPolicy rollingPolicy) {
		this.rollingPolicy = rollingPolicy;
	}

	public String getRealPath() {
		return realPath;
	}

	public void setRealPath(String realPath) {
		this.realPath = realPath;
	}

	public String getStoreFileName() {
		return storeFileName;
	}

	public void setStoreFileName(String storeFileName) {
		this.storeFileName = storeFileName;
	}
	
	public String getRealStoreFileName(String ip,String inputFileName)
	{
		//解析storePath 得到真正的要存储的文件名
		if (reservePathLevel == -1) 
		{
			//没有解析storepath
			int pos = this.storePath.indexOf("%");
			if (pos > 0) 
			{
				realPath = this.storePath.substring(0, pos);
				reservePathLevel = Integer.parseInt(this.storePath.trim().substring(pos+1));
			}
			else
			{
				realPath = storePath;
				reservePathLevel = 1;
			}
		}
		
		String[] fileSeg = inputFileName.split("[\\\\/]");
		StringBuilder fileName = new StringBuilder();
		if (fileSeg.length > reservePathLevel) 
		{
			for (int i = fileSeg.length-reservePathLevel; i < fileSeg.length; i++) 
			{
				fileName.append(fileSeg[i]);
				fileName.append("/");
			}
			
			return realPath + "/"+ip+"/"+fileName.substring(0, fileName.length()-1);
		}
		else
		{
			return realPath+"/"+ip+"/"+inputFileName;
		}
	}

	public int getReservePathLevel() {
		return reservePathLevel;
	}

	public void setReservePathLevel(int reservePathLevel) {
		this.reservePathLevel = reservePathLevel;
	}
}
