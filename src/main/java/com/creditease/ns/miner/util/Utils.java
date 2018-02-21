package com.creditease.ns.miner.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
	//获取当前的机器的ip
	public static String getIp()
	{
		String ip = "unknownhost";

		if (isWindowsOS()) 
		{
			InetAddress ia = null;
			try {
				//如果是Windows操作系统
				ia = InetAddress.getLocalHost();
				if (ia != null) 
				{
					ip = ia.getHostAddress();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}

		}
		else
		{
			try {
				Enumeration<NetworkInterface>  networkInterfaces = NetworkInterface.getNetworkInterfaces();
				boolean isFindIp = false;
				String s = "e\\w+0";
				Pattern p = Pattern.compile(s);

				while (networkInterfaces.hasMoreElements()) {  
					NetworkInterface item = networkInterfaces.nextElement(); 
					String devName = item.getDisplayName();
					if(devName == null)
					{
						continue;
					}
					Matcher m = p.matcher(devName);
					boolean isNeedDev = m.matches();

					if(!item.isLoopback() && isNeedDev)
					{
						for (InterfaceAddress address : item.getInterfaceAddresses()) {   
							if (address.getAddress() instanceof Inet4Address) {   
								Inet4Address inet4Address = (Inet4Address) address.getAddress();   
								if (!inet4Address.isLoopbackAddress() && !inet4Address.isMulticastAddress()) 
								{
									ip = inet4Address.getHostAddress();
									break;

								}
							}   
						}   
					}

					if (isFindIp) 
					{
						break;
					}

				}
			} catch (SocketException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		return ip;
	}

	public static boolean isWindowsOS(){
		boolean isWindowsOS = false;
		String osName = System.getProperty("os.name");
		if (osName != null) 
		{
			if(osName.toLowerCase().indexOf("windows")>-1){
				isWindowsOS = true;
			}
		}
			
		return isWindowsOS;
	}



	public static int getTotalLines(String fileName) throws Exception {
		File f = new File(fileName);
		if (!f.exists()) 
		{
			return 0;
		}
		int totalLines = 0;
		FileInputStream fileInputStream = new FileInputStream(f);
		InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream,"utf-8");
		LineNumberReader reader = new LineNumberReader(inputStreamReader);
		try
		{
			String strLine = reader.readLine();

			while (strLine != null) {
				totalLines++;
				strLine = reader.readLine();
			}
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			reader.close();
		}

		return totalLines;
	}
	
	
	public static boolean doMatch(char[] pattern,int pOffset,char[] name,int uOffset)
	{
		if (pOffset >= pattern.length && uOffset >= name.length) 
		{
			return true;
		}
		
		if(pOffset < pattern.length && uOffset < name.length)
		{
			if(pattern[pOffset] == name[uOffset])
				return doMatch(pattern, pOffset+1, name, uOffset+1);
		}
		
		if(pOffset < pattern.length)
		{
			if(pattern[pOffset] == '*' && (pOffset+1) < pattern.length && uOffset >= name.length)
			{
				return false;
			}
			
			if (pattern[pOffset] == '*') 
			{
				return doMatch(pattern, pOffset+1, name, uOffset) || doMatch(pattern, pOffset, name, uOffset+1);
			}
		}
		
		return false;
	}
}
