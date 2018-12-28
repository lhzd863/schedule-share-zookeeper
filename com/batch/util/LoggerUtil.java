package com.batch.util;
import ch.qos.logback.core.PropertyDefinerBase;

public class LoggerUtil extends PropertyDefinerBase{

	public static String logPath;
	public static String logFile;

	@Override
	public String getPropertyValue() {
		return getLogFileName();
	}
	
	public static void setLogFileName(String path,String file){
		logPath = path;
		logFile = file;
	}
	public static String getLogFileName(){
		return logPath+"/"+logFile;
	}
}
