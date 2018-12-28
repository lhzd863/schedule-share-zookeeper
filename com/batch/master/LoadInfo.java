package com.batch.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.util.ClearLog;
import com.batch.util.Configuration;
import com.batch.util.CronExpression;
import com.batch.util.UtilMth;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class LoadInfo implements Runnable{

	Configuration conf = null;
	Map shareM = null;
	Logger log = null;
	String cfg = "";
	final static String clss = "loadinfo";
	
	Map jobconfMaping = null;
	Map jobTriggerMaping = null;
	Map jobStreamMapping = null;
	Map jobDependencyMaping = null;
	Map jobStepMaping = null;
	Map jobTimewindowMaping = null;
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobconfMaping = (Map) this.shareM.get(StrVal.MAP_KEY_JOB);
    	this.jobTriggerMaping = (Map) this.shareM.get(StrVal.MAP_KEY_TRIGGER);
    	this.jobStepMaping = (Map) this.shareM.get(StrVal.MAP_KEY_STEP);
	}
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		boolean flag = true;
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat timestamp = new SimpleDateFormat("yyyyMMddHHmmss");
			
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
//			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			if(log==null){
				LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
				log = LoggerFactory.getLogger(LoadInfo.class);
			}
			//refresh config
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			long subvalconf = Long.parseLong(conf.get(StrVal.BATCH_TIME_REFRESH));
			//init conf
			if(subvalconf<=subval){
				log.info(clss+" refresh config.");
				conf.initialize(new File(cfg));
				DefaultConf.initialize(conf);
				starttime = endtime;
				flag = true;
			}
			if(flag){
				flag = false;
				if(conf.get(StrVal.BATCH_TYPE_CONFIG).equals("file")){
//					confFromFile(conf.get(StrVal.BATCH_PATH_FILE));
				}else if(conf.get(StrVal.BATCH_TYPE_CONFIG).equals("database")){
					Connection conn = UtilMth.getConnectionDB(conf);
					//job info
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
		                log.info(conf.get(StrVal.SQL_SHD_JOB_INFO)+" will executer.");
					}
					confFromDB(conn,conf.get(StrVal.SQL_SHD_JOB_INFO));
					//trigger
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
		                log.info(conf.get(StrVal.SQL_SHD_JOB_TRIGGER)+" will executer.");
					}
					triggerFromDB(conn,conf.get(StrVal.SQL_SHD_JOB_TRIGGER));
					//step
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
		                log.info(conf.get(StrVal.SQL_SHD_JOB_STEP)+" will executer.");
					}
					stepFromDB(conn,conf.get(StrVal.SQL_SHD_JOB_STEP));
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
			
			//log.info("*********************** Sleep ["+conf.get(StrVal.SYS_HEARTBEADT_TIME)+"] ***********************.");
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.BATCH_TIME_HEARTBEADT)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


//	public void confFromFile(String filenm){
//		String result = null;
//		FileReader fileReader = null;
//		BufferedReader bufferedReader = null;
//		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
//			log.info("load job info from file "+filenm+".");
//		}
//		File file = new File(filenm);
//		if(file.exists()&&file.isFile()){
//	      try {	
//			 String read = null;
//			 fileReader = new FileReader(file);
//			 bufferedReader = new BufferedReader(fileReader);
//			 while((read=bufferedReader.readLine())!=null){
//				if(read.trim().length()==0){
//					continue;
//				}
//				if(read.trim().startsWith("#")){
//					continue;
//				}
//				String[] keyval = read.split(conf.get(StrVal.DELIMITER_FILE_CONFIG));
//				String pool = keyval[0].trim().toUpperCase();
//				String system = keyval[1].trim().toUpperCase();
//				String job = keyval[2].trim().toUpperCase();
//				String nodeName = keyval[3].trim();
//				String jobType = keyval[5].trim().toUpperCase();
//				String checkTimewindow = keyval[6].trim().toUpperCase();
//				String checkTimeTrigger = keyval[7].trim().toUpperCase();
//				String checkCalendar = keyval[8].trim().toUpperCase();
//				String checkLastStatus = keyval[9].trim().toUpperCase();
//				String priority = keyval[10];
//				String enable = keyval[11].trim();
//				
//				Map tmpM = new HashMap();
//				tmpM.put(conf.get(StrVal.JOB_ATTR_POOL), pool);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_SYSTEM), system);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_JOB), job);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_NODE), nodeName);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_JOBTYPE), jobType);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKTIMEWINDOW), checkTimewindow);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKTIMETRIGGER), checkTimeTrigger);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKCALENDAR), checkCalendar);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_CHECKLASTSTATUS), checkLastStatus);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_PRIORITY), priority);
//				tmpM.put(conf.get(StrVal.JOB_ATTR_ENABLE), enable);
//				String ctl = pool+"_"+system+"_"+job;
//				jobconfMaping.put(ctl, tmpM);
//			}
//		  } catch (FileNotFoundException e) {
//			e.printStackTrace();
//		  }catch (IOException e) {
//			e.printStackTrace();
//		  }finally{
//			try {
//				bufferedReader.close();
//				fileReader.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		  }
//		}else{
//		   log.info(filenm +" conf is not exist.");
//		}
//	}
	
	public void confFromDB(Connection conn,String sqlText){
		Statement st = null;
        ResultSet rt = null;
        int index = 0;
   		try {
   			st = conn.createStatement();
   			rt = st.executeQuery(sqlText);
   			while(rt.next()) {
   				String pool = rt.getString(1).trim().toUpperCase();
				String system = rt.getString(2).trim().toUpperCase();
				String job = rt.getString(3).trim().toUpperCase();
				String nodeName = rt.getString(4).trim();
				String jobType = rt.getString(6).trim().toUpperCase();
				String checkTimewindow = rt.getString(7).trim().toUpperCase();
				String checkTimeTrigger = rt.getString(8).trim().toUpperCase();
				String checkCalendar = rt.getString(9).trim().toUpperCase();
				String checkLastStatus = rt.getString(10).trim().toUpperCase();
				String priority = rt.getString(11);
				String enable = rt.getString(12).trim();

				JSONObject jsonobj = new JSONObject();
				jsonobj.put(conf.get(StrVal.JSON_KEY_POOL), pool);
				jsonobj.put(conf.get(StrVal.JSON_KEY_SYS), system);
				jsonobj.put(conf.get(StrVal.JSON_KEY_JOB), job);
				jsonobj.put(conf.get(StrVal.JSON_KEY_NODE), nodeName);
				jsonobj.put(conf.get(StrVal.JSON_KEY_JOBTYPE), jobType);
				jsonobj.put(conf.get(StrVal.JSON_KEY_CHECKTIMEWINDOW), checkTimewindow);
				jsonobj.put(conf.get(StrVal.JSON_KEY_CHECKTIMETRIGGER), checkTimeTrigger);
				jsonobj.put(conf.get(StrVal.JSON_KEY_CHECKCALENDAR), checkCalendar);
				jsonobj.put(conf.get(StrVal.JSON_KEY_CHECKLASTSTATUS), checkLastStatus);
				jsonobj.put(conf.get(StrVal.JSON_KEY_PRIORITY), priority);
				jsonobj.put(conf.get(StrVal.JSON_KEY_ENABLE), enable);

				String ctl = pool+conf.get(StrVal.DELIMITER_CTL_REGION)+system+conf.get(StrVal.DELIMITER_CTL_REGION)+job;
				jobconfMaping.put(ctl, jsonobj);
   			}
   		} catch (SQLException e) {
   			e.printStackTrace();
   		}
	}
	
	public void triggerFromDB(Connection conn,String sqlText){
		Statement st = null;
        ResultSet rt = null;
        int index = 0;
   		try {
   			st = conn.createStatement();
   			rt = st.executeQuery(sqlText);
   			while(rt.next()) {
   				String pool = rt.getString(1).trim().toUpperCase();
				String system = rt.getString(2).trim().toUpperCase();
				String job = rt.getString(3).trim().toUpperCase();
				String batchNum = rt.getString(5).trim().toUpperCase();
				String triggerType = rt.getString(6).trim();
				String expression = rt.getString(7).trim();
				String offset = rt.getString(8).trim();
				String enable = rt.getString(9).trim().toUpperCase();

				JSONObject jsonobj = new JSONObject();
				jsonobj.put(conf.get(StrVal.JSON_KEY_POOL), pool);
				jsonobj.put(conf.get(StrVal.JSON_KEY_SYS), system);
				jsonobj.put(conf.get(StrVal.JSON_KEY_JOB), job);
				jsonobj.put(conf.get(StrVal.JSON_KEY_NUM), batchNum);
				jsonobj.put(conf.get(StrVal.JSON_KEY_TRIGGERTYPE), triggerType);
				jsonobj.put(conf.get(StrVal.JSON_KEY_EXPRESSION), expression);
				jsonobj.put(conf.get(StrVal.JSON_KEY_OFFSET), offset);
				jsonobj.put(conf.get(StrVal.JSON_KEY_ENABLE), enable);

				String ctl = pool+conf.get(StrVal.DELIMITER_CTL_REGION)+system+conf.get(StrVal.DELIMITER_CTL_REGION)+job;
				jobTriggerMaping.put(ctl, jsonobj);
   			}
   		} catch (SQLException e) {
   			e.printStackTrace();
   		}
	}
	
	public void stepFromDB(Connection conn,String sqlText){
		Statement st = null;
        ResultSet rt = null;
        int index = 0;
   		try {
   			st = conn.createStatement();
   			rt = st.executeQuery(sqlText);
   			while(rt.next()) {
   				String pool = rt.getString(1).trim().toUpperCase();
				String system = rt.getString(2).trim().toUpperCase();
				String job = rt.getString(3).trim().toUpperCase();
				String cmd = rt.getString(5).trim();
				String enable = rt.getString(6).trim().toUpperCase();
				
				JSONObject jsonobj = new JSONObject();
				jsonobj.put(conf.get(StrVal.JSON_KEY_POOL), pool);
				jsonobj.put(conf.get(StrVal.JSON_KEY_SYS), system);
				jsonobj.put(conf.get(StrVal.JSON_KEY_JOB), job);
				jsonobj.put(conf.get(StrVal.JSON_KEY_CMD), cmd);
				jsonobj.put(conf.get(StrVal.JSON_KEY_ENABLE), enable);

				String ctl = pool+conf.get(StrVal.DELIMITER_CTL_REGION)+system+conf.get(StrVal.DELIMITER_CTL_REGION)+job;
				jobStepMaping.put(ctl, jsonobj);
   			}
   		} catch (SQLException e) {
   			e.printStackTrace();
   		}
	}
	
}
