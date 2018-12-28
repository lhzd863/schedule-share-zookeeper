package com.batch.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.util.Configuration;
import com.batch.util.CronExpression;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class TimeTiggerJob implements Runnable{

	Configuration conf = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
	Logger log = null;
	String cfg = "";
	final static String clss = "trigger";
	Map triggerJobMap = new HashMap();
	Map jobconfMaping = null;
	
	Map res = null;
	CronExpression cronEx = null;
	Map jobTriggerMaping = null;
	
	ZkClient zkClient = null;
	String zkcip = "";
	String zkcport = "";
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
		zkcip = conf.get(StrVal.BATCH_ZKC_IP);
    	zkcport = conf.get(StrVal.BATCH_ZKC_PORT);
    	zkClient = new ZkClient(zkcip+":"+zkcport);
    	
    	this.shareM = shareM;
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.jobconfMaping = (Map) this.shareM.get(StrVal.MAP_KEY_JOB);
    	this.jobTriggerMaping = (Map) this.shareM.get(StrVal.MAP_KEY_TRIGGER);
	}
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat timestamp = new SimpleDateFormat("yyyyMMddHHmmss");
			
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
//			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			if(log==null){
				LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
				log = LoggerFactory.getLogger(TimeTiggerJob.class);
			}
			//init conf
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			long subvalconf = Long.parseLong(conf.get(StrVal.BATCH_TIME_REFRESH));
			//init conf
			if(subvalconf<=subval){
				log.info(clss+" refresh config.");
				conf.initialize(new File(cfg));
				DefaultConf.initialize(conf);
				starttime = endtime;
			}
			if(zkClient==null||!zkClient.isALive()){
				log.info("zkclient stop,will retry connection.");
				zkClient = new ZkClient(conf.get(StrVal.BATCH_ZKC_IP)+":"+conf.get(StrVal.BATCH_ZKC_PORT));
		    }
			Map hisTriggerM = logFromFile(conf.get(StrVal.BATCH_PATH_TRIGGER_LOG)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.BATCH_NAME_TRIGGER_LOG)+"_"+yyyymmdd+".log");
			Iterator iter = jobTriggerMaping.entrySet().iterator();
			while(iter.hasNext()){
				Entry entry = (Entry) iter.next();
				String key = (String) entry.getKey();
				JSONObject jsonobj = (JSONObject) entry.getValue();
				String triggertype = jsonobj.getString(conf.get(StrVal.JSON_KEY_TRIGGERTYPE));
				String triggerexpr = jsonobj.getString(conf.get(StrVal.JSON_KEY_EXPRESSION));
				String ctl = jsonobj.getString(conf.get(StrVal.JSON_KEY_NUM))+conf.get(StrVal.DELIMITER_CTL_REGION)+key;
				if(triggertype.trim().toUpperCase().equals("CRON")){
					Date ntDt = null;
					boolean iss = false;
					Date d = null;
					String nextTriggertime = "";
					try {
						cronEx = new CronExpression(triggerexpr);
						d = new Date();
						ntDt = cronEx.getTimeAfter(d);
						iss = cronEx.isSatisfiedBy(d);
						nextTriggertime =timestamp.format(ntDt);
					} catch (ParseException e1) {
						e1.printStackTrace();
					}
					if(iss){
						String flag = StrVal.VAL_CONSTANT_N;
						String initNextTriggertime = "30001231235959";
						if(triggerJobMap.containsKey(ctl)){
							Map tmpMap = (Map) triggerJobMap.get(ctl);
							flag = (String) tmpMap.get(StrVal.MAP_KEY_FLAG);
						}else{
							Map tmpMap = new HashMap();
							tmpMap.put(StrVal.MAP_KEY_FLAG, StrVal.VAL_CONSTANT_Y);
							triggerJobMap.put(ctl, tmpMap);
						}
						if(((Map)triggerJobMap.get(ctl)).get(StrVal.MAP_KEY_FLAG).equals(StrVal.VAL_CONSTANT_Y)){
							if(hisTriggerM.containsKey(ctl)&&hisTriggerM.get(ctl).equals(initNextTriggertime)){
								log.info(ctl+" has finished today.expression["+triggerexpr+"].");
							}else{
								String currenttime = timestamp.format(d);
								log.info(ctl+" is satisfy .expression["+triggerexpr+"].");
								invokeJob(ctl);
								UtilMth.writeCTLFile(ctl+conf.get(StrVal.DELIMITER_RECORD_TIRGGER)+initNextTriggertime+conf.get(StrVal.DELIMITER_PARAMETER_ROW),conf.get(StrVal.BATCH_PATH_TRIGGER_LOG)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.BATCH_NAME_TRIGGER_LOG)+"_"+yyyymmdd+".log");
							}
							Map tmpMap = (Map) triggerJobMap.get(ctl);
							tmpMap.put(StrVal.MAP_KEY_FLAG, "N");
						}else{
							if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
								log.info(ctl+" has finished last time.expression["+triggerexpr+"].");
							}
							continue;
						}
					}else{
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							log.info(ctl+" is not satisfy .expression["+triggerexpr+"].");
						}
						if(triggerJobMap.containsKey(ctl)){
							log.info(ctl+" has triggered.");
							UtilMth.writeCTLFile(ctl+conf.get(StrVal.DELIMITER_RECORD_TIRGGER)+nextTriggertime+conf.get(StrVal.DELIMITER_PARAMETER_ROW),conf.get(StrVal.BATCH_PATH_TRIGGER_LOG)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.BATCH_NAME_TRIGGER_LOG)+"_"+yyyymmdd+".log");
							triggerJobMap.remove(ctl);
						}
					}
				}else if(triggertype.trim().toUpperCase().equals("SIMPLE")){
					
				}else{
					log.info(ctl+" is not cron or simple trigger.");
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

	public Map logFromFile(String filenm){
		Map map = new HashMap();
		String result = null;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		File file = new File(filenm);
		if(file.exists()){
	      try {	
			String read = null;
			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);
			while((read=bufferedReader.readLine())!=null){
				String[] keyval = read.split(conf.get(StrVal.DELIMITER_RECORD_TIRGGER));
				String ctl = keyval[0].trim().toUpperCase();
				String val = keyval[1];
				map.put(ctl, val);
			}
		  } catch (FileNotFoundException e) {
			e.printStackTrace();
		  }catch (IOException e) {
			e.printStackTrace();
		  }finally{
			try {
				bufferedReader.close();
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		  }
		}
		return map;
	}
	public Map confFromDatabase(String datasource){
		Map map = new HashMap();
		return map;
	}
    public void triggerJobNoInfo(String ctl){
    	String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstStreamPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STREAM);
		if(zkClient.exists(mstStreamPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
			log.info(ctl+" path exists "+mstStreamPath+".");
		}else{
			JSONObject jsonobj = new JSONObject();
			zkClient.create(mstStreamPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl,jsonobj,CreateMode.PERSISTENT);
			log.info("Trigger "+ctl+",general ctl to "+mstStreamPath+".");
		}
    }
    public void invokeJob(String ctl){
    	String[] arr = ctl.split(conf.get(StrVal.DELIMITER_NODE_CTL));
    	String batchnum = arr[0];
    	String control = arr[1]+conf.get(StrVal.DELIMITER_CTL_REGION)+arr[2]+conf.get(StrVal.DELIMITER_CTL_REGION)+arr[3];
    	if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
			log.info("Invoke control "+control+".");
		}
    	if(jobconfMaping.containsKey(control)){
    		JSONObject jsonobj = (JSONObject) jobconfMaping.get(control);
			String priority = conf.get(StrVal.DEFAULT_VAL_NAN);
    		String checkTimeTrigger = conf.get(StrVal.DEFAULT_VAL_NAN);
			String enable = conf.get(StrVal.DEFAULT_VAL_NAN);
			if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_CHECKTIMETRIGGER))){
				checkTimeTrigger = jsonobj.getString(conf.get(StrVal.JSON_KEY_CHECKTIMETRIGGER));
			}
			if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_PRIORITY))){
				priority = jsonobj.getString(conf.get(StrVal.JSON_KEY_PRIORITY));
			}
			if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_ENABLE))){
				enable = jsonobj.getString(conf.get(StrVal.JSON_KEY_ENABLE));
			}
			
			if(!enable.equals("1")){
				log.info(ctl+" enable is not satisfy.");
				return;
			}
			if(!checkTimeTrigger.equals(StrVal.VAL_CONSTANT_Y)){
				log.info(ctl+" timeTrigger is not satisfy.");
				return;
			}
			if(priority==conf.get(StrVal.DEFAULT_VAL_NAN)){
				priority = "0";
			}
			triggerJobNoInfo(ctl);
    	}else{
    		log.info(ctl+" Info not exists config.");
    		triggerJobNoInfo(ctl);
    	}
    }
}
