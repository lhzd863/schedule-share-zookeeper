package com.batch.master;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.util.Configuration;
import com.batch.util.ErrorCode;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class ResourceCollect implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
	Map serverstatusM = null;
	String cfg = "";
	final static String clss = "resource";
	
	ZkClient zkClient = null;
	String zkcip = "";
	String zkcport = "";
	
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	serverstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_NODESTATUS);
    	
    	zkcip = conf.get(StrVal.BATCH_ZKC_IP);
    	zkcport = conf.get(StrVal.BATCH_ZKC_PORT);
    	zkClient = new ZkClient(zkcip+":"+zkcport);
    	
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
//		log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
		if(log==null){
			LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			log = LoggerFactory.getLogger(ResourceCollect.class);
		}
	}

	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
//			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			if(log==null){
				LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
				log = LoggerFactory.getLogger(ResourceCollect.class);
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
			}
			if(zkClient==null||!zkClient.isALive()){
				log.info("zkclient stop,will retry connection.");
				zkClient = new ZkClient(conf.get(StrVal.BATCH_ZKC_IP)+":"+conf.get(StrVal.BATCH_ZKC_PORT));
		    }
			//save server status
			String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
			String slvListPath = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SLV_LIST);
			if(!zkClient.exists(slvListPath)){
				log.info(ErrorCode.FAILURE_00001+slvListPath+".");
				try {
					Thread.sleep(Long.parseLong(conf.get(StrVal.BATCH_TIME_REFRESH)));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			//slvList
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check node "+slvListPath+".");
			}
			if(zkClient.exists(slvListPath)){
				List slvList = zkClient.getChildren(slvListPath);
				for(int i=0;i<slvList.size();i++){
					String serverctl = (String) slvList.get(i);
					JSONObject jsonobj = JSONObject.fromObject(zkClient.readData(slvListPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+serverctl));
					serverstatusM.put(serverctl, jsonobj);
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					   if(UtilMth.isJsonNull(jsonobj)&&
						  jsonobj.containsKey(conf.get(StrVal.JSON_KEY_CPUPCT))&&
						  jsonobj.containsKey(conf.get(StrVal.JSON_KEY_MEMPCT))&&
						  jsonobj.containsKey(conf.get(StrVal.JSON_KEY_PROCESSCNT))){
						  log.info("<"+jsonobj.getString(conf.get(StrVal.JSON_KEY_CPUPCT))+","+jsonobj.getString(conf.get(StrVal.JSON_KEY_MEMPCT))+","+jsonobj.getString(conf.get(StrVal.JSON_KEY_PROCESSCNT))+">");
					   }else{
						   log.info("No any node info.");
					   }
					}
				}
			}else{
				log.info(ErrorCode.FAILURE_00001+slvListPath+".");
			}
			
			//log.info("*********************** Sleep ["+conf.get(StrVal.SYS_HEARTBEADT_TIME)+"] ***********************.");
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.BATCH_TIME_HEARTBEADT)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
    
    
}
