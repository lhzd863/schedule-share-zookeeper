package com.batch.master;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.util.Configuration;
import com.batch.util.ErrorCode;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class StreamJob implements Runnable{

	Configuration conf = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
    Map jobconfMaping = null;
	Logger log = null;
	String cfg = "";
	final static String clss = "stream";
	ZkClient zkClient = null;
	String zkcip = "";
	String zkcport = "";
	Map jobStepMaping = null;
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.jobStepMaping = (Map) this.shareM.get(StrVal.MAP_KEY_STEP);
    	this.jobconfMaping = (Map) this.shareM.get(StrVal.MAP_KEY_JOB);
    	
    	zkcip = conf.get(StrVal.BATCH_ZKC_IP);
    	zkcport = conf.get(StrVal.BATCH_ZKC_PORT);
    	zkClient = new ZkClient(zkcip+":"+zkcport);
    	
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
//		log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
		if(log==null){
			LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			log = LoggerFactory.getLogger(StreamJob.class);
		}
	}
    
	public void stream(String control){
		String ctl = control;
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstStreamPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STREAM);
		String mstQueuePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_QUEUE);
		
		//header
		Map ctlInfoMap = UtilMth.getCTLInfoMap(conf,ctl);
		String[] arrjob = ctl.split(conf.get(StrVal.DELIMITER_NODE_LEVEL));
		String batchNum = arrjob[0];
		String t_pool = (String)ctlInfoMap.get(StrVal.MAP_KEY_POOL);
		String t_sys = (String)ctlInfoMap.get(StrVal.MAP_KEY_SYS);
		String t_job = (String)ctlInfoMap.get(StrVal.MAP_KEY_JOB);
		String t_ctl = t_pool+conf.get(StrVal.DELIMITER_CTL_REGION)+t_sys+conf.get(StrVal.DELIMITER_CTL_REGION)+t_job;
		
		//config
		JSONObject jsonjob = null;
		if(jobconfMaping.containsKey(t_ctl)){
			jsonjob = (JSONObject) jobconfMaping.get(t_ctl);
			if(UtilMth.isJsonNull(jsonjob)){
				log.info(control+" config is null,waite for next time.");
				return;
			}
		}else{
			log.info(control+" no config info,waite for next time.");
			return;
		}
		//enable
		if(jsonjob.containsKey(conf.get(StrVal.JSON_KEY_ENABLE))){
			String t_enable = (String) jsonjob.get(conf.get(StrVal.JSON_KEY_ENABLE));
			if(t_enable==null||t_enable.toString().equals("null")||!t_enable.equals("1")){
				log.info(control+" config enable is "+t_enable+",waite for next time.");
				return;
			}
		}
		//read map
		SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
		Date nowTime = new Date();
		String now = time.format(nowTime);
//		JSONObject jsonobj = JSONObject.fromObject(zkClient.readData(mstStreamPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+control));
//		if(UtilMth.isJsonNull(jsonobj)){
//			log.info(ctl+" no info,config job.");
//			jsonobj = new JSONObject();
////			jsonobj.put(conf.get(StrVal.JSON_KEY_POOL), t_pool);
////			jsonobj.put(conf.get(StrVal.JSON_KEY_SYS), t_sys);
////			jsonobj.put(conf.get(StrVal.JSON_KEY_JOB), t_job);
//		}
		JSONObject jsonobj = new JSONObject();		
		//node
		if(jsonjob.containsKey(conf.get(StrVal.JSON_KEY_NODE))){
			String t_node = (String) jsonjob.get(conf.get(StrVal.JSON_KEY_NODE));
			if(t_node!=null&&!t_node.toString().equals("null")&&t_node.length()>0){
				log.info(control+" config node is "+t_node+".");
				jsonobj.put(conf.get(StrVal.JSON_KEY_NODE), t_node.trim());
			}
		}
		//priority
		if(jsonjob.containsKey(conf.get(StrVal.JSON_KEY_PRIORITY))){
			String t_priority = (String) jsonjob.get(conf.get(StrVal.JSON_KEY_PRIORITY));
			if(t_priority!=null&&!t_priority.toString().equals("null")&&t_priority.length()>0){
				log.info(control+" config priority is "+t_priority+".");
				int t_int_priority = Integer.parseInt(t_priority.trim());
				if(t_int_priority>=Integer.parseInt(conf.get(StrVal.BATCH_MIN_PRIORITH))&&t_int_priority<=Integer.parseInt(conf.get(StrVal.BATCH_MAX_PRIORITH))){
					jsonobj.put(conf.get(StrVal.JSON_KEY_PRIORITY), t_int_priority);
				}else{
					jsonobj.put(conf.get(StrVal.JSON_KEY_PRIORITY), 0);
				}
			}
		}
		//step
		if(jobStepMaping.containsKey(t_ctl)){
			JSONObject jsonstep = (JSONObject) jobStepMaping.get(t_ctl);
			if(jsonstep.containsKey(conf.get(StrVal.JSON_KEY_CMD))){
				String t_cmd = (String) jsonstep.get(conf.get(StrVal.JSON_KEY_CMD));
				if(t_cmd!=null&&!t_cmd.toString().equals("null")&&t_cmd.length()>0){
					log.info(control+" config cmd is "+t_cmd+".");
					jsonobj.put(conf.get(StrVal.JSON_KEY_CMD), t_cmd);
				}
			}
		}
		//
		long  starttime = System.currentTimeMillis();
		//parametmeter set
		log.info("Move "+ctl+" from "+mstStreamPath+" to "+mstQueuePath+" .");
		log.info("Update "+ctl+" status to "+conf.get(StrVal.SYSTEM_STATUS_READY)+" .");
		synchronized(jobstatusM){
			jsonobj.put(conf.get(StrVal.JSON_KEY_STARTTIME), starttime);
			jsonobj.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_READY));
			UtilMth.mvNode(zkClient,mstStreamPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl,mstQueuePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl,jsonobj);
			jobstatusM.put(ctl,jsonobj);
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
				log = LoggerFactory.getLogger(StreamJob.class);
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
				if(!zkcip.equals(conf.get(StrVal.BATCH_ZKC_IP))||!zkcport.equals(conf.get(StrVal.BATCH_ZKC_PORT))){
					zkClient.closeConnect();
					zkClient = new ZkClient(conf.get(StrVal.BATCH_ZKC_IP)+":"+conf.get(StrVal.BATCH_ZKC_PORT));
				}
			}
			if(zkClient==null||!zkClient.isALive()){
				log.info("zkclient stop,will retry connection.");
				zkClient = new ZkClient(conf.get(StrVal.BATCH_ZKC_IP)+":"+conf.get(StrVal.BATCH_ZKC_PORT));
		    }
			String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
			String mstStreamPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STREAM);
			
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check node "+mstStreamPath+".");
			}
			if(!(conf.get(StrVal.BATCH_FLAG_RUNNING).equals("0"))){
				log.info("Stream job exit running flag is "+conf.get(StrVal.BATCH_FLAG_RUNNING)+".");
				break;
			}
			if(zkClient.exists(mstStreamPath)){
				List streamLst = zkClient.getChildren(mstStreamPath);
				for(int i=0;i<streamLst.size();i++){
					String ctl = (String) streamLst.get(i);
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
						log.info("Stream job received "+ctl+".");
					}
					if(jobstatusM.containsKey(ctl)){
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							JSONObject jsonobj = (JSONObject) jobstatusM.get(ctl);
							if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_STATUS))){
								log.info(ctl+" status "+jsonobj.getString(conf.get(StrVal.JSON_KEY_STATUS))+" ingore stream.");
							}else{
								log.info(ctl+" status lost ingore stream.");
							}
						}
					}else{
						stream(ctl);
					}
					
				}
			}else{
				log.info(ErrorCode.FAILURE_00001+mstStreamPath+".");
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
