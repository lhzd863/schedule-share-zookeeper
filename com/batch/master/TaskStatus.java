package com.batch.master;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.util.Configuration;
import com.batch.util.ErrorCode;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class TaskStatus implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
    Map pendingM = null;
    Map serverstatusM = null;
    Map jobserverMaping = null;
    Map jobconfMaping = null;
    String cfg = "";
    
    int cpulmt = 0;
	int memlmt = 0;
	int prclmt = 0;
	int maxres = 0;
	final static String clss = "task";
	
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
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.serverstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_NODESTATUS);
    	this.jobserverMaping = (Map) this.shareM.get(StrVal.MAP_KEY_MAPPING);
    	this.jobconfMaping = (Map) this.shareM.get(StrVal.MAP_KEY_JOB);
    	
    	zkcip = conf.get(StrVal.BATCH_ZKC_IP);
    	zkcport = conf.get(StrVal.BATCH_ZKC_PORT);
    	zkClient = new ZkClient(zkcip+":"+zkcport);
    	
    	//
    	cpulmt = Integer.parseInt(conf.get(StrVal.LMT_SLV_CPU));
		memlmt = Integer.parseInt(conf.get(StrVal.LMT_SLV_MEM));
		prclmt = Integer.parseInt(conf.get(StrVal.LMT_SLV_PRC));
		maxres = 100+100+prclmt;
		
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
//		log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);	
		if(log==null){
			LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			log = LoggerFactory.getLogger(TaskStatus.class);
		}
	}
	public boolean isDenpendencyOK(String ctl){
		return true;
	}
	
	public void list2notic(String ctl,JSONObject jsonval){
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstStreamPath = mstPath + conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STREAM);
		String mstQueuePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_QUEUE);
		String mstNoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
		
		//multi
		if(isDenpendencyOK(ctl)){
			//notice
			JSONObject jsonjob = JSONObject.fromObject(zkClient.readData(mstQueuePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
			if(UtilMth.isJsonNull(jsonjob)){
				 long  starttime = System.currentTimeMillis();
				 jsonjob = new JSONObject();
				 jsonjob.put(conf.get(StrVal.JSON_KEY_STARTTIME), starttime);
				 jsonjob.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_READY));
				 
				 Map ctlInfoMap = UtilMth.getCTLInfoMap(conf,ctl);
				 jsonjob.put(conf.get(StrVal.JSON_KEY_POOL), (String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
				 jsonjob.put(conf.get(StrVal.JSON_KEY_SYS), (String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
				 jsonjob.put(conf.get(StrVal.JSON_KEY_JOB), (String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
			}
			synchronized(jobstatusM){
				// list -> notice
				jsonjob.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_PENDING));
				UtilMth.mvNode(zkClient,mstQueuePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl,mstNoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl,jsonjob);
				jobstatusM.put(ctl,jsonjob);
			}
			log.info("Move "+ctl+" from "+mstQueuePath+" to "+mstNoticePath+" .");
			// Ready -> Pending
			log.info("Update Status "+ctl+" from "+conf.get(StrVal.SYSTEM_STATUS_READY)+" to "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" .");
		}
	}
	public void choseSlv(String ctl){
		String slvname=null;
		int culval = maxres;
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstlockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
		Map jobserverinfo = null;
		
		JSONObject jsonjob = JSONObject.fromObject(zkClient.readData(mstnoticePath + conf.get(StrVal.DELIMITER_NODE_LEVEL) + ctl));
		if(jsonjob.containsKey(conf.get(StrVal.JSON_KEY_NODE))&&
		   !UtilMth.isJsonNull(jsonjob.getString(conf.get(StrVal.JSON_KEY_NODE)))&&
		   !jsonjob.getString(conf.get(StrVal.JSON_KEY_NODE)).equals(conf.get(StrVal.DEFAULT_VAL_NAN))&&
		   jsonjob.getString(conf.get(StrVal.JSON_KEY_NODE)).length()>0){
			String jobnode = jsonjob.getString(conf.get(StrVal.JSON_KEY_NODE));
			String slvNodePath = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+jobnode;
			String slvNodeStatusPath = slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SLV_STATUS);
			String slvNodeCtl = slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
			String slvStatusCtl = slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
			if(zkClient.exists(slvNodeCtl)){
				mvNotice2Lock(ctl,jobnode);
			}else{
				log.info(jobnode+" has not submited ctl "+ctl+",wait for next time.");
			}
			return;
		}
		
		Iterator iter = serverstatusM.entrySet().iterator();
		while(iter.hasNext()){
			Entry entry = (Entry) iter.next();
			String key = (String) entry.getKey();
			JSONObject jsonserver = (JSONObject) entry.getValue();
			int tmpculval = resourceVal(key,jsonserver,1,1);
			
			String tmpslvctl = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key;
			if(!zkClient.exists(tmpslvctl)){
				log.info("Slave "+key+" not exists "+slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key+".");
				continue;
			}
			String slvctl = tmpslvctl+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
			if(!zkClient.exists(slvctl)){
				log.info(key+" has submited ctl "+ctl);
			}
			if(tmpculval<culval){
				culval = tmpculval;
				slvname = key;
			}
		}
		if(slvname!=null){
			log.info("Master submit "+ctl+" to "+slvname+".");
			mvNotice2Lock(ctl,slvname);
		}else{
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
                log.info(ctl+" server name not exists.");
			}
		}
	}

	public void checkSlvJobMaping(String ctl){
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
		if(jobserverMaping.containsKey(ctl)){
			JSONObject jsonjob = (JSONObject) jobserverMaping.get(ctl);
			long tmpstarttime = jsonjob.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
			long tmpendtime = System.currentTimeMillis();
			long tmpdivi = tmpendtime - tmpstarttime;
			if(tmpdivi>Long.parseLong(conf.get(StrVal.LMT_JOB_TIME))){
				log.info(ctl+" JOb time "+tmpdivi+",gt "+conf.get(StrVal.LMT_JOB_TIME));
			}
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(ctl+" ctl submit time "+tmpdivi+",lt "+conf.get(StrVal.LMT_JOB_TIME)+",mapping node "+jsonjob.getString(conf.get(StrVal.JSON_KEY_NODE))+".");
			}
			String tmpnodename = jsonjob.getString(conf.get(StrVal.JSON_KEY_NODE));
			String slvnodepath = slvPath + conf.get(StrVal.DELIMITER_NODE_LEVEL) + tmpnodename;
			if(zkClient.exists(slvnodepath)){
				String slvStatusctl = slvnodepath+ conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SLV_STATUS)+ conf.get(StrVal.DELIMITER_NODE_LEVEL)+ ctl;
				String slvNodeCtl = slvnodepath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
				JSONObject jsonmst = JSONObject.fromObject(zkClient.readData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
				if(!UtilMth.isJsonNull(jsonmst)&&
				   zkClient.exists(slvStatusctl)){
					JSONObject jsonslv = JSONObject.fromObject(zkClient.readData(slvStatusctl));
					if(!UtilMth.isJsonNull(jsonslv)&&
					   (jsonslv.containsKey(conf.get(StrVal.JSON_KEY_STATUS)))&&
					   (jsonslv.containsKey(conf.get(StrVal.JSON_KEY_SEQ)))&&
					   (jsonmst.containsKey(conf.get(StrVal.JSON_KEY_SEQ)))&&
					   !UtilMth.isJsonNull(jsonslv.getString(conf.get(StrVal.JSON_KEY_STATUS)))&&
					   (jsonslv.getLong(conf.get(StrVal.JSON_KEY_SEQ))!=-1)&&
					   (jsonmst.getLong(conf.get(StrVal.JSON_KEY_SEQ))!=-1)&&
					   (jsonslv.getLong(conf.get(StrVal.JSON_KEY_SEQ))==jsonmst.getLong(conf.get(StrVal.JSON_KEY_SEQ)))){
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					        log.info(ctl+" status ["+jsonslv.getString(conf.get(StrVal.JSON_KEY_STATUS))+"].");
						}
						jsonmst.put(conf.get(StrVal.JSON_KEY_STATUS), jsonslv.getString(conf.get(StrVal.JSON_KEY_STATUS)));
				        jobstatusM.put(ctl,jsonmst);
				        if(zkClient.exists(slvNodeCtl)){
				        	log.info(ctl+" delete from "+slvnodepath+ conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SLV_STATUS)+".");
				        	zkClient.delete(slvNodeCtl);
				        }
					}else{
						if(zkClient.exists(slvNodeCtl)){
							if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
						        log.info(tmpnodename+" not running job "+ctl+".");
							}
						}else{
							log.info("Update "+ctl+" status "+conf.get(StrVal.SYSTEM_STATUS_FAIL)+",seq not equal,"+slvNodeCtl+" not exists.");
							long  endtime = System.currentTimeMillis();
							jsonmst.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
							jsonmst.put(conf.get(StrVal.JSON_KEY_ENDTIME), endtime);
							jobstatusM.put(ctl,jsonmst);
						}
					}
				}else{
					if(zkClient.exists(slvNodeCtl)){
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					        log.info(tmpnodename+" slave not running job "+ctl+".");
						}
					}else{
						log.info("Update "+ctl+" status "+conf.get(StrVal.SYSTEM_STATUS_FAIL)+".");
						long  endtime = System.currentTimeMillis();
						if(UtilMth.isJsonNull(jsonmst)){
							jsonmst = (JSONObject) jobstatusM.get(ctl);
							jsonmst.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
							jsonmst.put(conf.get(StrVal.JSON_KEY_ENDTIME), endtime);
							jobstatusM.put(ctl,jsonmst);
						}else{
							jsonmst.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
							jsonmst.put(conf.get(StrVal.JSON_KEY_ENDTIME), endtime);
							jobstatusM.put(ctl,jsonmst);
						}
					}
				}
			}
		}else{
			JSONObject jsonmst = JSONObject.fromObject(zkClient.readData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
			if(zkClient.exists(mstnoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
				log.info("Delete "+ctl+" from "+mstnoticePath+".");
				zkClient.delete(mstnoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
			}
			log.info("Move "+ctl+" from "+mstLockPath+" to "+mstnoticePath+".");
			log.info("Update "+ctl+" status to "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" .");
			synchronized(jobstatusM){
				jsonmst.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_PENDING));
				UtilMth.mvNode(zkClient, mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, mstnoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonmst);
				jobstatusM.put(ctl,jsonmst);
			}
		}
	}
	
	public void checkRunningJob(String ctl){
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
		if(jobserverMaping.containsKey(ctl)){
			JSONObject jsonobj = (JSONObject) jobserverMaping.get(ctl);
			long tmpstarttime = jsonobj.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
			long tmpendtime = System.currentTimeMillis();
			long tmpdivi = tmpendtime - tmpstarttime;
			if(tmpdivi>Long.parseLong(conf.get(StrVal.LMT_JOB_TIME))){
				log.info(ctl+" JOb time "+tmpdivi+",gt "+conf.get(StrVal.LMT_JOB_TIME));
			}
			String jobnode = jsonobj.getString(conf.get(StrVal.JSON_KEY_NODE));
			String slvNodePath=slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+jobnode;
			String slvNodeStatusCtl = slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
			if(zkClient.exists(slvNodeStatusCtl)){
				if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					log.info(ctl+" job running time "+tmpdivi+",lt "+conf.get(StrVal.LMT_JOB_TIME)+",mapping node "+jsonobj.getString(conf.get(StrVal.JSON_KEY_NODE))+".");
				}
				JSONObject jsonjob = (JSONObject) jobstatusM.get(ctl);
				JSONObject jsonslv = JSONObject.fromObject(zkClient.readData(slvNodeStatusCtl));
				if(!UtilMth.isJsonNull(jsonslv)&&
				   jsonobj.containsKey(conf.get(StrVal.JSON_KEY_STATUS))&&
				   !UtilMth.isJsonNull(jsonobj.getString(conf.get(StrVal.JSON_KEY_STATUS)))){
					if(jsonobj.getString(conf.get(StrVal.JSON_KEY_STATUS)).equals(conf.get(StrVal.SYSTEM_STATUS_RUNNING))){
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							log.info(ctl+" status "+conf.get(StrVal.SYSTEM_STATUS_RUNNING)+",wait for next time.");
						}
					}else{
						jsonjob.put(conf.get(StrVal.JSON_KEY_STATUS), jsonslv.get(conf.get(StrVal.JSON_KEY_STATUS)));
						jobstatusM.put(ctl, jsonjob);
						log.info("Update "+ctl+" status from "+conf.get(StrVal.SYSTEM_STATUS_RUNNING)+" to "+conf.get(StrVal.SYSTEM_STATUS_RUNNING)+" .");
					}
				}else{
					long  endtime = System.currentTimeMillis();
					jsonjob.put(conf.get(StrVal.JSON_KEY_ENDTIME), endtime);
					jsonjob.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
					jobstatusM.put(ctl, jsonjob);
					log.info("Update "+ctl+" status to "+conf.get(StrVal.SYSTEM_STATUS_FAIL)+","+slvNodeStatusCtl+" status not exists.");
				}
			}else{
				JSONObject jsonjobstatus = (JSONObject) jobstatusM.get(ctl);
				long  endtime = System.currentTimeMillis();
				jsonjobstatus.put(conf.get(StrVal.JSON_KEY_ENDTIME), endtime);
				jsonjobstatus.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
				jobstatusM.put(ctl, jsonjobstatus);
				log.info("Update "+ctl+" status to "+conf.get(StrVal.SYSTEM_STATUS_FAIL)+","+slvNodeStatusCtl+" not exists.");
			}
		}else{
			log.info("Job and Server Mapping not "+ctl+".");
		}
	}
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		//
		boolean flg = true;
		//path
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstStreamPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STREAM);
		String mstQueuePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_QUEUE);
		String mstNoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
		String mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
//			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			if(log==null){
				LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
				log = LoggerFactory.getLogger(TaskStatus.class);
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
				flg = true;
			}
			//
	    	if(flg){
	    		flg = false;
	    		cpulmt = Integer.parseInt(conf.get(StrVal.LMT_SLV_CPU));
				memlmt = Integer.parseInt(conf.get(StrVal.LMT_SLV_MEM));
				prclmt = Integer.parseInt(conf.get(StrVal.LMT_SLV_PRC));
				maxres = 100+100+prclmt;
				
				mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
				mstStreamPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STREAM);
				mstQueuePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_QUEUE);
				mstNoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
				mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
	    	}
	    	if(zkClient==null||!zkClient.isALive()){
	    		log.info("zkclient stop,will retry connection.");
				zkClient = new ZkClient(conf.get(StrVal.BATCH_ZKC_IP)+":"+conf.get(StrVal.BATCH_ZKC_PORT));
		    }
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check Memory ctl.");
			}
			Iterator iter = jobstatusM.entrySet().iterator();
			while(iter.hasNext()){
				Entry entry = (Entry) iter.next();
				String key = (String) entry.getKey();
				JSONObject jsonval = (JSONObject) entry.getValue();
				String jobstatus = jsonval.getString(conf.get(StrVal.JSON_KEY_STATUS));
				if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					log.info("Check control "+key+",status["+jobstatus+"].");
				}
				if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_READY))){
					if(!zkClient.exists(mstQueuePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key)){
						log.info(mstQueuePath+" ignore "+key+" "+conf.get(StrVal.SYSTEM_STATUS_READY)+" status.");
						jobstatusM.remove(key);
					}else{
						//multi
						list2notic(key,jsonval);
					}
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_PENDING))){
					if(!zkClient.exists(mstNoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key)){
						log.info(mstNoticePath+" ignore "+key+" "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" status.");
						jobstatusM.remove(key);
					}else{
						choseSlv(key);
					}
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_SUBMIT))){
					if(!zkClient.exists(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key)){
						log.info(mstLockPath+" ignore "+key+" "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+" status.");
						jobstatusM.remove(key);
					}else{
						checkSlvJobMaping(key);
					}
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_RUNNING))){
					if(!zkClient.exists(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key)){
						log.info(mstLockPath+" ignore "+key+" "+conf.get(StrVal.SYSTEM_STATUS_RUNNING)+" status.");
						jobstatusM.remove(key);
					}else{
						checkRunningJob(key);
					}
				}else if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))||jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))){
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
						log.info("Ctl "+key+",status["+jobstatus+"].");
					}
				}else{
					jobstatusM.remove(key);
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
						log.info("Ctl "+key+",status["+jobstatus+"],unkonwn.");
					}
				}
			}
			//queue
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check node "+mstQueuePath+".");
			}
			if(zkClient.exists(mstQueuePath)){
				List queueLst = zkClient.getChildren(mstQueuePath);
				for(int i=0;i<queueLst.size();i++){
					String ctl = (String) queueLst.get(i);
					if(!jobstatusM.containsKey(ctl)){
						log.info("Memory not exist "+ctl+", put "+conf.get(StrVal.SYSTEM_STATUS_READY)+" status to memory.");
						JSONObject jsonobj = JSONObject.fromObject(zkClient.readData(mstQueuePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
						if(UtilMth.isJsonNull(jsonobj)){
							jsonobj = new JSONObject();
							Map ctlInfoMap = UtilMth.getCTLInfoMap(conf,ctl);
							jsonobj.put(conf.get(StrVal.JSON_KEY_POOL), (String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
							jsonobj.put(conf.get(StrVal.JSON_KEY_SYS), (String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
							jsonobj.put(conf.get(StrVal.JSON_KEY_JOB), (String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
						}
						jsonobj.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_READY));
						jobstatusM.put(ctl,jsonobj);
					}else{
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							log.info("Memory exists control "+ctl+".");
					    }
					}
				}
			}else{
				log.info(ErrorCode.FAILURE_00001+mstQueuePath+".");
			}
			
			//notice
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check node "+mstNoticePath+".");
			}
			if(zkClient.exists(mstNoticePath)){
				List noticeLst = zkClient.getChildren(mstNoticePath);
				for(int i=0;i<noticeLst.size();i++){
					String ctl = (String) noticeLst.get(i);
					if(!jobstatusM.containsKey(ctl)){
						log.info("Memory not exist "+ctl+", put "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" status to memory.");
						JSONObject jsonobj = JSONObject.fromObject(zkClient.readData(mstNoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
						synchronized(jobstatusM){
						    jsonobj.put(conf.get(StrVal.JSON_KEY_STATUS),conf.get(StrVal.SYSTEM_STATUS_PENDING));
						    zkClient.writeData(mstNoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonobj);
						    jobstatusM.put(ctl,jsonobj);
						}
					}else{ 
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							log.info("Memory exists control "+ctl+".");
						}
					}
				}
			}else{
				log.info(ErrorCode.FAILURE_00001+mstNoticePath+".");
			}
			
			//lock
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check node "+mstLockPath+".");
			}
			if(zkClient.exists(mstLockPath)){
				List lockLst = zkClient.getChildren(mstLockPath);
				for(int i=0;i<lockLst.size();i++){
					String ctl = (String) lockLst.get(i);
					if(!jobstatusM.containsKey(ctl)){
						log.info("Memory not exist "+ctl+", put "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" status to memory.");
						JSONObject jsonobj = JSONObject.fromObject(zkClient.readData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
						if(UtilMth.isJsonNull(jsonobj)){
							jsonobj = new JSONObject();
						}
						jsonobj.put(conf.get(StrVal.JSON_KEY_STATUS),conf.get(StrVal.SYSTEM_STATUS_PENDING));
						if(jobconfMaping.containsKey(ctl)){
							JSONObject jsonjob = (JSONObject) jobconfMaping.get(ctl);
							if(UtilMth.isJsonNull(jsonjob)){
								jsonjob.put(conf.get(StrVal.JSON_KEY_NODE), conf.get(StrVal.DEFAULT_VAL_NAN));
							}else{
								String jobnodetmp = jsonjob.getString(conf.get(StrVal.JSON_KEY_NODE));
								if(jobnodetmp==null||jobnodetmp.equals(conf.get(StrVal.DEFAULT_VAL_NAN))||jobnodetmp.length()<1){
									jsonjob.put(conf.get(StrVal.JSON_KEY_NODE), conf.get(StrVal.DEFAULT_VAL_NAN));
								}else{
									jsonjob.put(conf.get(StrVal.JSON_KEY_NODE), jobnodetmp);
								}
							}
						}
						if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_STATUS))){
							String t_status = jsonobj.getString(conf.get(StrVal.JSON_KEY_STATUS));
							if(t_status.equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))||t_status.equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))){
								if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
									log.info(ctl+" status ["+t_status+"],not load memory.");
								}
							}else{
								synchronized(jobstatusM){
									log.info("Move "+ctl+" from "+mstLockPath+" to "+mstNoticePath+".");
									UtilMth.mvNode(zkClient, mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, mstNoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonobj);
									jobstatusM.put(ctl,jsonobj);
								}
							}
						}
					}else{ 
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							log.info("Memory exists control "+ctl+".");
						}
					}
				}
			}else{
				log.info(ErrorCode.FAILURE_00001+mstNoticePath+".");
			}
			//log.info("*********************** Sleep ["+conf.get(StrVal.SYS_HEARTBEADT_TIME)+"] ***********************.");
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.BATCH_TIME_HEARTBEADT)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public int resourceVal(String slave,JSONObject jsonserver,double cpuwt,double memwt){
		int cpu = 0;
		int mem = 0;
		int proc = 0;
		int max = 0;
		int res = 100;
		if(UtilMth.isJsonNull(jsonserver)||jsonserver.getInt(conf.get(StrVal.JSON_KEY_CPUPCT))==-1){
			log.info(slave+" Lost CPU info.");
			return maxres;
		}
		//
		cpu = jsonserver.getInt(conf.get(StrVal.JSON_KEY_CPUPCT));
		mem = jsonserver.getInt(conf.get(StrVal.JSON_KEY_MEMPCT));
		proc = jsonserver.getInt(conf.get(StrVal.JSON_KEY_PROCESSCNT));
		double curproc=proc+1;
        if(prclmt<=curproc){
        	log.info(slave+" "+prclmt+"=>"+curproc+" Process reach limit.wait for next time.");
			return maxres;
		}
        double curcpu = cpu+(cpulmt*1.00)/prclmt*cpuwt;
        if(cpulmt<=curcpu){
        	log.info(slave+" "+cpulmt+"=>"+curcpu+" CPU reach limit.wait for next time.");
			return maxres;
		}
        double curmem = mem+(memlmt*1.00)/prclmt*memwt;
        if(memlmt<=curmem){
        	log.info(slave+" "+memlmt+"=>"+curmem+" memory reach limit.wait for next time.");
			return maxres;
		}
        res = (int) (curcpu+curmem+curproc);
        return res;
	}
	
	public void mvNotice2Lock(String ctl,String jobnode){
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstlockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		String mstnoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
		String mstStatusPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS);
		Map jobserverinfo = null;
		
		log.info("Move "+ctl+" from "+mstnoticePath+" to "+mstlockPath+".");
		log.info(ctl+" will running on "+jobnode+".");
		JSONObject jsonobj = JSONObject.fromObject(zkClient.readData(mstnoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
		long  starttime = System.currentTimeMillis();
		log.info(ctl+" update status from "+conf.get(StrVal.SYSTEM_STATUS_PENDING)+" to "+conf.get(StrVal.SYSTEM_STATUS_SUBMIT)+".");
		synchronized(jobstatusM){
			jsonobj.put(conf.get(StrVal.JSON_KEY_NODE), jobnode);
			jsonobj.put(conf.get(StrVal.JSON_KEY_STARTTIME), starttime);
			jsonobj.put(conf.get(StrVal.JSON_KEY_ENDTIME), starttime);
			jsonobj.put(conf.get(StrVal.JSON_KEY_SEQ), starttime);
			jsonobj.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_SUBMIT));
			UtilMth.mvNode(zkClient, mstnoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, mstlockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonobj);
			jobserverMaping.put(ctl, jsonobj);
		}
		//
		if(jobstatusM.containsKey(ctl)){
			JSONObject jsonstatus = (JSONObject) jobstatusM.get(ctl);
			jsonstatus.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_SUBMIT));
			jobstatusM.put(ctl, jsonstatus);
		}
	}
}
