package com.batch.slave;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.master.DefaultConf;
import com.batch.master.MasterTask;
import com.batch.master.TimeTiggerJob;
import com.batch.util.ClearLog;
import com.batch.util.Configuration;
import com.batch.util.ErrorCode;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SlaveTask {

	Map shareM = null;
	Configuration conf = null;
	Logger log = null;
	String node = null;
	ExecutorService fixThreadPool = null;
	String cfg = "";
	Map runningMap = null;
	final static String clss = "slv";
	
	ZkClient zkClient = null;
	String zkcip = "";
	String zkcport = "";
    public void initialize(String node,String cfg){
		//
		conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		this.node = node;
		
		shareM = new ConcurrentHashMap();
		runningMap = new ConcurrentHashMap();
		shareM.put(StrVal.MAP_KEY_RUNNING, runningMap);
		fixThreadPool = Executors.newFixedThreadPool(Integer.parseInt(conf.get(StrVal.LMT_SLV_PRC)));
		
		zkcip = conf.get(StrVal.BATCH_ZKC_IP);
    	zkcport = conf.get(StrVal.BATCH_ZKC_PORT);
    	zkClient = new ZkClient(zkcip+":"+zkcport);
    	
		//statistics resource submit to master
		SubmitResource sr = new SubmitResource();
		sr.initialize(node,cfg, shareM);
		new Thread(sr).start();
		
		//clear log
		ClearLog cl = new ClearLog();
		cl.initialize(node,cfg, shareM);
		new Thread(cl).start();
		
		long starttime = System.currentTimeMillis();
		while(true){
			//
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			long subvalconf = Long.parseLong(conf.get(StrVal.BATCH_TIME_REFRESH));
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
//			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), node+"_"+yyyymmdd);
			if(log==null){
				LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), node+"_"+yyyymmdd);
				log = LoggerFactory.getLogger(SlaveTask.class);
			}
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
			//schedule()
			schedule();
			//log.info("*********************** Sleep ["+conf.get(StrVal.SYS_HEARTBEADT_TIME)+"] ***********************.");
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.BATCH_TIME_HEARTBEADT)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}finally{
				
			}
    	}
	}
    public void schedule(){
    	String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		String slvNodePath = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node;
		String slvNodeStatusPath = slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS);
		String mstNoticePath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_NOTICE);
		//lock
		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
			log.info("Check node "+mstLockPath+".");
		}
		if(zkClient.exists(mstLockPath)){
			List mstLockLst = zkClient.getChildren(mstLockPath);
			Map runningMap = (Map) shareM.get(StrVal.MAP_KEY_RUNNING);
			for(int i=0;i<mstLockLst.size();i++){
				String ctl = (String) mstLockLst.get(i);
				if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					log.info("Check ctl "+ctl+".");
				}
				//chose server
				JSONObject jsonmstlock = JSONObject.fromObject(zkClient.readData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
				if(UtilMth.isJsonNull(jsonmstlock)||
				   !jsonmstlock.containsKey(conf.get(StrVal.JSON_KEY_NODE))||
				   UtilMth.isJsonNull(jsonmstlock.getString(conf.get(StrVal.JSON_KEY_NODE)))||
				   !jsonmstlock.getString(conf.get(StrVal.JSON_KEY_NODE)).equals(node)){
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
						if(jsonmstlock.containsKey(conf.get(StrVal.JSON_KEY_NODE))){
							log.info(ctl+" run node "+jsonmstlock.getString(conf.get(StrVal.JSON_KEY_NODE))+" not local node.");
						}else{
							log.info(ctl+" run node null not local node.");
						}
					}
					if(runningMap.containsKey(ctl)){
						log.info("Delete "+ctl+" from memory.");
						runningMap.remove(ctl);
					}
					continue;
				}
				//running status
				JSONObject jsonslvstatus = null;
				if(zkClient.exists(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
					jsonslvstatus = JSONObject.fromObject(zkClient.readData(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
					if(zkClient.exists(slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
						if(!UtilMth.isJsonNull(jsonslvstatus)){
							long mstseq = jsonmstlock.getLong(conf.get(StrVal.JSON_KEY_SEQ));
							long slvseq = jsonslvstatus.getLong(conf.get(StrVal.JSON_KEY_SEQ));
							if(mstseq==slvseq){
								if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
									log.info(node+" "+ctl+" run status ["+jsonmstlock.getString(conf.get(StrVal.JSON_KEY_STATUS))+"].");
								}
								log.info(node+" delete "+ctl+" from "+slvNodeStatusPath+".");
								zkClient.delete(slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
								//compare time 
								continue;
							}else{
								if(runningMap.containsKey(ctl)){
									runningMap.remove(ctl);
									log.info("Delete "+ctl+" from memory.");
								}
								zkClient.delete(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
								log.info(node+" delete "+ctl+" from "+slvNodeStatusPath+".");
								
								log.info(ctl+" will running.");
								SlaveExecuter ser = new SlaveExecuter();
								ser.initialize(node,conf, shareM,zkClient);
								ser.setCtl(ctl);
								long  starttime = System.currentTimeMillis();
								Map tmpMap = new ConcurrentHashMap();
								tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME),starttime);
								tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE), jsonmstlock.getLong(conf.get(StrVal.JSON_KEY_SEQ)));
								jsonmstlock.put(conf.get(StrVal.JSON_KEY_STARTTIME), starttime);
								jsonmstlock.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_RUNNING));
								UtilMth.mvNode(zkClient, slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonmstlock);
								log.info("Move "+ctl+" from "+slvNodePath+" to "+slvNodeStatusPath+".");
								runningMap.put(ctl, tmpMap);
								fixThreadPool.execute(ser);
							}
						}else{
							if(runningMap.containsKey(ctl)){
								runningMap.remove(ctl);
								log.info("Delete "+ctl+" from memory.");
							}
							zkClient.delete(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
							log.info(node+" delete "+ctl+" from "+slvNodeStatusPath+".");
							log.info(ctl+" will running.");
							
							SlaveExecuter ser = new SlaveExecuter();
							ser.initialize(node,conf, shareM,zkClient);
							ser.setCtl(ctl);
							long  starttime = System.currentTimeMillis();
							Map tmpMap = new ConcurrentHashMap();
							
							tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME),starttime);
							tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE), jsonmstlock.getLong(conf.get(StrVal.JSON_KEY_SEQ)));
							jsonmstlock.put(conf.get(StrVal.JSON_KEY_STARTTIME), starttime);
							jsonmstlock.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_RUNNING));
							UtilMth.mvNode(zkClient, slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonmstlock);
							log.info("Move "+ctl+" from "+slvNodePath+" to "+slvNodeStatusPath+".");
							runningMap.put(ctl, tmpMap);
							fixThreadPool.execute(ser);
						}
					}else{
						if(!UtilMth.isJsonNull(jsonslvstatus)){
							long mstseq = jsonmstlock.getLong(conf.get(StrVal.JSON_KEY_SEQ));
							long slvseq = jsonslvstatus.getLong(conf.get(StrVal.JSON_KEY_SEQ));
							if(mstseq==slvseq){
								if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
									if(jsonmstlock.containsKey(conf.get(StrVal.JSON_KEY_STATUS))){
										log.info(ctl+" is running,status ["+jsonmstlock.getString(conf.get(StrVal.JSON_KEY_STATUS))+"].");
									}else{
										log.info(ctl+" is running, no status .");
									}
								}
								continue;
							}else{
								if(runningMap.containsKey(ctl)){
									runningMap.remove(ctl);
									if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
										log.info("Delete "+ctl+" from memory.");
									}
								}
								zkClient.delete(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
								if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
									log.info(node+" delete "+ctl+" from "+slvNodeStatusPath+".");
								}
							}
						}else{
							if(runningMap.containsKey(ctl)){
								runningMap.remove(ctl);
								if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
									log.info("Delete "+ctl+" from memory.");
								}
							}
							zkClient.delete(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
							if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
								log.info(node+" delete "+ctl+" from "+slvNodeStatusPath+".");
							}
						}
					}
				}else{
					if(zkClient.exists(slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
						if(runningMap.containsKey(ctl)){
							runningMap.remove(ctl);
							log.info("Delete "+ctl+" from memory.");
						}
						log.info(ctl+" will running.");
						SlaveExecuter ser = new SlaveExecuter();
						ser.initialize(node,conf, shareM,zkClient);
						ser.setCtl(ctl);
						long  starttime = System.currentTimeMillis();
						Map tmpMap = new ConcurrentHashMap();
						tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME),starttime);
						tmpMap.put(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE), jsonmstlock.getLong(conf.get(StrVal.JSON_KEY_SEQ)));
						
						jsonmstlock.put(conf.get(StrVal.JSON_KEY_STARTTIME), starttime);
						jsonmstlock.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_RUNNING));
						UtilMth.mvNode(zkClient, slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonmstlock);
						log.info("Move "+ctl+" from "+slvNodePath+" to "+slvNodeStatusPath+".");
						runningMap.put(ctl, tmpMap);
						fixThreadPool.execute(ser);
					}else{
						if(runningMap.containsKey(ctl)){
							runningMap.remove(ctl);
							log.info("Delete "+ctl+" from memory.");
						}
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							log.info(node+" ctl "+ctl+" all not exists in "+slvNodeStatusPath+" and "+slvNodePath+",ingore ctl.");
						}
					}
				}
			}
		}else{
			log.info(ErrorCode.FAILURE_00001+mstLockPath+".");
		}
		//notice
		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
			log.info("Check node "+mstNoticePath+".");
		}
		if(zkClient.exists(mstNoticePath)){
			List noticeLst = zkClient.getChildren(mstNoticePath);
			for(int i=0;i<noticeLst.size();i++){
				String ctl = (String) noticeLst.get(i);
				String slvCtlPath = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
				if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					log.info("Check ctl "+ctl+".");
				}
				if(isResourceOK()){
					if(!zkClient.exists(slvCtlPath)){
						JSONObject jsonslvctl = new JSONObject();
						zkClient.create(slvCtlPath,jsonslvctl,CreateMode.PERSISTENT);
						log.info(ctl+" submit ctl to "+slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node+".");
					}else{
						if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
							log.info(ctl+" has submit.");
						}
						continue;
					}
				}else{
					log.info(node+" not free resource ,ignore "+ctl);
				}
			}
		}else{
			log.info(ErrorCode.FAILURE_00001+mstNoticePath+".");
		}
		
		//node status
		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
			log.info("Check node "+slvNodeStatusPath+".");
		}
		if(zkClient.exists(slvNodeStatusPath)){
			List slvNodeStatusLst = zkClient.getChildren(slvNodeStatusPath);
			for(int i=0;i<slvNodeStatusLst.size();i++){
				String ctl = (String) slvNodeStatusLst.get(i);
				if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					log.info("Check ctl "+ctl+".");
				}
				String slv2mstlockCtlPath = mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
				if(!zkClient.exists(slv2mstlockCtlPath)){
					log.info(ctl+" slave status exists,master lock not ctl,delete ctl.");
					zkClient.delete(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
				}
			}
		}
		//node
		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
			log.info("Check node "+slvNodePath+".");
		}
		if(zkClient.exists(slvNodePath)){
			List slvNodelst = zkClient.getChildren(slvNodePath);
			for(int i=0;i<slvNodelst.size();i++){
				String ctl = (String) slvNodelst.get(i);
				//expect status
				if(ctl.equals(conf.get(StrVal.NODE_MST_STATUS))){
					continue;
				}
				if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
					log.info("Check control "+ctl+".");
				}
				String slv2mstlockCtlPath = mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
				String slv2mstnoticeCtlPath = mstNoticePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl;
				if(!zkClient.exists(slv2mstlockCtlPath)&&!zkClient.exists(slv2mstnoticeCtlPath)){
					log.info(ctl+" slave node exists,delete from "+slvNodePath+",master lock and notice path not ctl,delete ctl.");
					zkClient.delete(slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
				}
			}
		}
    }
    
    public boolean isResourceOK(){
    	int lmtprc = Integer.parseInt((String)conf.get(StrVal.LMT_SLV_PRC));
    	if(runningMap.size()>=lmtprc){
    		return false;
    	}
    	return true;
    }
    private static void printUsage(String node,String cfg) {
	    System.out.println("*******************************************************************");
	    System.out.println("*Usage: java -jar *.jar cfg node");
	    System.out.println("* node:"+node);
	    System.out.println("* cfg :"+cfg);
	    System.out.println("*******************************************************************");
	}
	public static void main(String[] args) {
		String cfg = args[0];// "D:/conf/core-batch.cfg";
		String node = args[1];// "slv-1";
		printUsage( node, cfg);
		SlaveTask st = new SlaveTask();
		st.setNode(node);
		st.initialize(node, cfg);
		//
	}
	public String getNode() {
		return node;
	}
	public void setNode(String node) {
		this.node = node;
	}
}
