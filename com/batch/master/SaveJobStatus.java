package com.batch.master;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.util.Configuration;
import com.batch.util.ErrorCode;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SaveJobStatus implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
    Map pendingM = null;
    Map serverstatusM = null;
    String cfg = "";
    final static String clss = "save";
    Logger statuslog = null;
    
	ZkClient zkClient = null;
	String zkcip = "";
	String zkcport = "";
	SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat timestampformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
    public void initialize(String node,String cfgfile,Map shareM){
    	//init conf
    	this.cfg = cfgfile;
		this.conf = new Configuration();
		conf.initialize(new File(cfg));
		DefaultConf.initialize(conf);
		
    	this.shareM = shareM;
    	this.jobstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_STATUS);
    	this.serverstatusM = (Map) this.shareM.get(StrVal.MAP_KEY_NODESTATUS);
    	
    	zkcip = conf.get(StrVal.BATCH_ZKC_IP);
    	zkcport = conf.get(StrVal.BATCH_ZKC_PORT);
    	zkClient = new ZkClient(zkcip+":"+zkcport);
    	
		Date nowTime = new Date();
		String yyyymmdd = dateformat.format(nowTime);
//		log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
		if(log==null){
			LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			log = LoggerFactory.getLogger(SaveJobStatus.class);
		}
	}

	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		//
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		String mstStatusPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS);
		String resFinishPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS) +conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_COMPLETED);
		long subvalconf = Long.parseLong(conf.get(StrVal.BATCH_TIME_REFRESH));
		boolean flag = true;
		
		while(true){
			//init log
			Date nowTime = new Date();
			String yyyymmdd = dateformat.format(nowTime);
//			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
			if(log==null){
				LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), clss+"_"+yyyymmdd);
				log = LoggerFactory.getLogger(SaveJobStatus.class);
			}
			//refresh config
			long endtime = System.currentTimeMillis();
			long subval = endtime - starttime;
			
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
				mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
				slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
				mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
				mstStatusPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS);
				resFinishPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS) +conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_COMPLETED);
				subvalconf = Long.parseLong(conf.get(StrVal.BATCH_TIME_REFRESH));
			}
			if(zkClient==null||!zkClient.isALive()){
				log.info("zkclient stop,will retry connection.");
				zkClient = new ZkClient(conf.get(StrVal.BATCH_ZKC_IP)+":"+conf.get(StrVal.BATCH_ZKC_PORT));
		    }
			//
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check Memory ctl status match node "+mstLockPath+".");
			}
			if(!(conf.get(StrVal.BATCH_FLAG_RUNNING).equals("0"))){
				log.info("Stream job exit running flag is "+conf.get(StrVal.BATCH_FLAG_RUNNING)+".");
				break;
			}
			List liststatus = new ArrayList();
			//save status
			Iterator iter = jobstatusM.entrySet().iterator();
			while(iter.hasNext()){
				Entry entry = (Entry) iter.next();
				String key = (String) entry.getKey();
				JSONObject jsonobj = (JSONObject) entry.getValue();
				if(!UtilMth.isJsonNull(jsonobj)){
					String jobstatus = jsonobj.getString(conf.get(StrVal.JSON_KEY_STATUS));
	                if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))||jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))){
	                	if(zkClient.exists(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key)){
	                		JSONObject jsonmstctl = JSONObject.fromObject(zkClient.readData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key));
							JSONObject jsonslvctl = null;
							if(!UtilMth.isJsonNull(jsonmstctl)){
								if(zkClient.exists(slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE))+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key)){
									jsonslvctl = JSONObject.fromObject(zkClient.readData(slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE))+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key));
								}else{
									jsonslvctl = new JSONObject();
								}
			                	long t_starttime = -1;
			                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_STARTTIME))&&(jsonobj.getLong(conf.get(StrVal.JSON_KEY_STARTTIME))!=-1)){
			                		t_starttime = jsonobj.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
			                	}else if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_STARTTIME))&&(jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME))!=-1)){
			                		t_starttime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
			                	}else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_STARTTIME))&&(jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME))!=-1)){
			                		t_starttime = jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
			                	}
			                	long t_endtime = -1;
			                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_ENDTIME))&&(jsonobj.getLong(conf.get(StrVal.JSON_KEY_ENDTIME))!=-1)){
			                		t_endtime = jsonobj.getLong(conf.get(StrVal.JSON_KEY_ENDTIME));
			                	}else if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_ENDTIME))&&(jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME))!=-1)){
			                		t_endtime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME));
			                	}else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_ENDTIME))&&(jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME))!=-1)){
			                		t_endtime = jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME));
			                	}
			                	String t_node = conf.get(StrVal.DEFAULT_VAL_NAN);
			                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_NODE))&&(!UtilMth.isJsonNull(jsonobj.getString(conf.get(StrVal.JSON_KEY_NODE))))){
			                		t_node = jsonobj.getString(conf.get(StrVal.JSON_KEY_NODE));
			                	}else if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_NODE))&&(!UtilMth.isJsonNull(jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE))))){
			                		t_node = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE));
			                	}else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_NODE))&&(!UtilMth.isJsonNull(jsonslvctl.getString(conf.get(StrVal.JSON_KEY_NODE))))){
			                		t_node = jsonslvctl.getString(conf.get(StrVal.JSON_KEY_NODE));
			                	}
			                	String t_txdate = conf.get(StrVal.DEFAULT_VAL_NAN);
			                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&(!UtilMth.isJsonNull(jsonobj.getString(conf.get(StrVal.JSON_KEY_TXDATE))))){
			                		t_txdate = jsonobj.getString(conf.get(StrVal.JSON_KEY_TXDATE));
			                	}else if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&(!UtilMth.isJsonNull(jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE))))){
			                		t_txdate = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE));
			                	}else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&(!UtilMth.isJsonNull(jsonslvctl.getString(conf.get(StrVal.JSON_KEY_TXDATE))))){
			                		t_txdate = jsonslvctl.getString(conf.get(StrVal.JSON_KEY_TXDATE));
			                	}
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_STATUS), jobstatus);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_NODE), t_node);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_TXDATE), t_txdate);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_STARTTIME), t_starttime);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_ENDTIME), t_endtime);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_MODULE), clss);
			                	log.info(" Move "+key+" from "+mstLockPath+" to "+mstStatusPath+".");
								UtilMth.mvNode(zkClient, mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key, mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key, jsonobj);
							}else{
								String t_node=conf.get(StrVal.DEFAULT_VAL_NAN);
								String t_txdate=conf.get(StrVal.DEFAULT_VAL_NAN);
								long t_starttime=-1;
								long t_endtime=-1;
								jsonobj.put(conf.get(StrVal.JSON_KEY_STATUS), jobstatus);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_NODE), t_node);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_TXDATE), t_txdate);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_STARTTIME), t_starttime);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_ENDTIME), t_endtime);
			                	jsonobj.put(conf.get(StrVal.JSON_KEY_MODULE), clss);
			                	log.info(" Move "+key+" from "+mstLockPath+" to "+mstStatusPath+".");
								UtilMth.mvNode(zkClient, mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key, mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+key, jsonobj);
							}
	                	}else{
	                		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
	            				log.info(key+" not exists in "+mstLockPath+".");
	            			}
	                	}
	                }
				}
			}
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check node "+mstLockPath+".");
			}
			if(zkClient.exists(mstLockPath)){
				List lockLst = zkClient.getChildren(mstLockPath);
				for(int i=0;i<lockLst.size();i++){
					String ctl = (String) lockLst.get(i);
					if(jobstatusM.containsKey(ctl)){
						log.info(ctl+" exists memory,ingore this.");
						continue;
					}
					//chose server
					JSONObject jsonmstctl = JSONObject.fromObject(zkClient.readData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
					if(!UtilMth.isJsonNull(jsonmstctl)){
						String ctlslvnode = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE));
						String slvNodeStatusPath = slvPath +conf.get(StrVal.DELIMITER_NODE_LEVEL)+ ctlslvnode+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS);
						if(zkClient.exists(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
							if(jsonmstctl.getString(conf.get(StrVal.JSON_KEY_STATUS)).equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))||jsonmstctl.getString(conf.get(StrVal.JSON_KEY_STATUS)).equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))){
								   JSONObject jsonslvctl = JSONObject.fromObject(zkClient.readData(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));   
								   if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
									  log.info(ctl+" job has finish status "+jsonmstctl.getString(conf.get(StrVal.JSON_KEY_STATUS))+".");
								   }
								   String jobstatus = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_STATUS));
								   //
								   long t_starttime = -1;
								   if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_STARTTIME))&&jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME))!=-1){
									   t_starttime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
								   }else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_STARTTIME))&&jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME))!=-1){
									   t_starttime = jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
								   }
				                   long t_endtime = -1;
				                   if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_ENDTIME))&&jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME))!=-1){
				                	   t_endtime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME));
								   }else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_ENDTIME))&&jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME))!=-1){
									   t_endtime = jsonslvctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME));
								   }
				                   String t_node = conf.get(StrVal.DEFAULT_VAL_NAN);
				                   if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_NODE))&&!UtilMth.isJsonNull(jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE)))){
				                	   t_node = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE));
								   }else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_NODE))&&!UtilMth.isJsonNull(jsonslvctl.getString(conf.get(StrVal.JSON_KEY_NODE)))){
									   t_node = jsonslvctl.getString(conf.get(StrVal.JSON_KEY_NODE));
								   }
				                   String t_txdate = conf.get(StrVal.DEFAULT_VAL_NAN);
				                   if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&!UtilMth.isJsonNull(jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE)))){
				                	   t_txdate = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE));
								   }else if(jsonslvctl.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&!UtilMth.isJsonNull(jsonslvctl.getString(conf.get(StrVal.JSON_KEY_TXDATE)))){
									   t_txdate = jsonslvctl.getString(conf.get(StrVal.JSON_KEY_TXDATE));
								   }
				                   jsonmstctl.put(conf.get(StrVal.JSON_KEY_STATUS), jobstatus);
				                   jsonmstctl.put(conf.get(StrVal.JSON_KEY_NODE), t_node);
				                   jsonmstctl.put(conf.get(StrVal.JSON_KEY_TXDATE), t_txdate);
				                   jsonmstctl.put(conf.get(StrVal.JSON_KEY_STARTTIME), t_starttime);
				                   jsonmstctl.put(conf.get(StrVal.JSON_KEY_ENDTIME), t_endtime);
				                   jsonmstctl.put(conf.get(StrVal.JSON_KEY_MODULE), clss);
				                   
				                   log.info(" Move "+ctl+" from "+mstLockPath+" to "+mstStatusPath+".");
				                   UtilMth.mvNode(zkClient, mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonmstctl);
							}
						}else{
							if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
								log.info(slvNodeStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl+" not exists.");
							}
							if(zkClient.exists(slvPath +conf.get(StrVal.DELIMITER_NODE_LEVEL)+ ctlslvnode +conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
								log.info(ctl+" slave node "+slvPath +conf.get(StrVal.DELIMITER_NODE_LEVEL)+ ctlslvnode+" exists,wait for next time.");
								continue;
							}else{
								if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
									log.info(slvPath +conf.get(StrVal.DELIMITER_NODE_LEVEL)+ ctlslvnode +conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl+" not exists.");
								}
								String t_status = conf.get(StrVal.SYSTEM_STATUS_FAIL);
								String t_node = ctlslvnode;
								String t_txdate = conf.get(StrVal.DEFAULT_VAL_NAN);;
								if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&!UtilMth.isJsonNull(jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE)))){
				                	t_txdate = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE));
								}
								JSONObject jsonctl = new JSONObject();
								jsonctl.put(conf.get(StrVal.JSON_KEY_STATUS), t_status);
								jsonctl.put(conf.get(StrVal.JSON_KEY_NODE), t_node);
								jsonctl.put(conf.get(StrVal.JSON_KEY_TXDATE), t_txdate);
								jsonctl.put(conf.get(StrVal.JSON_KEY_STARTTIME), -1);
								jsonctl.put(conf.get(StrVal.JSON_KEY_ENDTIME), -1);
			                	jsonctl.put(conf.get(StrVal.JSON_KEY_MODULE), clss);
			                	
			                	log.info(" *** Exception "+ctl+" status ["+conf.get(StrVal.SYSTEM_STATUS_FAIL)+"],"+slvNodeStatusPath+" info is lost.");
								log.info(" Move "+ctl+" from "+mstLockPath+" to "+mstStatusPath+".");
								UtilMth.mvNode(zkClient, mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonctl);
			                }
						}
					}else{
						String t_status = conf.get(StrVal.SYSTEM_STATUS_FAIL);
						String t_node = conf.get(StrVal.DEFAULT_VAL_NAN);
						String t_txdate = conf.get(StrVal.DEFAULT_VAL_NAN);
						JSONObject jsonctl = new JSONObject();
						jsonctl.put(conf.get(StrVal.JSON_KEY_STATUS), t_status);
						jsonctl.put(conf.get(StrVal.JSON_KEY_NODE), t_node);
						jsonctl.put(conf.get(StrVal.JSON_KEY_TXDATE), t_txdate);
						jsonctl.put(conf.get(StrVal.JSON_KEY_STARTTIME), -1);
						jsonctl.put(conf.get(StrVal.JSON_KEY_ENDTIME), -1);
	                	jsonctl.put(conf.get(StrVal.JSON_KEY_MODULE), clss);
	                	
	                	log.info(" *** Exception "+ctl+" status ["+conf.get(StrVal.SYSTEM_STATUS_FAIL)+"],master lock info is lost.");
	                	log.info(" Move "+ctl+" from "+mstLockPath+" to "+mstStatusPath+".");
		                UtilMth.mvNode(zkClient, mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonctl);
					}
				}
			}else{
				log.info(ErrorCode.FAILURE_00001+mstLockPath+".");
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
