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

public class RecycleJobStatus implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	Map jobstatusM = null;
    Map jobsteamM = null;
    Map pendingM = null;
    Map serverstatusM = null;
    String cfg = "";
    final static String clss = "recycle";
    Logger statuslog = null;
    
    //database 
    Connection conn = null;
	PreparedStatement pst=null;
	PreparedStatement psti=null;
	PreparedStatement pstd=null;
	
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
			log = LoggerFactory.getLogger(LoadInfo.class);
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
				log = LoggerFactory.getLogger(LoadInfo.class);
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
				log.info("Check Memory ctl status match node "+mstStatusPath+".");
			}
			if(!(conf.get(StrVal.BATCH_FLAG_RUNNING).equals("0"))){
				log.info("Stream job exit running flag is "+conf.get(StrVal.BATCH_FLAG_RUNNING)+".");
				break;
			}
			
			if(conn==null){
				conn = UtilMth.getConnectionDB(conf); 
				try {
					conn.setAutoCommit(false);
					pst = conn.prepareStatement(conf.get(StrVal.SQL_SHD_JOB_STATUS),ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
					pstd = conn.prepareStatement(conf.get(StrVal.SQL_DELETE_STATUS),ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
					psti = conn.prepareStatement(conf.get(StrVal.SQL_INSERT_STATUS),ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				log.info("Connection database update job status.");
			}
			try {
				//delete memory table
				pstd.executeBatch();
				conn.commit();
			} catch (SQLException e1) {
				e1.printStackTrace();
			};
			try {
				conn.setAutoCommit(false);
			} catch (SQLException e2) {
				e2.printStackTrace();
			}
			List list = new ArrayList();
			List liststatus = new ArrayList();
			//save status
			Iterator iter = jobstatusM.entrySet().iterator();
			while(iter.hasNext()){
				Entry entry = (Entry) iter.next();
				String ctl = (String) entry.getKey();
				JSONObject jsonobj = (JSONObject) entry.getValue();
				if(!UtilMth.isJsonNull(jsonobj)){
					String jobstatus = jsonobj.getString(conf.get(StrVal.JSON_KEY_STATUS));
	                if(jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_SUCCESS))||jobstatus.equals(conf.get(StrVal.SYSTEM_STATUS_FAIL))){
	                	if(zkClient.exists(mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
	                		String[] arrjob = ctl.split(conf.get(StrVal.DELIMITER_NODE_CTL));
	                    	String batchNum = arrjob[0];
	    					String batchNumPath = resFinishPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchNum;
	    					if(!zkClient.exists(batchNumPath)){
	    						JSONObject jsonbatch = new JSONObject();
	    						zkClient.create(resFinishPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchNum, jsonbatch,CreateMode.PERSISTENT);
	    					}
	    					JSONObject jsonmstctl = JSONObject.fromObject(zkClient.readData(mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
	    					JSONObject jsonslvctl = null;
	    					
	                    	log.info(" Move "+ctl+" from "+mstStatusPath+" to "+batchNumPath+".");
	    					UtilMth.mvNode(zkClient, mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, batchNumPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonmstctl);
	    					log.info("Remove "+ctl+" from memory,status ["+jobstatus+"].");
	                    	jobstatusM.remove(ctl);
	                    	list.add(ctl);
	                    	
	                    	//
	    					Map ctlInfoMap = UtilMth.getCTLInfoMap(conf,ctl);
	                    	String t_node = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE));
	                    	String t_txdate = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE));
	                    	long t_starttime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
	                    	long t_endtime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME));
	                    	if(t_starttime==-1){
	                    		t_starttime = System.currentTimeMillis();
	                    	}
	                    	if(t_endtime==-1){
	                    		t_endtime = System.currentTimeMillis();
	                    	}
	                    	try {
	    						pst.setString(1,(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
	    						pst.setString(2,(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
	    						pst.setString(3,(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
	    						pst.setString(4,t_node);
	    						pst.setString(5,jobstatus);
	    						pst.setString(6,batchNum);
	    						pst.setString(7,t_txdate);
	    						pst.setString(8,timestampformat.format(t_starttime));
	    						pst.setString(9,timestampformat.format(t_endtime));
	    						pst.addBatch();
	    					} catch (SQLException e) {
	    						e.printStackTrace();
	    					}
//	                    	log.info("1. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL)+
//									 "2. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS)+
//									 "3. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB)+
//									 "4. =>"+t_node+
//									 "5. =>"+jobstatus+
//									 "6. =>"+batchNum+
//									 "7. =>"+t_txdate+
//									 "7. =>"+timestampformat.format(t_starttime)+
//									 "8. =>"+timestampformat.format(t_endtime)
//									 );
	                	}else if(zkClient.exists(mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
	                		log.info(conf.get(StrVal.NODE_MST_LOCK)+" node exists ,waite for next time.");
	                	}else{
	                		log.info("Remove "+ctl+" from memory,status ["+jobstatus+"],path status or lock not exists ctl.");
	                    	jobstatusM.remove(ctl);
	                	}
	                }else{
	                	Map ctlInfoMap = UtilMth.getCTLInfoMap(conf,ctl);
	                	String[] arrjob = ctl.split(conf.get(StrVal.DELIMITER_NODE_CTL));
	                	String batchNum = arrjob[0];
	                	liststatus.add(ctl);
	                	long  endts = System.currentTimeMillis();
	                	long tmpstarttime = endts;
	                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_STARTTIME))){
	                		tmpstarttime = (jsonobj.getLong(conf.get(StrVal.JSON_KEY_STARTTIME))==-1)?(endts):(jsonobj.getLong(conf.get(StrVal.JSON_KEY_STARTTIME)));
	                	}
	                	String t_node = conf.get(StrVal.DEFAULT_VAL_NAN);
	                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_NODE))){
	                		t_node = jsonobj.getString(conf.get(StrVal.JSON_KEY_NODE));
	                	}
	                	String t_txdate = conf.get(StrVal.DEFAULT_VAL_NAN);
	                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))){
	                		t_txdate = jsonobj.getString(conf.get(StrVal.JSON_KEY_TXDATE));
	                	}
	                	String t_status = conf.get(StrVal.DEFAULT_VAL_NAN);
	                	if(jsonobj.containsKey(conf.get(StrVal.JSON_KEY_STATUS))){
	                		t_status = jsonobj.getString(conf.get(StrVal.JSON_KEY_STATUS));
	                	}
	                	try {
	                		psti.setString(1,(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
	                		psti.setString(2,(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
	                		psti.setString(3,(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
	                		psti.setString(4,t_node);
	                		psti.setString(5,t_status);
	                		psti.setString(6,batchNum);
	                		psti.setString(7,t_txdate);
	                		psti.setString(8,timestampformat.format(tmpstarttime));
	                		psti.setString(9,timestampformat.format(endts));
	                		psti.addBatch();
						} catch (SQLException e) {
							e.printStackTrace();
						}
//	                	log.info("1. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL)+
//								 "2. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS)+
//								 "3. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB)+
//								 "4. =>"+t_node+
//								 "5. =>"+t_status+
//								 "6. =>"+batchNum+
//								 "7. =>"+t_txdate+
//								 "7. =>"+timestampformat.format(tmpstarttime)+
//								 "8. =>"+timestampformat.format(endts)
//								 );
	                }
				}else{
					log.info("Remove "+ctl+" from memory,node info null.");
                	jobstatusM.remove(ctl);
				}
			}
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info("Check node "+mstStatusPath+".");
			}
			if(zkClient.exists(mstStatusPath)){
				List statusLst = zkClient.getChildren(mstStatusPath);
				for(int i=0;i<statusLst.size();i++){
					String ctl = (String) statusLst.get(i);
					if(jobstatusM.containsKey(ctl)){
						log.info(ctl+" exists memory,ingore this.");
						continue;
					}
					if(list.contains(ctl)){
						log.info(ctl+" has remove from memory,ingore this.");
						continue;
					}
					//chose server
					JSONObject jsonmstctl = JSONObject.fromObject(zkClient.readData(mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
					if(!UtilMth.isJsonNull(jsonmstctl)){
						Map ctlInfoMap = UtilMth.getCTLInfoMap(conf,ctl);
	                	String[] arrjob = ctl.split(conf.get(StrVal.DELIMITER_NODE_CTL));
	                	String batchNum = arrjob[0];
	                	String batchNumPath = resFinishPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchNum;
                    	String t_status = conf.get(StrVal.SYSTEM_STATUS_FAIL);
                    	if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_STATUS))){
                    		t_status = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_STATUS));
                    	}
                    	String t_node = conf.get(StrVal.DEFAULT_VAL_NAN);
                    	if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_NODE))){
                    		t_node = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_NODE));
                    	}
                    	String t_txdate = conf.get(StrVal.DEFAULT_VAL_NAN);
                    	if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))){
                    		t_txdate = jsonmstctl.getString(conf.get(StrVal.JSON_KEY_TXDATE));
                    	}
                    	long t_starttime = -1;
                    	if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_STARTTIME))){
                    		t_starttime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_STARTTIME));
                    	}
                    	long t_endtime = -1;
                    	if(jsonmstctl.containsKey(conf.get(StrVal.JSON_KEY_ENDTIME))){
                    		t_endtime = jsonmstctl.getLong(conf.get(StrVal.JSON_KEY_ENDTIME));
                    	}
                    	if(t_starttime==-1){
                    		t_starttime = System.currentTimeMillis();
                    	}
                    	if(t_endtime==-1){
                    		t_endtime = System.currentTimeMillis();
                    	}
                    	log.info(" Move "+ctl+" from "+mstStatusPath+" to "+batchNumPath+".");
    					UtilMth.mvNode(zkClient, mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, batchNumPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, jsonmstctl);
    					
                    	log.info("Remove "+ctl+" from memory,status ["+t_status+"].");
                    	jobstatusM.remove(ctl);
                    	list.add(ctl);
                    	try {
    						pst.setString(1,(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL));
    						pst.setString(2,(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS));
    						pst.setString(3,(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB));
    						pst.setString(4,t_node);
    						pst.setString(5,t_status);
    						pst.setString(6,batchNum);
    						pst.setString(7,t_txdate);
    						pst.setString(8,timestampformat.format(t_starttime));
    						pst.setString(9,timestampformat.format(t_endtime));
    						pst.addBatch();
//    						log.info("1. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_POOL)+
//    								 "2. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_SYS)+
//    								 "3. =>"+(String)ctlInfoMap.get(StrVal.MAP_KEY_JOB)+
//    								 "4. =>"+t_node+
//    								 "5. =>"+t_status+
//    								 "6. =>"+batchNum+
//    								 "7. =>"+t_txdate+
//    								 "7. =>"+timestampformat.format(t_starttime)+
//    								 "8. =>"+timestampformat.format(t_endtime)
//    								 );
    					} catch (SQLException e) {
    						e.printStackTrace();
    					}
					}else{
						log.info("Delete not "+ctl+",node info null.");
						zkClient.delete(mstStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl);
						log.info("Remove "+ctl+" from memory,node info null.");
	                	jobstatusM.remove(ctl);
					}
				}
			}else{
				log.info(ErrorCode.FAILURE_00001+mstStatusPath+".");
			}
			try {
				if(list.size()>0||liststatus.size()>0){
					if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
						log.info("Result status size:"+list.size()+".");
						log.info("Temp status size:"+liststatus.size()+".");
					}
					if(list.size()>0){
						pst.executeBatch();
					}
					if(liststatus.size()>0){
						psti.executeBatch();
					}
					conn.commit();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}finally{
//				try {
//					pst.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
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
