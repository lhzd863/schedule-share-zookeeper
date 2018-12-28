package com.batch.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.master.DefaultConf;
import com.batch.util.Configuration;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SubmitResource implements Runnable{

	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	String node = "";
	String cfg = "";
	Map runningMap = null;
	final static String clss = "slv";
	
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
    	this.node = node;
    	runningMap = (Map) shareM.get(StrVal.MAP_KEY_RUNNING);
	}
	
	@Override
	public void run() {
		long starttime = System.currentTimeMillis();
		while(true){
			//init log
			SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
			Date nowTime = new Date();
			String yyyymmdd = time.format(nowTime);
//			log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), node+"_"+yyyymmdd);
			if(log==null){
				LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), node+"_"+yyyymmdd);
				log = LoggerFactory.getLogger(SubmitResource.class);
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
			JSONObject jsonobj = new JSONObject();
			//statistics resource
			log.info(node+" Submit slave resource.");
			String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
			String slvNodeListPath = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SLV_LIST);
			//statistics cpu
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(node+" node exe cmd statistics "+conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT)+"["+conf.get(StrVal.SLV_STAT_CPU)+"].");
			}
			String cpu = exeSystemCmd(conf.get(StrVal.SLV_STAT_CPU));
			jsonobj.put(conf.get(StrVal.JSON_KEY_CPUPCT), cpu);
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(node+" node exe cmd statistics "+conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT)+"["+conf.get(StrVal.SLV_STAT_MEM)+"].");
			}
			//statistics mem
			String mem = exeSystemCmd(conf.get(StrVal.SLV_STAT_MEM));
			jsonobj.put(conf.get(StrVal.JSON_KEY_MEMPCT), mem);
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(node+" delete node resource ctl.");
			}
			jsonobj.put(conf.get(StrVal.JSON_KEY_PROCESSCNT), runningMap.size());
			//slv time
			long slvtime = System.currentTimeMillis();
			jsonobj.put(conf.get(StrVal.JSON_KEY_SLVTIME), slvtime);
			
			//write info
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(node+" node resource "+conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT)+" ["+cpu+"].");
				log.info(node+" node resource "+conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT)+" ["+mem+"].");
				log.info(node+" node resource "+conf.get(StrVal.SYSTEM_PARAMETER_PRCCNT)+" ["+runningMap.size()+"].");
			}
			if(zkClient.exists(slvNodeListPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node)){
				zkClient.writeData(slvNodeListPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node, jsonobj);
			}else{
				zkClient.create(slvNodeListPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node, jsonobj,CreateMode.PERSISTENT);
			}
			//sleep
			try {
				Thread.sleep(Long.parseLong(conf.get(StrVal.BATCH_TIME_HEARTBEADT)));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public String exeSystemCmd(String cmd){
		Process p = null;
		InputStream is = null;
		BufferedReader reader = null;
		String line = null;
		String res = "";
		try {
			p = Runtime.getRuntime().exec(cmd);
			is = p.getInputStream();
			reader = new BufferedReader(new InputStreamReader(is));
			while((line=reader.readLine())!=null){
				res = line;
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(p.waitFor()!=0){
					
				}
				is.close();
				reader.close();
				p.destroy();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return res;
	}

}
