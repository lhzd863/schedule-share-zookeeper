package com.batch.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.batch.master.TimeTiggerJob;
import com.batch.util.Configuration;
import com.batch.util.UtilMth;
import com.batch.util.ZkClient;

import net.sf.json.JSONObject;

import com.batch.util.LoggerUtil;
import com.batch.util.StrVal;

public class SlaveExecuter implements Runnable{
	
	Configuration conf = null;
	Logger log = null;
	Map shareM = null;
	String ctl = "";
	String node = "";
	String yyyymmdd = null;
	Map runningMap = null;
	
	ZkClient zkClient = null;
	
	final static String clss = "slv";
	public void initialize(String node,Configuration conf,Map shareM,ZkClient zkc){
		this.conf = conf;
    	this.shareM = shareM;
    	this.node = node;
    	runningMap = (Map) shareM.get(StrVal.MAP_KEY_RUNNING);
    	
    	zkClient = zkc;
    	
    	SimpleDateFormat time = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String yyyymmdd = time.format(nowTime);
//		log = new LoggerUtil().getLoggerByName(conf.get(StrVal.BATCH_PATH_LOG), node+"_exe_"+yyyymmdd);
		if(log==null){
			LoggerUtil.setLogFileName(conf.get(StrVal.BATCH_PATH_LOG), node+"_exe_"+yyyymmdd);
			log = LoggerFactory.getLogger(SlaveExecuter.class);
		}
	}

	public boolean execmd(String cmd){
		//log
		SimpleDateFormat time = new SimpleDateFormat("yyyyMMddHHmmss");
		SimpleDateFormat timedate = new SimpleDateFormat("yyyyMMdd");
		Date nowTime = new Date();
		String timedir = time.format(nowTime);
		long  starttime = System.currentTimeMillis();
		File logdir = new File(conf.get(StrVal.BATCH_PATH_LOG)+"/"+timedate.format(nowTime));
		if(!logdir.exists()){
			log.info("mkdir "+logdir.getAbsolutePath()+".");
			logdir.mkdirs();
		}
		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
			log.info(node+" node exe cmd ["+cmd+"].");
		}
//		Logger joblog = new LoggerUtil().getLoggerByName(logdir.getAbsolutePath(), ctl+"_"+timedir);
		LoggerUtil.setLogFileName(logdir.getAbsolutePath(), ctl+"_"+timedir);
		Logger joblog = LoggerFactory.getLogger(SlaveExecuter.class);
		
		//parameter
		boolean res = false;
		ArrayList cmdarr = new ArrayList<String>();
		String[] args = cmd.split(" ");
		for (int k=0; k<args.length;k++){
			cmdarr.add(args[k]);
		}
		ProcessBuilder pb = new ProcessBuilder(cmdarr);
		Process processWork;
		try {
			//execute script
			processWork = pb.start();
			BufferedReader jobOut = new BufferedReader(new InputStreamReader(processWork.getInputStream()));
			String line;
			String ret = "";
			while ( (line = jobOut.readLine()) != null ) {
				ret +=line+"\n";
				joblog.info(line);
			}
			jobOut.close();
			try {
				processWork.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			int returncode = processWork.exitValue();
			if(returncode==0){
				res = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}

	public String replaceVarValue(String str,String var,String val){
		String ret = str.replaceAll("\\$\\{"+var+"\\}", val);
		return ret;
	}

	@Override
	public void run() {
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String mstParaPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_PARAMETER);
		String mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		String slvStatusPath = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS);
		//
		if(!zkClient.exists(slvStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
			JSONObject slvobj = new JSONObject();
			zkClient.create(slvStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl, slvobj.toString(), CreateMode.PERSISTENT);
		}
		
		String[] arr = ctl.split(conf.get(StrVal.DELIMITER_NODE_CTL));
		String batchnumber = arr[0];

		JSONObject jsonmstPara = null;
		if(zkClient.exists(mstParaPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchnumber)&&
		   !zkClient.isNullNode(mstParaPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchnumber)&&
		   zkClient.readData(mstParaPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchnumber)!=null&&
		   zkClient.readData(mstParaPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchnumber).toString().length()>0){
			jsonmstPara = JSONObject.fromObject(zkClient.readData(mstParaPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchnumber));
		}else{
			log.info(mstParaPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+batchnumber+" not exists or is null.");
			jsonmstPara = new JSONObject();
		}
		JSONObject jsonmstLock = null;
		if(!zkClient.exists(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl)){
			log.info(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl+" ctl not exists.");
			long  endtime = System.currentTimeMillis();
			jsonmstLock = new JSONObject();
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_NODE), node);
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_STARTTIME), endtime);
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_ENDTIME), endtime);
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
			String txdatetmp = "";
			if(jsonmstLock.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&(UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE)))||jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE)).length()!=8)){
				SimpleDateFormat yyyymmddfmt = new SimpleDateFormat("yyyyMMdd");
				Date nowTimestarttmp = new Date();
				txdatetmp = yyyymmddfmt.format(nowTimestarttmp);
				jsonmstLock.put(conf.get(StrVal.JSON_KEY_TXDATE), txdatetmp);
			}
			endjobstatus(jsonmstLock);
			return ;	
		}
		jsonmstLock = JSONObject.fromObject(zkClient.readData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+ctl));
		if(UtilMth.isJsonNull(jsonmstLock)){
			log.info(ctl+" no data execute.");
			long  endtime = System.currentTimeMillis();
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_ENDTIME),endtime);
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_STATUS),conf.get(StrVal.SYSTEM_STATUS_FAIL));
			String txdatetmp = "";
			if(jsonmstLock.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&(UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE)))||jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE)).length()!=8)){
				SimpleDateFormat yyyymmddfmt = new SimpleDateFormat("yyyyMMdd");
				Date nowTimestarttmp = new Date();
				txdatetmp = yyyymmddfmt.format(nowTimestarttmp);
				jsonmstLock.put(conf.get(StrVal.JSON_KEY_TXDATE),txdatetmp);
			}
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_TXDATE),txdatetmp);
			endjobstatus(jsonmstLock);
			return ;	
		}
		String cmd = "";
		//system
		if(!UtilMth.isJsonNull(jsonmstPara)&&jsonmstPara.containsKey(conf.get(StrVal.JSON_KEY_CMD))&&!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_CMD)))){
			cmd = jsonmstPara.getString(conf.get(StrVal.JSON_KEY_CMD));
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(ctl+" system cmd ["+cmd+"].");
			}
		}else{
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(ctl+" not exists System cmd.");
			}
		}
		//job
		if(jsonmstLock.containsKey(conf.get(StrVal.JSON_KEY_CMD))&&!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_CMD)))){
			cmd = jsonmstLock.getString(conf.get(StrVal.JSON_KEY_CMD));
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(ctl+" cmd ["+cmd+"].");
			}
		}else{
			if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
				log.info(ctl+" not exists cmd.");
			}
		}
		if(cmd.length()<1){
			String apppath =  conf.get(StrVal.BATCH_PATH_SCRIPT);
			String[] arrS = ctl.split(conf.get(StrVal.DELIMITER_NODE_CTL));
			String pools = arrS[1];
			String sys = arrS[2];
			String job = arrS[3];
			String scriptPath = apppath + conf.get(StrVal.DELIMITER_NODE_LEVEL) + pools + conf.get(StrVal.DELIMITER_NODE_LEVEL) + sys + conf.get(StrVal.DELIMITER_NODE_LEVEL) + job +conf.get(StrVal.DELIMITER_NODE_LEVEL)+"bin";
			File scriptf = new File(scriptPath);
			if(scriptf.exists()){
				String[] scriptlst = scriptf.list();
				if(scriptlst.length>0){
					Arrays.sort(scriptlst);
				}
				for(int k=0;k<scriptlst.length;k++){
					String tmp1file = scriptlst[k];
					if(tmp1file.endsWith(".pl")){
						cmd += "perl "+scriptPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+tmp1file+" "+conf.get(StrVal.SYSTEM_PARAMETER_FORMAT)+" "+conf.get(StrVal.DELIMITER_CMD_LINE);
					}else if(tmp1file.endsWith(".sh")){
						cmd += "sh "+scriptPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+tmp1file+" "+conf.get(StrVal.SYSTEM_PARAMETER_FORMAT)+" "+conf.get(StrVal.DELIMITER_CMD_LINE);
					}else if(tmp1file.endsWith(conf.get(StrVal.BATCH_SCRIPT_ENDWITH))){
						cmd += conf.get(StrVal.BATCH_SCRIPT_ENDWITH_CMD)+" "+scriptPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+tmp1file+" "+conf.get(StrVal.SYSTEM_PARAMETER_FORMAT)+" "+conf.get(StrVal.DELIMITER_CMD_LINE);
					}
					if(!new File(scriptPath).exists()){
						log.info("Script not exists:"+scriptPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+tmp1file);
					}
				}
			}else{
				log.info(scriptPath+" not exists.");
				long  endtime = System.currentTimeMillis();
				jsonmstLock.put(conf.get(StrVal.JSON_KEY_ENDTIME), endtime);
				jsonmstLock.put(conf.get(StrVal.JSON_KEY_STATUS), conf.get(StrVal.SYSTEM_STATUS_FAIL));
				String txdatetmp = "";
				if(jsonmstLock.containsKey(conf.get(StrVal.JSON_KEY_TXDATE))&&(UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE)))||jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE)).length()!=8)){
					SimpleDateFormat yyyymmddfmt = new SimpleDateFormat("yyyyMMdd");
					Date nowTimestarttmp = new Date();
					txdatetmp = yyyymmddfmt.format(nowTimestarttmp);
					jsonmstLock.put(conf.get(StrVal.JSON_KEY_TXDATE),txdatetmp);
				}
				endjobstatus(jsonmstLock);
				return;
			}
		}
		if(conf.get(StrVal.BATCH_FLAG_DEBUG).toUpperCase().trim().equals(StrVal.VAL_CONSTANT_Y)){
			log.info(ctl+" execute cmd["+cmd+"].");
		}
		cmd = replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_CONTROLFILE),ctl);
		String txdate = "";
		//job
		if(!UtilMth.isJsonNull(jsonmstLock)&&jsonmstLock.containsKey(conf.get(StrVal.JSON_KEY_POOL))){
			cmd = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_POOL))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_POOL),jsonmstLock.getString(conf.get(StrVal.JSON_KEY_POOL))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_SYS))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_SYS),jsonmstLock.getString(conf.get(StrVal.JSON_KEY_SYS))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_JOB))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_JOB),jsonmstLock.getString(conf.get(StrVal.JSON_KEY_JOB))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_NODE))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_NODE),jsonmstLock.getString(conf.get(StrVal.JSON_KEY_NODE))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_JOBTYPE))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_JOBTYPE),jsonmstLock.getString(conf.get(StrVal.JSON_KEY_JOBTYPE))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_NUM))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_BATCHNUM),jsonmstLock.getString(conf.get(StrVal.JSON_KEY_NUM))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_TXDATE),jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE))):cmd;
			txdate = (!UtilMth.isJsonNull(jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE))))?jsonmstLock.getString(conf.get(StrVal.JSON_KEY_TXDATE)):txdate;
		}
		//system
		if(!UtilMth.isJsonNull(jsonmstPara)&&jsonmstPara.containsKey(conf.get(StrVal.JSON_KEY_POOL))){
			cmd = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_POOL))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_POOL),jsonmstPara.getString(conf.get(StrVal.JSON_KEY_POOL))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_SYS))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_SYS),jsonmstPara.getString(conf.get(StrVal.JSON_KEY_SYS))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_JOB))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_JOB),jsonmstPara.getString(conf.get(StrVal.JSON_KEY_JOB))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_NODE))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_NODE),jsonmstPara.getString(conf.get(StrVal.JSON_KEY_NODE))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_JOBTYPE))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_JOBTYPE),jsonmstPara.getString(conf.get(StrVal.JSON_KEY_JOBTYPE))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_NUM))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_BATCHNUM),jsonmstPara.getString(conf.get(StrVal.JSON_KEY_NUM))):cmd;
			cmd = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_TXDATE))))?replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_TXDATE),jsonmstPara.getString(conf.get(StrVal.JSON_KEY_TXDATE))):cmd;
			txdate = (!UtilMth.isJsonNull(jsonmstPara.getString(conf.get(StrVal.JSON_KEY_TXDATE))))?jsonmstPara.getString(conf.get(StrVal.JSON_KEY_TXDATE)):txdate;
		}
		Date nowTimestart = new Date();
		if(txdate.length()<1){
			SimpleDateFormat yyyymmddfmt = new SimpleDateFormat("yyyyMMdd");
			//txdate replace
			txdate = yyyymmddfmt.format(nowTimestart);
			cmd = replaceVarValue(cmd,conf.get(StrVal.CMD_PARAMETER_TXDATE),txdate);
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_TXDATE),txdate);
		}
		//
		String[] cmdarr = cmd.split(conf.get(StrVal.DELIMITER_CMD_LINE));
		
		boolean res = false;
		for(int j=0;j<cmdarr.length;j++){
			if(cmdarr[j].length()<1){
				continue;
			}
			log.info("System will executer command ["+cmdarr[j]+"].");
			res = execmd(cmdarr[j]);
			if(!res){
				break;
			}
		}
		if(res){
			log.info(ctl+" running result "+conf.get(StrVal.SYSTEM_STATUS_SUCCESS)+".");
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_STATUS),conf.get(StrVal.SYSTEM_STATUS_SUCCESS));
		}else{
			log.info(ctl+" running result "+conf.get(StrVal.SYSTEM_STATUS_FAIL)+".");
			jsonmstLock.put(conf.get(StrVal.JSON_KEY_STATUS),conf.get(StrVal.SYSTEM_STATUS_FAIL));
		}
		long  endtime = System.currentTimeMillis();
		jsonmstLock.put(conf.get(StrVal.JSON_KEY_ENDTIME),endtime);
		endjobstatus(jsonmstLock);
	}

	public String getCtl() {
		return ctl;
	}

	public void setCtl(String ctl) {
		this.ctl = ctl;
	}
	
	public  void endjobstatus(JSONObject josnobj){
		String slvPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_SLV);
		String mstPath = conf.get(StrVal.BATCH_PATH_HOME)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_INS)+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_SYS_MST);
		String slvNodePath = slvPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+node;
		String slvStatusPath = slvNodePath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_STATUS);
		String mstLockPath = mstPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+conf.get(StrVal.NODE_MST_LOCK);
		log.info(getCtl()+" update status to ["+josnobj.getString(conf.get(StrVal.JSON_KEY_STATUS))+"],end job.");
		zkClient.writeData(mstLockPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+getCtl(), josnobj);
		zkClient.writeData(slvStatusPath+conf.get(StrVal.DELIMITER_NODE_LEVEL)+getCtl(), josnobj);
		runningMap.remove(ctl);
		log.info(ctl+" running result "+josnobj.getString(conf.get(StrVal.JSON_KEY_STATUS))+".");
	}

}
