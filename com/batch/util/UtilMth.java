package com.batch.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.CreateMode;


import net.sf.json.JSONObject;

public class UtilMth {

	
	public static Map readCTLFile(Configuration conf,String fileName){
		File file = new File(fileName);
		Map map = new HashMap();
		if(!file.exists()){
			return map;
		}
		String result = null;
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		try {
			String read = null;
			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);
			String key = null;
			String val = null;
			while((read=bufferedReader.readLine())!=null){
				String[] keyval = read.split(conf.get(StrVal.DELIMITER_PARAMETER_KEYVAL));
				String tmpkey = keyval[0].toLowerCase();
				String tmpval = keyval[1];
				if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_TRIGGERTYPE))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_TRIGGERTIME))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_PRIORITY))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_PRCCNT))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_TIMESTAMP))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_STATUS))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_NODE))){
					key = tmpkey;
					val = tmpval;
				}else if(tmpkey.equals(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE))){
					key = tmpkey;
					val = tmpval;
				}
				if(key!=null){
					map.put(key, val);
				}
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
		return map;
	}
	
	public static void writeCTLFile(String content ,String filename) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(filename,true));
			bw.write(content);
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
	}
	
	public static void writeBlankFile(String file){
		if(new File(file).exists()){
			FileWriter fw = null;
			try {
				fw = new FileWriter(new File(file));
				fw.write("");
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}else{
			try {
				new File(file).createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static boolean copyCTLFile(String spath,String sfilenm,String tpath,String tfilenm) {
		int bytesum = 0;
		int byteread = 0;
		File sfile = new File(spath+"/"+sfilenm);
		File tfile = new File(tpath+"/"+tfilenm);
		
		if(sfile.exists()) {
			InputStream inStream=null;
			FileOutputStream fs = null;
			try {
				inStream = new FileInputStream(sfile);
				fs = new FileOutputStream(tfile);
				byte[] buffer = new byte[1444];
				int length;				
			    while((byteread = inStream.read(buffer))!=-1) {
					bytesum+=byteread;
					fs.write(buffer,0,byteread);
			    }
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				try {
					inStream.close();
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}		
		}else{
			return false;
		}
		return true;
     }
	
	
	public static Map getCtlInfo(Configuration conf,Map shareM) {
		Map ctlInfoM = (Map) shareM.get(StrVal.MAP_KEY_CTL);
		if(conf.get(StrVal.BATCH_TYPE_CONFIG).equals("file")){
			File fl = new File(conf.get(StrVal.BATCH_PATH_FILE));
			if(!fl.exists()){
				return ctlInfoM;
			}
			String result = null;
			FileReader fileReader = null;
			BufferedReader bufferedReader = null;
			try {
				String read = null;
				fileReader = new FileReader(fl);
				bufferedReader = new BufferedReader(fileReader);
				String key = null;
				String val = null;
				while((read=bufferedReader.readLine())!=null){
					String[] keyval = read.split(conf.get(StrVal.DELIMITER_FILE_CONFIG));
					if(keyval.length<2){
						continue;
					}
					String ctl = keyval[0].toLowerCase();
					String cron = keyval[1];
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
			
		}else if(conf.get(StrVal.BATCH_TYPE_CONFIG).equals("database")){
			
		}else{
			
		}
		return ctlInfoM;
	}
	
	public static Connection getConnectionDB(Configuration conf) {
		DBConnect dbct = new DBConnect(conf.get(StrVal.BATCH_DB_IP), conf.get(StrVal.BATCH_DB_PORT),conf.get(StrVal.BATCH_DB_DEFAULTDB), conf.get(StrVal.BATCH_DB_USR), conf.get(StrVal.BATCH_DB_PASSWD),  conf.get(StrVal.BATCH_DB_TYPE));
		Connection conn = dbct.getDBConnection();
		return conn; 
	}
	
	public static Map getCTLInfoMap(Configuration conf,String ctl){
		String[] ctlarr = ctl.split(conf.get(StrVal.DELIMITER_NODE_CTL));
		String batchnum = ctlarr[0];
		String pool = ctlarr[1];
		String sys = ctlarr[2];
		String job = ctlarr[3];
		Map tmpMap = new HashMap();
		tmpMap.put(conf.get(StrVal.MAP_KEY_POOL), pool);
		tmpMap.put(conf.get(StrVal.MAP_KEY_SYS), sys);
		tmpMap.put(conf.get(StrVal.MAP_KEY_JOB), job);
		tmpMap.put(conf.get(StrVal.MAP_KEY_BATCHNUM), batchnum);
		return tmpMap;
	}
	
	public static void mvNode(ZkClient zkClient,String srcNode,String tarNode,JSONObject jsonobj) {
		boolean e = false;
		if(zkClient.exists(tarNode)){
			zkClient.delete(tarNode);
		}
		if(zkClient.exists(srcNode)){
			zkClient.delete(srcNode);
		}
		zkClient.create(tarNode,jsonobj.toString(),CreateMode.PERSISTENT);
	}
	
	public static boolean isJsonNull(Object obj){
		if(obj==null||obj.toString().equals("null")){
			return true;
		}
		return false;
	}
	
}
