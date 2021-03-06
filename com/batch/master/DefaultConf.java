package com.batch.master;

import com.batch.util.Configuration;
import com.batch.util.StrVal;

public class DefaultConf {

    public static void initialize(Configuration conf){
    	if(conf.get(StrVal.NODE_SYS_INS)==null){
			conf.set(StrVal.NODE_SYS_INS,"ins");
		}
    	if(conf.get(StrVal.NODE_SYS_MST)==null){
			conf.set(StrVal.NODE_SYS_MST,"mst");
		}
    	if(conf.get(StrVal.NODE_SYS_SLV)==null){
			conf.set(StrVal.NODE_SYS_SLV,"slv");
		}
    	if(conf.get(StrVal.NODE_MST_LOCK)==null){
			conf.set(StrVal.NODE_MST_LOCK,"lock");
		}
    	if(conf.get(StrVal.NODE_MST_STREAM)==null){
			conf.set(StrVal.NODE_MST_STREAM,"stream");
		}
    	if(conf.get(StrVal.NODE_MST_STATUS)==null){
			conf.set(StrVal.NODE_MST_STATUS,"status");
		}
    	if(conf.get(StrVal.NODE_MST_NOTICE)==null){
			conf.set(StrVal.NODE_MST_NOTICE,"notice");
		}
    	if(conf.get(StrVal.NODE_MST_QUEUE)==null){
			conf.set(StrVal.NODE_MST_QUEUE,"queue");
		}
    	if(conf.get(StrVal.NODE_MST_COMPLETED)==null){
			conf.set(StrVal.NODE_MST_COMPLETED,"completed");
		}
    	if(conf.get(StrVal.NODE_SLV_LIST)==null){
			conf.set(StrVal.NODE_SLV_LIST,"list");
		}
    	if(conf.get(StrVal.NODE_SLV_STATUS)==null){
			conf.set(StrVal.NODE_SLV_STATUS,"status");
		}
    	if(conf.get(StrVal.NODE_MST_PARAMETER)==null){
			conf.set(StrVal.NODE_MST_PARAMETER,"parameter");
		}
    	
    	if(conf.get(StrVal.DELIMITER_NODE_LEVEL)==null){
			conf.set(StrVal.DELIMITER_NODE_LEVEL,"/");
		}
    	if(conf.get(StrVal.DELIMITER_PARAMETER_KEYVAL)==null){
			conf.set(StrVal.DELIMITER_PARAMETER_KEYVAL,"=");
		}
    	if(conf.get(StrVal.DELIMITER_PARAMETER_ROW)==null){
			conf.set(StrVal.DELIMITER_PARAMETER_ROW,"\n");
		}
    	if(conf.get(StrVal.DELIMITER_NODE_CTL)==null){
			conf.set(StrVal.DELIMITER_NODE_CTL,"\\.");
		}
    	if(conf.get(StrVal.DELIMITER_CTL_BATCHNUM)==null){
			conf.set(StrVal.DELIMITER_CTL_BATCHNUM,".");
		}
    	if(conf.get(StrVal.DELIMITER_CTL_REGION)==null){
			conf.set(StrVal.DELIMITER_CTL_REGION,".");
		}
    	if(conf.get(StrVal.DELIMITER_CMD_LINE)==null){
			conf.set(StrVal.DELIMITER_CMD_LINE,";");
		}
    	if(conf.get(StrVal.DELIMITER_RECORD_TIRGGER)==null){
			conf.set(StrVal.DELIMITER_RECORD_TIRGGER,"=>");
		}
    	if(conf.get(StrVal.DELIMITER_PATH_DIR)==null){
			conf.set(StrVal.DELIMITER_PATH_DIR,"/");
		}
    	if(conf.get(StrVal.DELIMITER_CMD_LINE)==null){
			conf.set(StrVal.DELIMITER_CMD_LINE,";");
		}
    	if(conf.get(StrVal.SYSTEM_STATUS_RUNNING)==null){
			conf.set(StrVal.SYSTEM_STATUS_RUNNING,"Running");
		}
    	if(conf.get(StrVal.SYSTEM_STATUS_READY)==null){
			conf.set(StrVal.SYSTEM_STATUS_READY,"Ready");
		}
    	if(conf.get(StrVal.SYSTEM_STATUS_FAIL)==null){
			conf.set(StrVal.SYSTEM_STATUS_FAIL,"Fail");
		}
    	if(conf.get(StrVal.SYSTEM_STATUS_SUCCESS)==null){
			conf.set(StrVal.SYSTEM_STATUS_SUCCESS,"Success");
		}
    	if(conf.get(StrVal.SYSTEM_STATUS_PENDING)==null){
			conf.set(StrVal.SYSTEM_STATUS_PENDING,"Pending");
		}
    	if(conf.get(StrVal.SYSTEM_STATUS_SUBMIT)==null){
			conf.set(StrVal.SYSTEM_STATUS_SUBMIT,"Submit");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_TRIGGERTYPE)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_TRIGGERTYPE,"triggertype");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_TRIGGERTIME)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_TRIGGERTIME,"triggertime");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_TXDATE)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_TXDATE,"txdate");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_PRIORITY)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_PRIORITY,"priority");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_CPUPNT)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_CPUPNT,"cpupnt");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_MEMPNT)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_MEMPNT,"mempnt");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_PRCCNT)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_PRCCNT,"prccnt");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_TIMESTAMP)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_TIMESTAMP,"timestamp");
		}
    	if(conf.get(StrVal.LMT_SLV_CPU)==null){
			conf.set(StrVal.LMT_SLV_CPU,"98");
		}
    	if(conf.get(StrVal.LMT_SLV_MEM)==null){
			conf.set(StrVal.LMT_SLV_MEM,"95");
		}
    	if(conf.get(StrVal.LMT_SLV_PRC)==null){
			conf.set(StrVal.LMT_SLV_PRC,"30");
		}
    	if(conf.get(StrVal.LMT_JOB_TIME)==null){
			conf.set(StrVal.LMT_JOB_TIME,"108000000");
		}
    	
    	if(conf.get(StrVal.SYSTEM_PARAMETER_STARTTIME)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_STARTTIME,"starttime");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_ENDTIME)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_ENDTIME,"endtime");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_NODE)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_NODE,"node");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_STATUS)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_STATUS,"status");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_FORMAT)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_FORMAT,"${ctl}_${txdate}");
		}
    	if(conf.get(StrVal.SYSTEM_PARAMETER_SEQUENCE)==null){
			conf.set(StrVal.SYSTEM_PARAMETER_SEQUENCE,"sequence");
		}
    	//job attribute
    	if(conf.get(StrVal.JOB_ATTR_POOL)==null){
			conf.set(StrVal.JOB_ATTR_POOL,"pool");
		}
    	if(conf.get(StrVal.JOB_ATTR_SYSTEM)==null){
			conf.set(StrVal.JOB_ATTR_SYSTEM,"system");
		}
    	if(conf.get(StrVal.JOB_ATTR_JOB)==null){
			conf.set(StrVal.JOB_ATTR_JOB,"job");
		}
    	if(conf.get(StrVal.JOB_ATTR_NODE)==null){
			conf.set(StrVal.JOB_ATTR_NODE,"node");
		}
    	if(conf.get(StrVal.JOB_ATTR_JOBTYPE)==null){
			conf.set(StrVal.JOB_ATTR_JOBTYPE,"jobtype");
		}
    	if(conf.get(StrVal.JOB_ATTR_CHECKTIMEWINDOW)==null){
			conf.set(StrVal.JOB_ATTR_CHECKTIMEWINDOW,"checktimewindow");
		}
    	if(conf.get(StrVal.JOB_ATTR_CHECKTIMETRIGGER)==null){
			conf.set(StrVal.JOB_ATTR_CHECKTIMETRIGGER,"checktimetrigger");
		}
    	if(conf.get(StrVal.JOB_ATTR_CHECKCALENDAR)==null){
			conf.set(StrVal.JOB_ATTR_CHECKCALENDAR,"checkcalendar");
		}
    	if(conf.get(StrVal.JOB_ATTR_CHECKLASTSTATUS)==null){
			conf.set(StrVal.JOB_ATTR_CHECKLASTSTATUS,"checklaststatus");
		}
    	if(conf.get(StrVal.JOB_ATTR_PRIORITY)==null){
			conf.set(StrVal.JOB_ATTR_PRIORITY,"priority");
		}
    	if(conf.get(StrVal.JOB_ATTR_ENABLE)==null){
			conf.set(StrVal.JOB_ATTR_ENABLE,"enable");
		}
    	if(conf.get(StrVal.JOB_ATTR_TRIGGER_TYPE)==null){
			conf.set(StrVal.JOB_ATTR_TRIGGER_TYPE,"triggertype");
		}
    	if(conf.get(StrVal.JOB_ATTR_EXPRESSION)==null){
			conf.set(StrVal.JOB_ATTR_EXPRESSION,"expression");
		}
    	if(conf.get(StrVal.JOB_ATTR_CMD)==null){
			conf.set(StrVal.JOB_ATTR_CMD,"cmd");
		}
    	//Map key
    	if(conf.get(StrVal.MAP_KEY_POOL)==null){
			conf.set(StrVal.MAP_KEY_POOL,"pool");
		}
    	if(conf.get(StrVal.MAP_KEY_SYS)==null){
			conf.set(StrVal.MAP_KEY_SYS,"sys");
		}
    	if(conf.get(StrVal.MAP_KEY_JOB)==null){
			conf.set(StrVal.MAP_KEY_JOB,"job");
		}
    	if(conf.get(StrVal.MAP_KEY_BATCHNUM)==null){
			conf.set(StrVal.MAP_KEY_BATCHNUM,"batchnum");
		}
    	if(conf.get(StrVal.MAP_KEY_TXDATE)==null){
			conf.set(StrVal.MAP_KEY_TXDATE,"txdate");
		}
    	if(conf.get(StrVal.MAP_KEY_RUNNING)==null){
			conf.set(StrVal.MAP_KEY_RUNNING,"running");
		}
    	//config sql
    	if(conf.get(StrVal.SQL_SHD_JOB_INFO)==null){
			conf.set(StrVal.SQL_SHD_JOB_INFO,"select * from test.shd_job_info");
		}
    	if(conf.get(StrVal.SQL_SHD_JOB_TRIGGER)==null){
			conf.set(StrVal.SQL_SHD_JOB_TRIGGER,"select * from test.shd_job_trigger");
		}
    	if(conf.get(StrVal.SQL_SHD_JOB_STATUS)==null){
			conf.set(StrVal.SQL_SHD_JOB_STATUS,"insert into test.shd_job_status(Pool,Sys,Job,SlvNode,Status,batchnum,Txdate,StartTime,EndTime) values(?,?,?,?,?,?,?,?,?);");
		}
    	if(conf.get(StrVal.SQL_INSERT_STATUS)==null){
			conf.set(StrVal.SQL_INSERT_STATUS,"insert into test.shd_job_memory_status(Pool,Sys,Job,SlvNode,Status,batchnum,Txdate,StartTime,EndTime) values(?,?,?,?,?,?,?,?,?);");
		}
    	if(conf.get(StrVal.SQL_DELETE_STATUS)==null){
			conf.set(StrVal.SQL_DELETE_STATUS,"delete from ptemp.shd_job_memory_status ");
		}
    	if(conf.get(StrVal.SQL_SHD_JOB_STEP)==null){
			conf.set(StrVal.SQL_SHD_JOB_STEP,"select * from test.shd_job_step ");
		}
    	
    	if(conf.get(StrVal.BATCH_TIME_HEARTBEADT)==null){
			conf.set(StrVal.BATCH_TIME_HEARTBEADT,"10000");
		}
    	if(conf.get(StrVal.BATCH_TIME_REFRESH)==null){
			conf.set(StrVal.BATCH_TIME_REFRESH,"10000");
		}
    	if(conf.get(StrVal.BATCH_FLAG_RUNNING)==null){
			conf.set(StrVal.BATCH_FLAG_RUNNING,"0");
		}
    	if(conf.get(StrVal.BATCH_FLAG_RUNNING)==null){
			conf.set(StrVal.BATCH_FLAG_RUNNING,"1");
		}
    	if(conf.get(StrVal.BATCH_MAX_PRIORITH)==null){
			conf.set(StrVal.BATCH_MAX_PRIORITH,"100");
		}
    	if(conf.get(StrVal.BATCH_MIN_PRIORITH)==null){
			conf.set(StrVal.BATCH_MIN_PRIORITH,"0");
		}
    	if(conf.get(StrVal.BATCH_LOG_KEEPPERIOD)==null){
			conf.set(StrVal.BATCH_LOG_KEEPPERIOD,"7");
		}
    	//ZKC
    	if(conf.get(StrVal.BATCH_ZKC_IP)==null){
			conf.set(StrVal.BATCH_ZKC_IP,"192.168.1.6");
		}
    	if(conf.get(StrVal.BATCH_ZKC_PORT)==null){
			conf.set(StrVal.BATCH_ZKC_PORT,"2181");
		}
    	if(conf.get(StrVal.BATCH_ZKC_HOME)==null){
			conf.set(StrVal.BATCH_ZKC_HOME,"/BATCH");
		}
    	if(conf.get(StrVal.BATCH_FLAG_DEBUG)==null){
			conf.set(StrVal.BATCH_FLAG_DEBUG,"N");
		}
    	if(conf.get(StrVal.BATCH_TYPE_CONFIG)==null){
			conf.set(StrVal.BATCH_TYPE_CONFIG,"database");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_CONTROLFILE)==null){
			conf.set(StrVal.CMD_PARAMETER_CONTROLFILE,"CONTROLFILE");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_POOL)==null){
			conf.set(StrVal.CMD_PARAMETER_POOL,"POOL");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_SYS)==null){
			conf.set(StrVal.CMD_PARAMETER_SYS,"SYS");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_JOB)==null){
			conf.set(StrVal.CMD_PARAMETER_JOB,"JOB");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_NODE)==null){
			conf.set(StrVal.CMD_PARAMETER_NODE,"NODE");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_JOBTYPE)==null){
			conf.set(StrVal.CMD_PARAMETER_JOBTYPE,"JOBTYPE");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_BATCHNUM)==null){
			conf.set(StrVal.CMD_PARAMETER_BATCHNUM,"BATCHNUM");
		}
    	if(conf.get(StrVal.CMD_PARAMETER_TXDATE)==null){
			conf.set(StrVal.CMD_PARAMETER_TXDATE,"TXDATE");
		}
    	
    	if(conf.get(StrVal.SLV_STAT_CPU)==null){
			conf.set(StrVal.SLV_STAT_CPU,"vmstat |tail -1|awk -F ' ' '{printf (100-$15)}'");
		}
    	if(conf.get(StrVal.SLV_STAT_MEM)==null){
			conf.set(StrVal.SLV_STAT_MEM,"free|head -2|tail -1|awk -F ' ' '{printf (int)$3/$2*100}'");
		}
    	
    	//json key
    	if(conf.get(StrVal.JSON_KEY_POOL)==null){
			conf.set(StrVal.JSON_KEY_POOL,"pool");
		}
    	if(conf.get(StrVal.JSON_KEY_SYS)==null){
			conf.set(StrVal.JSON_KEY_SYS,"sys");
		}
    	if(conf.get(StrVal.JSON_KEY_JOB)==null){
			conf.set(StrVal.JSON_KEY_JOB,"job");
		}
    	if(conf.get(StrVal.JSON_KEY_NODE)==null){
			conf.set(StrVal.JSON_KEY_NODE,"node");
		}
    	if(conf.get(StrVal.JSON_KEY_STATUS)==null){
			conf.set(StrVal.JSON_KEY_STATUS,"status");
		}
    	if(conf.get(StrVal.JSON_KEY_ENABLE)==null){
			conf.set(StrVal.JSON_KEY_ENABLE,"enable");
		}
    	if(conf.get(StrVal.JSON_KEY_NUM)==null){
			conf.set(StrVal.JSON_KEY_NUM,"num");
		}
    	if(conf.get(StrVal.JSON_KEY_STARTTIME)==null){
			conf.set(StrVal.JSON_KEY_STARTTIME,"starttime");
		}
    	if(conf.get(StrVal.JSON_KEY_ENDTIME)==null){
			conf.set(StrVal.JSON_KEY_ENDTIME,"endtime");
		}
    	if(conf.get(StrVal.JSON_KEY_SEQ)==null){
			conf.set(StrVal.JSON_KEY_SEQ,"seq");
		}
    	if(conf.get(StrVal.JSON_KEY_CMD)==null){
			conf.set(StrVal.JSON_KEY_CMD,"cmd");
		}
    	if(conf.get(StrVal.JSON_KEY_TXDATE)==null){
			conf.set(StrVal.JSON_KEY_TXDATE,"txdate");
		}
    	if(conf.get(StrVal.JSON_KEY_EXPRESSION)==null){
			conf.set(StrVal.JSON_KEY_EXPRESSION,"expression");
		}
    	if(conf.get(StrVal.JSON_KEY_DESCRIPTION)==null){
			conf.set(StrVal.JSON_KEY_DESCRIPTION,"description");
		}
    	if(conf.get(StrVal.JSON_KEY_JOBTYPE)==null){
			conf.set(StrVal.JSON_KEY_JOBTYPE,"jobtype");
		}
    	if(conf.get(StrVal.JSON_KEY_CHECKTIMEWINDOW)==null){
			conf.set(StrVal.JSON_KEY_CHECKTIMEWINDOW,"checktimewindow");
		}
    	if(conf.get(StrVal.JSON_KEY_CHECKTIMETRIGGER)==null){
			conf.set(StrVal.JSON_KEY_CHECKTIMETRIGGER,"checktimetrigger");
		}
    	if(conf.get(StrVal.JSON_KEY_CHECKCALENDAR)==null){
			conf.set(StrVal.JSON_KEY_CHECKCALENDAR,"checkcalendar");
		}
    	if(conf.get(StrVal.JSON_KEY_CHECKLASTSTATUS)==null){
			conf.set(StrVal.JSON_KEY_CHECKLASTSTATUS,"checklaststatus");
		}
    	if(conf.get(StrVal.JSON_KEY_PRIORITY)==null){
			conf.set(StrVal.JSON_KEY_PRIORITY,"priority");
		}
    	if(conf.get(StrVal.JSON_KEY_TRIGGERTYPE)==null){
			conf.set(StrVal.JSON_KEY_TRIGGERTYPE,"triggertype");
		}
    	if(conf.get(StrVal.JSON_KEY_OFFSET)==null){
			conf.set(StrVal.JSON_KEY_OFFSET,"offset");
		}
    	if(conf.get(StrVal.JSON_KEY_CPUPCT)==null){
			conf.set(StrVal.JSON_KEY_CPUPCT,"cpupct");
		}
    	if(conf.get(StrVal.JSON_KEY_MEMPCT)==null){
			conf.set(StrVal.JSON_KEY_MEMPCT,"mempct");
		}
    	if(conf.get(StrVal.JSON_KEY_PROCESSCNT)==null){
			conf.set(StrVal.JSON_KEY_PROCESSCNT,"processcnt");
		}
    	if(conf.get(StrVal.JSON_KEY_SLVTIME)==null){
			conf.set(StrVal.JSON_KEY_SLVTIME,"slvtime");
		}
    	if(conf.get(StrVal.JSON_KEY_MODULE)==null){
			conf.set(StrVal.JSON_KEY_MODULE,"module");
		}
    	
    	//default
    	if(conf.get(StrVal.DEFAULT_VAL_NAN)==null){
			conf.set(StrVal.DEFAULT_VAL_NAN,"NaN");
		}
	}
}
