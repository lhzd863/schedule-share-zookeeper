package com.batch.util;

public class StrVal {
	//system directory
	public static final String NODE_SYS_INS = "node.sys.ins";
	public static final String NODE_SYS_MST = "node.sys.mst";
	public static final String NODE_SYS_SLV = "node.sys.slv";
	//master directory
	public static final String NODE_MST_LOCK = "node.mst.lock";
	public static final String NODE_MST_STREAM = "node.mst.stream";
	public static final String NODE_MST_STATUS = "node.mst.status";
	public static final String NODE_MST_NOTICE = "node.mst.notice";
	public static final String NODE_MST_QUEUE = "node.mst.queue";
	public static final String NODE_MST_COMPLETED = "node.mst.completed";
	public static final String NODE_MST_PARAMETER = "node.mst.parameter";
	//SLAVE directory
	public static final String NODE_SLV_STATUS = "node.slv.status";
	public static final String NODE_SLV_LIST = "node.slv.list";

	//System
	//status
	public static final String SYSTEM_STATUS_RUNNING = "system.status.running";
	public static final String SYSTEM_STATUS_READY = "system.status.ready";
	public static final String SYSTEM_STATUS_FAIL = "system.status.fail";
	public static final String SYSTEM_STATUS_SUCCESS = "system.status.success";
	public static final String SYSTEM_STATUS_PENDING = "system.status.pending";
	public static final String SYSTEM_STATUS_SUBMIT = "system.status.submit";
	//node
	
	//parameter
	public static final String SYSTEM_PARAMETER_TRIGGERTYPE = "system.parameter.tiggertype";
	public static final String SYSTEM_PARAMETER_TRIGGERTIME = "system.parameter.tiggertime";
	public static final String SYSTEM_PARAMETER_TXDATE = "system.parameter.txdate";
	public static final String SYSTEM_PARAMETER_PRIORITY = "system.parameter.priority";
	public static final String SYSTEM_PARAMETER_STATUS = "system.parameter.status";
	public static final String SYSTEM_PARAMETER_STARTTIME = "system.parameter.starttime";
	public static final String SYSTEM_PARAMETER_ENDTIME = "system.parameter.endtime";
	public static final String SYSTEM_PARAMETER_NODE = "system.parameter.node";
	public static final String SYSTEM_PARAMETER_SEQUENCE = "system.parameter.sequence";
	public static final String SYSTEM_PARAMETER_CMD = "system.parameter.cmd";
	public static final String SYSTEM_PARAMETER_CPUPNT = "system.parameter.cpupnt";
	public static final String SYSTEM_PARAMETER_MEMPNT = "system.parameter.mempnt";
	public static final String SYSTEM_PARAMETER_PRCCNT = "system.parameter.prccnt";
	public static final String SYSTEM_PARAMETER_TIMESTAMP = "system.parameter.timestamp";
	public static final String SYSTEM_PARAMETER_FORMAT = "system.parameter.format";
	//PARAMETER
	public static final String CMD_PARAMETER_CONTROLFILE = "cmd.parameter.controlfile";
	public static final String CMD_PARAMETER_POOL = "cmd.parameter.pool";
	public static final String CMD_PARAMETER_SYS = "cmd.parameter.sys";
	public static final String CMD_PARAMETER_JOB = "cmd.parameter.job";
	public static final String CMD_PARAMETER_NODE = "cmd.parameter.node";
	public static final String CMD_PARAMETER_JOBTYPE = "cmd.parameter.jobtype";
	public static final String CMD_PARAMETER_BATCHNUM = "cmd.parameter.batchnum";
	public static final String CMD_PARAMETER_TXDATE = "cmd.parameter.txdate";
	
	//delimiter
	public static final String DELIMITER_NODE_LEVEL = "delimiter.node.level";
	public static final String DELIMITER_PARAMETER_KEYVAL = "delimiter.parameter.keyval";
	public static final String DELIMITER_LOG_RANGE = "delimiter.log.range";
	public static final String DELIMITER_FILE_CONFIG = "delimiter.file.config";
	public static final String DELIMITER_PARAMETER_ROW = "delimiter.parameter.row";
	public static final String DELIMITER_NODE_CTL = "delimiter.node.ctl";
	public static final String DELIMITER_CTL_BATCHNUM = "delimiter.ctl.batchnum";
	public static final String DELIMITER_CMD_LINE = "delimiter.cmd.line";
	public static final String DELIMITER_RECORD_TIRGGER = "delimiter.record.trigger";
	public static final String DELIMITER_CTL_REGION = "delimiter.ctl.region";
	public static final String DELIMITER_PATH_DIR = "delimiter.path.dir";
	
	//limit
	public static final String LMT_SLV_CPU = "lmt.slv.cpu";
	public static final String LMT_SLV_MEM = "lmt.slv.mem";
	public static final String LMT_SLV_PRC = "lmt.slv.prc";	
	public static final String LMT_JOB_TIME = "lmt.job.time";
	
	//sql
	public static final String SQL_SHD_JOB_INFO = "sql.shd.job.info";
	public static final String SQL_SHD_JOB_TRIGGER = "sql.shd.job.trigger";
	public static final String SQL_SHD_JOB_STATUS = "sql.shd.job.status";
	public static final String SQL_INSERT_STATUS = "sql.insert.status";
	public static final String SQL_UPDATE_STATUS = "sql.update.status";
	public static final String SQL_DELETE_STATUS = "sql.delete.status";
	public static final String SQL_SHD_JOB_STEP = "sql.shd.job.step";
	
	//job ≈‰÷√–≈œ¢
	public static final String JOB_ATTR_POOL = "job.attr.pool";
	public static final String JOB_ATTR_SYSTEM = "job.attr.system";
	public static final String JOB_ATTR_JOB = "job.attr.job";
	public static final String JOB_ATTR_NODE = "job.attr.node";
	public static final String JOB_ATTR_JOBTYPE = "job.attr.jobtype";
	public static final String JOB_ATTR_CHECKTIMEWINDOW = "job.attr.checktimewindow";
	public static final String JOB_ATTR_CHECKTIMETRIGGER = "job.attr.checktimetrigger";
	public static final String JOB_ATTR_CHECKCALENDAR = "job.attr.checkcalendar";
	public static final String JOB_ATTR_CHECKLASTSTATUS = "job.attr.checklaststatus";
	public static final String JOB_ATTR_PRIORITY = "job.attr.priority";
	public static final String JOB_ATTR_ENABLE = "job.attr.enable";
	public static final String JOB_ATTR_TRIGGER_TYPE = "job.attr.trigger.type";
	public static final String JOB_ATTR_EXPRESSION = "job.attr.expression";
	public static final String JOB_ATTR_BATCHNUM = "job.attr.batchnum";
	public static final String JOB_ATTR_CMD = "job.attr.cmd";
	
	//key
	public static final String MAP_KEY_POOL = "pool";
	public static final String MAP_KEY_SYS = "sys";
	public static final String MAP_KEY_JOB = "job";
	public static final String MAP_KEY_BATCHNUM = "batchnum";
	public static final String MAP_KEY_TXDATE = "txdate";
	public static final String MAP_KEY_STREAM = "stream";
	public static final String MAP_KEY_DEPENDENCY = "dependency";
	public static final String MAP_KEY_STEP = "step";
	public static final String MAP_KEY_TIMEWINDOW = "timewindow";
	public static final String MAP_KEY_CTL = "ctl";
	public static final String MAP_KEY_TRIGGER = "trigger";
	public static final String MAP_KEY_NODE = "node";
	public static final String MAP_KEY_MAPPING = "mapping";
	public static final String MAP_KEY_STATUS = "status";
	public static final String MAP_KEY_NODESTATUS = "nodestatus";
	public static final String MAP_KEY_RUNNING = "running";
	public static final String MAP_KEY_TYPE = "type";
	public static final String MAP_KEY_EXPRESSION = "expression";
	public static final String MAP_KEY_FLAG = "flag";
	
	//
	public static final String VAL_CONSTANT_N = "N";
	public static final String VAL_CONSTANT_Y = "Y";
	
	//
	public static final String BATCH_TIME_HEARTBEADT = "batch.time.heartbeat";
	public static final String BATCH_TIME_REFRESH = "batch.time.refresh";	
	public static final String BATCH_PATH_HOME = "batch.path.home";
	public static final String BATCH_PATH_LOG = "batch.path.log";
	public static final String BATCH_PATH_SCRIPT = "batch.path.script";
	public static final String BATCH_PATH_FILE = "batch.path.file";
	public static final String BATCH_PATH_TRIGGER_LOG = "batch.path.trigger.log";
	public static final String BATCH_FLAG_DEBUG = "batch.flag.debug";
	public static final String BATCH_FLAG_RUNNING = "batch.flag.running";
	public static final String BATCH_DB_IP = "batch.db.ip";
	public static final String BATCH_DB_PORT = "batch.db.port";
	public static final String BATCH_DB_USR = "batch.db.usr";
	public static final String BATCH_DB_PASSWD = "batch.db.passwd";
	public static final String BATCH_DB_DEFAULTDB = "batch.db.defaultdb";
	public static final String BATCH_DB_TYPE = "batch.db.type";	
	public static final String BATCH_FILE_PATH = "batch.file.path";
	public static final String BATCH_TYPE_CONFIG = "batch.type.config";
	public static final String BATCH_SCRIPT_ENDWITH = "batch.script.endwith";
	public static final String BATCH_SCRIPT_ENDWITH_CMD = "batch.script.endwith.cmd";
	public static final String BATCH_NAME_TRIGGER_LOG = "batch.name.trigger.log";
	public static final String BATCH_MAX_PRIORITH = "batch.max.priority";
	public static final String BATCH_MIN_PRIORITH = "batch.min.priority";
	public static final String BATCH_LOG_KEEPPERIOD = "batch.log.keepperiod";
	
	public static final String BATCH_ZKC_IP = "batch.zkc.ip";
	public static final String BATCH_ZKC_PORT = "batch.zkc.port";
	public static final String BATCH_ZKC_HOME = "batch.zkc.home";
	
	public static final String SLV_STAT_CPU = "slv.stat.cpu";
	public static final String SLV_STAT_MEM = "slv.stat.mem";
	public static final String SLV_STAT_PROC = "slv.stat.proc";
	
	
	public static final String JSON_KEY_POOL = "json.key.pool";
	public static final String JSON_KEY_SYS = "json.key.sys";
	public static final String JSON_KEY_JOB = "json.key.job";
	public static final String JSON_KEY_NODE = "json.key.node";
	public static final String JSON_KEY_STATUS = "json.key.status";
	public static final String JSON_KEY_ENABLE = "json.key.enable";
	public static final String JSON_KEY_NUM = "json.key.num";
	public static final String JSON_KEY_STARTTIME = "json.key.starttime";
	public static final String JSON_KEY_ENDTIME = "json.key.endtime";
	public static final String JSON_KEY_SEQ = "json.key.seq";
	public static final String JSON_KEY_CMD = "json.key.cmd";
	public static final String JSON_KEY_TXDATE = "json.key.txdate";
	public static final String JSON_KEY_EXPRESSION = "json.key.expression";
	public static final String JSON_KEY_DESCRIPTION = "json.key.description";
	public static final String JSON_KEY_JOBTYPE = "json.key.jobtype";
	public static final String JSON_KEY_CHECKTIMEWINDOW = "json.key.checktimewindow";
	public static final String JSON_KEY_CHECKTIMETRIGGER = "json.key.checktimetrigger";
	public static final String JSON_KEY_CHECKCALENDAR = "json.key.checkcalendar";
	public static final String JSON_KEY_CHECKLASTSTATUS = "json.key.checklaststatus";
	public static final String JSON_KEY_PRIORITY = "json.key.priority";
	public static final String JSON_KEY_TRIGGERTYPE = "json.key.triggertype";
	public static final String JSON_KEY_OFFSET = "json.key.offset";
	public static final String JSON_KEY_CPUPCT = "json.key.cpupct";
	public static final String JSON_KEY_MEMPCT = "json.key.mempct";
	public static final String JSON_KEY_PROCESSCNT = "json.key.processcnt";
	public static final String JSON_KEY_SLVTIME = "json.key.slvtime";
	public static final String JSON_KEY_MODULE = "json.key.module";
	
	public static final String DEFAULT_VAL_NAN = "default.val.nan";
	
}
