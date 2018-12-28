package com.batch.util;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class T {

	public void zkccon(){
		//zk集群的地址  
        String ZKServers = "192.168.1.189:2191";  
        
        ClearLog cl = new ClearLog();
        cl.delSubDirectory("D:/temp");
        
        /** 
         * 创建会话 
         * new SerializableSerializer() 创建序列化器接口，用来序列化和反序列化 
         */  
//        ZkClient zkClient = new ZkClient(ZKServers,10000,10000,new SerializableSerializer());  
          
//        User user = new User();  
//        user.setId(1);  
//        user.setName("testUser");
//          zkClient.delete("/BATCH");
//          zkClient.create("/BATCH", null, CreateMode.PERSISTENT);  
//          zkClient.create("/BATCH/ins", null, CreateMode.PERSISTENT);
//          zkClient.create("/BATCH/ins/mst", null, CreateMode.PERSISTENT);
//          zkClient.create("/BATCH/ins/mst/queue", null, CreateMode.PERSISTENT);
//          zkClient.create("/BATCH/ins/mst/notice", null, CreateMode.PERSISTENT);
//          zkClient.create("/BATCH/ins/mst/lock", null, CreateMode.PERSISTENT);
//          zkClient.create("/BATCH/ins/mst/stream/10000.APP_ODS_ODS_CHN_TEST", null, CreateMode.PERSISTENT);
//        zkClient.create("/BATCH/ins/mst/parameter", null, CreateMode.PERSISTENT);
//        zkClient.create("/BATCH/ins/slv/slv-1/status", null, CreateMode.PERSISTENT);
//        NodeInfo slvni =zkClient.readData("/BATCH/ins/slv/slv-1/status/10000.APP_ODS_ODS_CHN_TEST");
//        NodeInfo mstni =zkClient.readData("/BATCH/ins/mst/lock/10000.APP_ODS_ODS_CHN_TEST");
//        System.out.println(mstni.getSequence()+"=>"+slvni.getSequence()+"=>"+mstni.getNodeName()+"=>"+mstni.getStarttime());
//        zkClient.delete("/BATCH/ins/slv/slv-1/status/10000.APP_ODS_ODS_CHN_TEST");
//        zkClient.create("/BATCH/ins/slv/slv-1/10000.APP_ODS_ODS_CHN_TEST", null, CreateMode.PERSISTENT);
//        zkClient.create("/BATCH/ins/mst/stream/10000.APP_ODS_ODS_CHN_TEST", null, CreateMode.PERSISTENT);
//        zkClient.delete("/BATCH/ins/slv/slv-1/status/10000.APP_ODS_ODS_CHN_TEST");
//        zkClient.delete("/BATCH/ins/slv/slv-1/10000.APP_ODS_ODS_CHN_TEST");
//        mstni.setStatus("Fail");
//        zkClient.writeData("/BATCH/ins/mst/lock/10000.APP_ODS_ODS_CHN_TEST", mstni);
//        zkClient.create("/BATCH/ins/slv/slv-1/status/10000.APP_ODS_ODS_CHN_TEST", null, CreateMode.PERSISTENT);
//        zkClient.writeData("/BATCH/ins/slv/slv-1/status/10000.APP_ODS_ODS_CHN_TEST", mstni);
//        zkClient.delete("/BATCH/ins/slv/slv-1/status/10000.APP_ODS_ODS_CHN_TEST");
//        zkClient.create("/BATCH/ins/slv/slv-1/status/10000.APP_ODS_ODS_CHN_TEST", mstni, CreateMode.PERSISTENT);
//        zkClient.delete("/BATCH/ins/slv/slv-1/status1");
//        zkClient.create("/BATCH/ins/slv", null, CreateMode.PERSISTENT);
//        zkClient.create("/BATCH/ins/slv/slv-1", null, CreateMode.PERSISTENT);
//        zkClient.create("/BATCH/ins/slv/slv-1/status", null, CreateMode.PERSISTENT);
//        zkClient.create("/BATCH/ins/slv/list", null, CreateMode.PERSISTENT);
//        Stat stat = new Stat();  
//        //获取 节点中的对象  
//        User  user = zkClient.readData("/BATCH",stat);
//        
//        System.out.println(user.getName());  
//        System.out.println(stat);  
//        System.out.println("conneted ok!");  
	}
}
