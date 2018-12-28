package com.batch.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkClient implements Watcher {
	//public static Logger log = Logger.getLogger("ZookeeperClient.Class");
	public ZooKeeper zk =null;
    private static int SESSION_TIME_OUT = 2000;
    private CountDownLatch countdownlatch = new CountDownLatch(1);
    
    public ZkClient(String host){
    	try {
			zk = new ZooKeeper(host,SESSION_TIME_OUT,this);
			countdownlatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
	/**
	 * 实现watcher 的接口方法,当连接成功后,zookeeper通过此方法会通知watcher
	 * 此处为如果接受连接成功的EVENT,则countDown ,让当前线程继续其他事情
	 */
	@Override
	public void process(WatchedEvent event) {
		if(event.getState()==KeeperState.SyncConnected){
			//log.info("watcher receive event!");
			countdownlatch.countDown();
		}
	}

	public String create(String path,Object data,final CreateMode mode){
		String ret = null;
		try {
			ret = this.zk.create(path, serialize(data.toString()), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	public List getChildren(String path){
		List lst = null;
		try {
			lst = this.zk.getChildren(path, false);
		} catch (KeeperException e) {
			lst = null;
			e.printStackTrace();
		} catch (InterruptedException e) {
			lst = null;
			e.printStackTrace();
		}
		return lst;
	}
	
	/**
	 * 根据路径获取节点数据
	 * @param path
	 * @return
	 */
	public Object readData(String path){
		byte[] ret = null;
		Object obj = null;
		try {
			ret = this.zk.getData(path, false, null);
			if(ret!=null||ret.length==0){
				obj = deserialize(ret);
			}
		} catch (Exception e) {
			return null;
		}
		return obj;
	}
	public boolean isNullNode(String path){
		byte[] ret = null;
		Object obj = null;
		try {
			ret = this.zk.getData(path, false, null);
			System.out.println(ret.toString());
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}
	public Stat writeData(String path ,Object obj){
		Stat stat = null;
		try {
			//创建目录节点
			stat = this.zk.setData(path,serialize(obj.toString()), -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return stat;
	}
	
	/**
	 * 删除节点
	 * @param path
	 * @param version
	 */
	public void delete(String path){
		try {
			this.zk.delete(path, -1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
	
	public boolean exists(String path){
		Stat stat = null;
		boolean ret = false;
		try {
			stat = this.zk.exists(path, false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(stat==null){
			ret = false;
		}else{
			ret = true;
		}
		return ret;
	}
	
	/**
	 * 关闭zookeeper连接
	 */
	public void closeConnect(){
		if(zk!=null){
			try {
				zk.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public byte[] serialize(Object serializable)  {
		ByteArrayOutputStream byteArrayOS = null;
        try {
            byteArrayOS = new ByteArrayOutputStream();
            ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
            stream.writeObject(serializable);
            stream.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
        return byteArrayOS.toByteArray();
    }
	
	public Object deserialize(byte[] bytes) {
		ObjectInputStream inputStream = null;
		Object object = null;
        try {
            inputStream = new TcclAwareObjectIputStream(new ByteArrayInputStream(bytes));
            object = inputStream.readObject();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        return object;
    }
	
	public boolean isALive(){
		boolean ret = zk.getState().isAlive();
		return ret;
	}
}
