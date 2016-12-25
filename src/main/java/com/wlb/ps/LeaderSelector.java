package com.wlb.ps;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class LeaderSelector implements Closeable{
	 private volatile boolean hasLeadership;
	 private ZooKeeper zooKeeper;
	 private String leaderPath;
	 private LeaderSelectorListener leaderSelectorListener;
	 private static final String LOCK_NAME = "lock-";
	 private AtomicBoolean state = new AtomicBoolean(false);
	 public LeaderSelector(ZooKeeper zooKeeper , String leaderPath , LeaderSelectorListener leaderSelectorListener){
		 this.zooKeeper = zooKeeper;
		 this.leaderPath = leaderPath;
		 this.leaderSelectorListener = leaderSelectorListener;
	 }
	 
	public void takeLeader() throws Exception{
		if (!state.compareAndSet(false, true)) {
			throw new Exception("Cannot be started more than once");
		}
		attemptLock(false);
	}
	
	public void waitTakeLeader() throws Exception{
		if (!state.compareAndSet(false, true)) {
			throw new RuntimeException("Cannot be started more than once");
		}
		attemptLock(true);
	}
	
	private synchronized void attemptLock(boolean isWaitTakeLeader)throws Exception{
		boolean isDone = false;
		while(!isDone){
			isDone = true;
			try {
				String nodePath = createLockNode(zooKeeper);
				hasLeadership = internalLockLoop(nodePath , isWaitTakeLeader);
				if (hasLeadership) {
					leaderSelectorListener.takeLeaderShip(zooKeeper);
				}
			} catch (Exception e) {
				throw e;
			}
		}
	}
	
    public boolean hasLeadership()
    {
        return hasLeadership;
    }
    
    private boolean internalLockLoop(String nodePath , boolean isWaitTakeLeader)throws Exception{
    	boolean hasTakeLeader = false;
		boolean doDeleted = false;
		String meLockNodePath = nodePath.substring(LOCK_NAME.length() + 1);
		
		try {
			while(!hasTakeLeader){
				List<String> childrens = getSortedChildrens();
				int meIndex = childrens.indexOf(meLockNodePath);
				if (meIndex < 0) {
					throw new NoNodeException(String.format("lock node path = %s is not exist!", meLockNodePath));
				}
				
				boolean isGetTheLock = meIndex == 0;
				if (isGetTheLock) {
					hasTakeLeader = true;
				}else if (isWaitTakeLeader){
					String pathToWatch = new StringBuilder(leaderPath).append("/").append(childrens.get(meIndex - 1)).toString();
					final CountDownLatch countDownLatch = new CountDownLatch(1);
					Watcher watcher = new Watcher() {
						public void process(WatchedEvent event) {
							if (EventType.NodeDeleted == event.getType()) {
								countDownLatch.countDown();
							}
						}
					};
					
					try {
						zooKeeper.exists(pathToWatch, watcher);
						countDownLatch.await();
					} catch (NoNodeException e) {
						// TODO: ignore
					}finally{
						zooKeeper.exists(pathToWatch, null);
					}
				}
			}
		} catch (Exception e) {
			doDeleted = true;
			throw e;
		}finally{
			if (doDeleted) {
				zooKeeper.delete(nodePath, -1);
			}
		}
		
		return hasTakeLeader;
    }
    
    /**
     * 创建节点
     * @param zooKeeper
     * @return
     * @throws Exception
     */
	private String createLockNode(ZooKeeper zooKeeper) throws Exception{
		KeeperException  exception = null;
		String nodePath = new StringBuilder(leaderPath).append("/").append(LOCK_NAME).toString();
		try {
			return zooKeeper.create(nodePath , null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException.ConnectionLossException e) {
			exception = e;
		}catch (KeeperException.SessionExpiredException e) {
			exception = e;
		}catch (KeeperException.NoNodeException e) {
			String parentPath = nodePath.substring(0, nodePath.lastIndexOf("/"));
			zooKeeper.create(parentPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return createLockNode(zooKeeper);
		}catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}catch (KeeperException e) {
			exception = e;
		}
		
		throw exception;
	}
	
	public void close() throws IOException {
		if (!state.compareAndSet(true, false)) {
			throw new RuntimeException("Already closed or has not been started");
		}
		try {
			zooKeeper.close();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	private List<String> getSortedChildrens() throws Exception{
		try {
			List<String> childrens = zooKeeper.getChildren(leaderPath, null);
			Collections.sort(childrens, new Comparator<String>(){
				public int compare(String childPath1, String childPath2) {
					return getLockNodeNumber(childPath1, LOCK_NAME).compareTo(getLockNodeNumber(childPath2, LOCK_NAME));
				}
				
			});
			return childrens;
		}catch (KeeperException e) {
			throw e;
		}
	}
	
	
	private String getLockNodeNumber(String childPath , String lockName){
		int index = childPath.indexOf(lockName);
		if (index >= 0) {
			index += lockName.length();
			return index < lockName.length() ? childPath.substring(index) : "";
		}
		return childPath;
	}
	
}
