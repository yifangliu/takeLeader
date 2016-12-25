package com.wlb.ps;

import org.apache.zookeeper.ZooKeeper;

public interface LeaderSelectorListener {

	public void takeLeaderShip(ZooKeeper zooKeeper) throws Exception;
}
