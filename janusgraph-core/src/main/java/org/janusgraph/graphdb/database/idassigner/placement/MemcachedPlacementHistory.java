package org.janusgraph.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class MemcachedPlacementHistory<T> implements PlacementHistory<T> {

	private static String INSTANCE_NAME = "placement";

	private MemCachedClient client;

	public MemcachedPlacementHistory(String... servers) {
		SockIOPool pool = SockIOPool.getInstance(INSTANCE_NAME);
		pool.setServers(servers);
		pool.setFailover(true);
		pool.setInitConn(10);
		pool.setMinConn(5);
		pool.setMaxConn(250);
		pool.setMaintSleep(30);
		pool.setNagle(false);
		pool.setSocketTO(3000);
		pool.setAliveCheck(true);
		pool.initialize();

		client = new MemCachedClient(INSTANCE_NAME);
		client.flushAll();
	}

	@Override
	public Integer getPartition(T id) {
		Object value = client.get(id.toString());
		if (value == null)
			return null;
		return (Integer) value;
	}

	@Override
	public void setPartition(T id, Integer partition) {
		Preconditions.checkArgument(partition >= 0);
		client.set(id.toString(), partition);
	}

}
