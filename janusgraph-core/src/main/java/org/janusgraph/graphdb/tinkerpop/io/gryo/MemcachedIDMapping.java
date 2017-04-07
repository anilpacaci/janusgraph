package org.janusgraph.graphdb.tinkerpop.io.gryo;

import com.google.common.base.Preconditions;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class MemcachedIDMapping implements IDMapping {

	private static String INSTANCE_NAME = "mapping";

	private static String ID_APPENDIX = ":id";

	private MemCachedClient client;

	public MemcachedIDMapping(String... servers) {
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
	public Long getCurrentID(Long originialID) {
		Object value = client.get(originialID.toString() + ID_APPENDIX);
		if (value == null)
			return null;
		return (Long) value;
	}

	@Override
	public void setCurrentID(Long originialID, Long currentID) {
		Preconditions.checkArgument(currentID != null);
		client.set(originialID.toString() + ID_APPENDIX, currentID);
	}

}
