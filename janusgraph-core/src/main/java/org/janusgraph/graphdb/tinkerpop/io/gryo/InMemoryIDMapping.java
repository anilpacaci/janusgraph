package org.janusgraph.graphdb.tinkerpop.io.gryo;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Java HashMap based implementation
 * 
 * @author apacaci
 *
 */
public class InMemoryIDMapping implements IDMapping {

	private static final Integer DEFAULT_CAPACITY = 10000;

	Map<Long, Long> idMapping;

	public InMemoryIDMapping() {
		this(DEFAULT_CAPACITY);
	}

	public InMemoryIDMapping(Integer totalCapacity) {
		Preconditions.checkArgument(totalCapacity > 0);
		this.idMapping = Maps.newHashMapWithExpectedSize(totalCapacity);
	}

	@Override
	public Long getCurrentID(Long originialID) {
		return idMapping.getOrDefault(originialID, Long.valueOf(-1));
	}

	@Override
	public void setCurrentID(Long originialID, Long currentID) {
		Preconditions.checkArgument(originialID != null && currentID != null);
		idMapping.put(originialID, currentID);
	}

}
