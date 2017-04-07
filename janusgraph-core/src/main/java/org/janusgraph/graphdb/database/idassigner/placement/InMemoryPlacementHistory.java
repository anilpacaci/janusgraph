package org.janusgraph.graphdb.database.idassigner.placement;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Java HashMap based implementation
 * @author apacaci
 *
 */
public class InMemoryPlacementHistory implements PlacementHistory {
	
	Map<Long, Integer> placementHistory;
	
	public InMemoryPlacementHistory(Integer totalCapacity) {
		Preconditions.checkArgument(totalCapacity > 0);
		this.placementHistory = Maps.newHashMapWithExpectedSize(totalCapacity);
	}

	@Override
	public Integer getPartition(Long id) {
		return placementHistory.getOrDefault(id, -1);
	}

	@Override
	public void setPartition(Long id, Integer partition) {
		Preconditions.checkArgument(partition >= 0);
		placementHistory.put(id, partition);
	}

}
