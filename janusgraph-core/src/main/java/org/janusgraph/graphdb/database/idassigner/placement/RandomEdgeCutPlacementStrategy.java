package org.janusgraph.graphdb.database.idassigner.placement;

import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.internal.InternalElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PreInitializeConfigOptions
public class RandomEdgeCutPlacementStrategy extends AbstractEdgeCutPlacementStrategy implements IDPlacementStrategy {

	private static final Logger log = LoggerFactory.getLogger(RandomEdgeCutPlacementStrategy.class);

	public RandomEdgeCutPlacementStrategy(Configuration config) {
		super(config);
	}

	@Override
	public int getPartition(InternalElement element, StarVertex vertex) {
		return getRandomPartition();
	}

}
