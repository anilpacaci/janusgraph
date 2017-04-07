package org.janusgraph.graphdb.database.idassigner.placement;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.internal.InternalElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@PreInitializeConfigOptions
public class LDGGreedyPlacementStrategy extends AbstractEdgeCutPlacementStrategy implements IDPlacementStrategy {

	private static final Logger log = LoggerFactory.getLogger(LDGGreedyPlacementStrategy.class);

	public LDGGreedyPlacementStrategy(Configuration config) {
		super(config);
	}

	@Override
	public int getPartition(InternalElement element, StarVertex vertex) {
		// assign first 0.1% of vertices randomly
		if (this.counter < this.totalCapacity / 1000) {
			return getRandomPartition();
		}

		if (vertex == null || !this.partitioningEnabled || !considerForPartitioning(vertex)) {
			// there is no adjacency information or partitioning is explicity
			// disabled, resort to random partitioning
			return getRandomPartition();
		}

		double[] partitionScores = new double[maxPartitions];
		int[] neighbourCount = getNeighbourCount(vertex);

		for (int i = 0; i < maxPartitions; i++) {
			// actual LDG formula
			partitionScores[i] = neighbourCount[i] * (1 - ((double) partitionSizes[i]) / partitionCapacity);
		}

		List<Integer> candidatePartitions = Lists.newArrayList();

		double tempMax = -1;
		for (int i : availablePartitions) {
			if (partitionScores[i] > tempMax) {
				tempMax = partitionScores[i];
				candidatePartitions.clear();
				candidatePartitions.add(i);
			} else if (partitionScores[i] == tempMax) {
				candidatePartitions.add(i);
			}
		}
		int assignedPartition = candidatePartitions.get(random.nextInt(candidatePartitions.size()));

		return assignedPartition;
	}

}
