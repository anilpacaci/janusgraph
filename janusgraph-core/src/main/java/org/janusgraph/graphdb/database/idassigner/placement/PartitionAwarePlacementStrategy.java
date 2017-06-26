package org.janusgraph.graphdb.database.idassigner.placement;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

@PreInitializeConfigOptions
public class PartitionAwarePlacementStrategy implements IDPlacementStrategy {

	private static final Logger log = LoggerFactory.getLogger(PartitionAwarePlacementStrategy.class);

	private static String MEMCACHED_INSTANCE_NAME = "partition-lookup";

	/**
	 * This option was originally in {@link GraphDatabaseConfiguration} but then
	 * disabled. Now it is just used by GreedyPartitioner to decide between
	 * random - explicit partitioning. For explicit partitioning to kick in, one
	 * needs to set this flag <code>true</code>
	 */

	protected final Random random = new Random();

	protected int maxPartitions;
	protected int totalCapacity;

	protected boolean partitioningEnabled;

	protected PlacementHistory<String> placementHistory;

	public PartitionAwarePlacementStrategy(Configuration config) {
		this.maxPartitions = config.get(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS);
		this.totalCapacity = config.get(GraphDatabaseConfiguration.TOTAL_CAPACITY);
		this.partitioningEnabled = config.get(GraphDatabaseConfiguration.CLUSTER_PARTITION);

		log.warn("Partitioning enabled: {}", partitioningEnabled);

		Preconditions.checkArgument(totalCapacity > 0 && maxPartitions > 0);

		if (config.get(GraphDatabaseConfiguration.IDS_PLACEMENT_HISTORY)
				.equals(PlacementHistory.MEMCACHED_PLACEMENT_HISTORY)) {
			String hostname = config.get(GraphDatabaseConfiguration.IDS_PLACEMENT_HISTORY_HOSTNAME);
			this.placementHistory = new MemcachedPlacementHistory<String>(MEMCACHED_INSTANCE_NAME, hostname);
			log.warn("Memcached location: {}", hostname);
		} else {
			this.placementHistory = new InMemoryPlacementHistory<String>(totalCapacity);
		}
	}

	@Override
	public int getPartition(InternalElement element) {
		// XXX partition assignment without a context. Random
		return random.nextInt(maxPartitions);
	}

	@Override
	public int getPartition(InternalElement element, StarVertex vertex) {
		String id;
		if(vertex != null) {
			id = vertex.value("iid");
		} else {
			id = element.value("iid");
		}
		Integer partition = placementHistory.getPartition(id);
		return partition;
	}

	@Override
	public void assignedPartition(InternalElement element, int partitionID) {
		// TODO Auto-generated method stub

	}

	@Override
	public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
		// TODO Auto-generated method stub

	}

	@Override
	public void injectIDManager(IDManager idManager) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean supportsBulkPlacement() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges) {
		// TODO Auto-generated method stub

	}

	@Override
	public void exhaustedPartition(int partitionID) {
		// TODO Auto-generated method stub

	}

	@Override
	public void assignedPartition(InternalElement element, StarVertex vertex, int partitionID) {
		// @apacaci : we do not need partition assignment hints
	}

}
