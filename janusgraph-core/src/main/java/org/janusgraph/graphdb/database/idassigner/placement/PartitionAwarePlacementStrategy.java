package org.janusgraph.graphdb.database.idassigner.placement;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.janusgraph.diskstorage.configuration.ConfigOption;
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

	/**
	 * This option was originally in {@link GraphDatabaseConfiguration} but then
	 * disabled. Now it is just used by GreedyPartitioner to decide between
	 * random - explicit partitioning. For explicit partitioning to kick in, one
	 * needs to set this flag <code>true</code>
	 */
	public static final ConfigOption<Boolean> CLUSTER_PARTITION = new ConfigOption<Boolean>(
			GraphDatabaseConfiguration.CLUSTER_NS, "partition",
			"Whether the graph's element should be randomly distributed across the cluster "
					+ "(true) or explicitly allocated to individual partition blocks based on the configured graph partitioner (false). "
					+ "Unless explicitly set, this defaults false for stores that hash keys and defaults true for stores that preserve key order "
					+ "(such as HBase and Cassandra with ByteOrderedPartitioner).",
			ConfigOption.Type.MASKABLE, false);

	public static final ConfigOption<String> IDS_PLACEMENT_HISTORY = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "placement-history",
			"Placement history Implementation for Greedy Partitioners", ConfigOption.Type.MASKABLE, "inmemory");

	public static final ConfigOption<Integer> TOTAL_CAPACITY = new ConfigOption<Integer>(
			GraphDatabaseConfiguration.CLUSTER_NS, "total-capacity",
			"Total size (number of vertices) for all partitions, only applicable for explicit graph partitioners",
			ConfigOption.Type.MASKABLE, 10);

	public static final ConfigOption<String> IDS_PLACEMENT_HISTORY_HOSTNAME = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "placement-history-hostname",
			"Memcached Server address for Placement History Implementation", ConfigOption.Type.MASKABLE,
			"localhost:11211");

	protected final Random random = new Random();

	protected int maxPartitions;
	protected int totalCapacity;

	protected boolean partitioningEnabled;

	protected PlacementHistory<String> placementHistory;

	public PartitionAwarePlacementStrategy(Configuration config) {
		this.maxPartitions = config.get(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS);
		this.totalCapacity = config.get(TOTAL_CAPACITY);
		this.partitioningEnabled = config.get(CLUSTER_PARTITION);

		log.warn("Partitioning enabled: {}", partitioningEnabled);

		Preconditions.checkArgument(totalCapacity > 0 && maxPartitions > 0);

		if (config.get(IDS_PLACEMENT_HISTORY).equals(PlacementHistory.MEMCACHED_PLACEMENT_HISTORY)) {
			String hostname = config.get(IDS_PLACEMENT_HISTORY_HOSTNAME);
			this.placementHistory = new MemcachedPlacementHistory<String>(hostname);
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
		String id = vertex.value(BulkLoaderVertexProgram.DEFAULT_BULK_LOADER_VERTEX_ID);
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
