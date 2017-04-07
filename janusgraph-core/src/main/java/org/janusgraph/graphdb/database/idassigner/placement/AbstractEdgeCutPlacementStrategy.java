package org.janusgraph.graphdb.database.idassigner.placement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.tinkerpop.gremlin.structure.Direction;
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
import com.google.common.collect.Lists;


@PreInitializeConfigOptions
public abstract class AbstractEdgeCutPlacementStrategy implements IDPlacementStrategy {

	private static final Logger log = LoggerFactory.getLogger(AbstractEdgeCutPlacementStrategy.class);

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

	public static final ConfigOption<Integer> TOTAL_CAPACITY = new ConfigOption<Integer>(
			GraphDatabaseConfiguration.CLUSTER_NS, "total-capacity",
			"Total size (number of vertices) for all partitions, only applicable for explicit graph partitioners",
			ConfigOption.Type.MASKABLE, 10);

	public static final ConfigOption<Double> PARTITION_BALANCE_SLACK = new ConfigOption<Double>(
			GraphDatabaseConfiguration.CLUSTER_NS, "partition-balance-slackness",
			"Slackness paramater for partition balance", ConfigOption.Type.MASKABLE, (double) 0);

	public static final ConfigOption<String> IDS_PLACEMENT_HISTORY = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "placement-history",
			"Placement history Implementation for Greedy Partitioners", ConfigOption.Type.MASKABLE, "inmemory");

	public static final ConfigOption<String> IDS_PLACEMENT_HISTORY_HOSTNAME = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "placement-history-hostname",
			"Memcached Server address for Placement History Implementation", ConfigOption.Type.MASKABLE,
			"localhost:11211");

	public static final ConfigOption<String[]> PARTITIONING_VERTEX_LABELS = new ConfigOption<String[]>(
			GraphDatabaseConfiguration.PARTITIONING_NS, "vertex-labels",
			"List of vertex labels to be considered for partitioning", ConfigOption.Type.MASKABLE, new String[0]);

	public static final ConfigOption<String[]> PARTITIONING_EDGE_LABELS = new ConfigOption<String[]>(
			GraphDatabaseConfiguration.PARTITIONING_NS, "edge-labels",
			"List of edge labels to be considered for partitioning", ConfigOption.Type.MASKABLE, new String[0]);

	protected final Random random = new Random();

	protected int maxPartitions;
	protected int totalCapacity;
	protected int partitionCapacity;

	protected double balanceSlack;

	protected int counter = 0;

	protected boolean partitioningEnabled;

	protected List<Integer> availablePartitions;
	public static int[] partitionSizes;
	// 2D array keeping track of number of edges between partitions
	public static int[][] edgeCut;

	protected PlacementHistory placementHistory;

	private String[] vertexLabels;
	private String[] edgeLabels;

	public AbstractEdgeCutPlacementStrategy(Configuration config) {
		this.maxPartitions = config.get(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS);
		this.totalCapacity = config.get(TOTAL_CAPACITY);
		this.balanceSlack = config.get(PARTITION_BALANCE_SLACK);
		this.partitioningEnabled = config.get(CLUSTER_PARTITION);

		log.warn("Partitioning enabled: {}", partitioningEnabled);

		Preconditions.checkArgument(totalCapacity > 0 && maxPartitions > 0);

		this.partitionCapacity = (int) ((totalCapacity / maxPartitions) * (1 + balanceSlack));

		if (config.get(IDS_PLACEMENT_HISTORY).equals(PlacementHistory.MEMCACHED_PLACEMENT_HISTORY)) {
			String hostname = config.get(IDS_PLACEMENT_HISTORY_HOSTNAME);
			this.placementHistory = new MemcachedPlacementHistory(hostname);
			log.warn("Memcached location: {}", hostname);
		} else {
			this.placementHistory = new InMemoryPlacementHistory(totalCapacity);
		}

		availablePartitions = new ArrayList<>(maxPartitions);
		partitionSizes = new int[maxPartitions];
		edgeCut = new int[maxPartitions][maxPartitions];

		// initially all partitions are available
		for (int i = 0; i < maxPartitions; i++) {
			availablePartitions.add(i);
		}

		// edge labels to be considered for partitioning
		vertexLabels = config.get(PARTITIONING_VERTEX_LABELS);
		edgeLabels = config.get(PARTITIONING_EDGE_LABELS);
	}

	@Override
	public int getPartition(InternalElement element) {
		// XXX partition assignment without a context. Random
		return getRandomPartition();
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
		Preconditions.checkArgument(partitionID < maxPartitions && vertex != null);
		placementHistory.setPartition((Long) vertex.id(), partitionID);
		partitionSizes[partitionID]++;
		// check whether partition achieved its capacity
		if (partitionSizes[partitionID] >= partitionCapacity) {
			availablePartitions.remove(Integer.valueOf(partitionID));
		}

		log.warn("Vertex {} assigned to partition {}", ++counter, partitionID);

		// record number of edges to different partitions, so that we can
		// compute edge-cut
		int[] neighbourCount = getNeighbourCount(vertex);
		for (int i = 0; i < neighbourCount.length; i++) {
			this.edgeCut[partitionID][i] += neighbourCount[i];
		}
	}

	/**
	 *
	 * @return one of the available partitions randomly drawn from a uniform
	 *         distribution
	 */
	public int getRandomPartition() {
		return availablePartitions.get(random.nextInt(availablePartitions.size()));
	}

	/**
	 * Checks whether given vertex should be considered for partitioning or not.
	 * If no vertex label specified partition, all vertices are considered for
	 * partitioning by default.
	 * 
	 * @param vertex
	 * @return
	 */
	protected boolean considerForPartitioning(StarVertex vertex) {
		if (this.vertexLabels.length == 0) {
			// means that all vertices are considered for partitioning
			return true;
		} else {
			String vertexLabel = vertex.label();
			return Arrays.stream(this.vertexLabels).anyMatch(s -> s.equals(vertexLabel));
		}
	}

	/**
	 * for a given StarVertex it checks all the nighbours and produces neighbour
	 * count per partition It only considers edges specified in edgeLabels
	 * 
	 * @param vertex
	 * @return
	 */
	protected int[] getNeighbourCount(StarVertex vertex) {
		List<Long> neighbourList = Lists.newArrayList();
		vertex.edges(Direction.BOTH, this.edgeLabels).forEachRemaining(edge -> {
			if (edge.inVertex().id().equals(vertex.id())) {
				neighbourList.add((Long) edge.outVertex().id());
			} else {
				neighbourList.add((Long) edge.inVertex().id());
			}
		});

		int[] neighbourCount = new int[maxPartitions];

		for (Long neighbour : neighbourList) {
			Integer partition = placementHistory.getPartition(neighbour);
			if (partition != null) {
				// means that adjacent vertex previously assigned
				neighbourCount[partition]++;
			}
		}

		return neighbourCount;
	}

}
