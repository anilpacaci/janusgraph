package org.janusgraph.graphdb.tinkerpop.io.gryo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.Host;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.VertexIDAssigner;
import org.janusgraph.graphdb.database.idassigner.placement.AbstractEdgeCutPlacementStrategy;
import org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Titan Graph specific Gryo Reader Implementation
 * 
 * @author apacaci
 *
 */
public final class JanusGraphGryoReader implements GraphReader {

	private static final Logger log = LoggerFactory.getLogger(JanusGraphGryoReader.class);

	public static final ConfigOption<String> IDS_LOADER_MAPPING = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "loader-mapping", "ID Mapping implementation for TitanGryoReader",
			ConfigOption.Type.MASKABLE, "inmemory");

	public static final ConfigOption<String> IDS_LOADER_MAPPING_HOSTNAME = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "loader-mapping-hostname",
			"Memcached Server address for ID Mapping Implementation", ConfigOption.Type.MASKABLE, "localhost:11211");

	private static final Map<String, String> REGISTERED_PLACEMENT_STRATEGIES = ImmutableMap.of("simple",
			SimpleBulkPlacementStrategy.class.getName());

	private static final String ORIGINAL_ID_PROPERTY = "originalID";
	private static final String ORIGINAL_ID_INDEX_NAME = "byOriginalID";

	private final Kryo kryo;

	private final long batchSize;

	private JanusGraphGryoReader(final long batchSize, final Mapper<Kryo> gryoMapper) {
		this.kryo = gryoMapper.createMapper();
		this.batchSize = batchSize;
	}

	/**
	 * Partitioning aware implementation for read graph method
	 * 
	 * @param file
	 *            Path to file
	 * @param graphToWriteTo
	 *            TitanGraph
	 * @throws IOException
	 */
	public void readGraph(final String file, final JanusGraph graphToWriteTo) throws IOException {
		// first initialize ID Mapping implementation
		IDMapping idMapping;
		Configuration configuration = ((StandardJanusGraph) graphToWriteTo).getConfiguration().getConfiguration();
		if (configuration.get(IDS_LOADER_MAPPING).equals(IDMapping.MEMCACHED_IDMAPPING)) {
			String hostname = configuration.get(IDS_LOADER_MAPPING_HOSTNAME);
			idMapping = new MemcachedIDMapping(hostname);
		} else {
			idMapping = new InMemoryIDMapping();
		}

		AbstractEdgeCutPlacementStrategy placementStrategy = Backend.getImplementationClass(configuration,
				configuration.get(VertexIDAssigner.PLACEMENT_STRATEGY), REGISTERED_PLACEMENT_STRATEGIES);

		log.warn("ID Index have been created");

		// first read vertices from stream
		FileInputStream inputStream = new FileInputStream(file);
		readVertices(inputStream, graphToWriteTo, idMapping);

		log.warn("Vertices are read into the graph");

		// now read the edges
		inputStream = new FileInputStream(file);
		readEdges(inputStream, graphToWriteTo, idMapping);

		log.warn("Edges are read into the graph");

		int[] partitionSizes = placementStrategy.partitionSizes;
		for (int i = 0; i < partitionSizes.length; i++) {
			log.warn("Partition {} vertices {}", i, partitionSizes[i]);
		}

		long edgeCut = 0;
		long edgePreserved = 0;

		int[][] edgecounts = placementStrategy.edgeCut;
		for (int i = 0; i < edgecounts.length; i++) {
			for (int j = 0; j < edgecounts[i].length; j++) {
				if (i == j) {
					edgePreserved += edgecounts[i][j];
				} else {
					edgeCut += edgecounts[i][j];
				}
			}
		}

		log.warn("# of Edge-cut: {}", edgeCut);
		log.warn("# of Edges: {}", edgeCut + edgePreserved);

	}

	/**
	 * Read vertices into a {@link TitanGraph} from output generated by any of
	 * the {@link GryoWriter} {@code writeVertex} or {@code writeVertices}
	 * methods or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing an entire graph of vertices and edges as
	 *            defined by the accompanying
	 *            {@link GraphWriter#writeGraph(OutputStream, Graph)}.
	 * @param graphToWriteTo
	 *            the Titan graph to write to when reading from the stream.
	 * @throws IOException
	 */
	public void readVertices(final InputStream inputStream, final JanusGraph graphToWriteTo, final IDMapping idMapping)
			throws IOException {
		final AtomicLong counter = new AtomicLong(0);

		final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();

		Input input = new Input(inputStream);

		IteratorUtils.iterate(new VertexInputIterator(input, attachable -> {
			final StarVertex starVertex = (StarGraph.StarVertex) attachable.get();
			final Vertex v = graphToWriteTo.addStarVertex(starVertex, T.label, starVertex.label());
			starVertex.properties().forEachRemaining(vp -> {
				final VertexProperty vertexProperty = graphToWriteTo.features().vertex().properties()
						.willAllowId(vp.id())
								? v.property(graphToWriteTo.features().vertex().getCardinality(vp.key()), vp.key(),
										vp.value(), T.id, vp.id())
								: v.property(graphToWriteTo.features().vertex().getCardinality(vp.key()), vp.key(),
										vp.value());
				vp.properties().forEachRemaining(p -> vertexProperty.property(p.key(), p.value()));
			});

			Long originalID = (Long) starVertex.id();
			Long currentID = (Long) v.id();

			idMapping.setCurrentID(originalID, currentID);

			if (supportsTx && counter.incrementAndGet() % batchSize == 0)
				graphToWriteTo.tx().commit();
			return v;
		}, null, null));

		if (supportsTx)
			graphToWriteTo.tx().commit();

		log.warn("Total number of vertices {}", counter.get());
	}

	/**
	 * Read edges into a {@link TitanGraph} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing an entire graph of vertices and edges as
	 *            defined by the accompanying
	 *            {@link GraphWriter#writeGraph(OutputStream, Graph)}.
	 * @param graphToWriteTo
	 *            the Titan graph to write to when reading from the stream.
	 * @throws IOException
	 */
	public void readEdges(final InputStream inputStream, final JanusGraph graphToWriteTo, final IDMapping idMapping)
			throws IOException {
		final AtomicLong counter = new AtomicLong(0);

		final Graph.Features.EdgeFeatures edgeFeatures = graphToWriteTo.features().edge();
		final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();

		Input input = new Input(inputStream);

		GraphTraversalSource traversal = graphToWriteTo.traversal();

		IteratorUtils.iterate(new VertexInputIterator(input, attachable -> {
			final StarVertex starVertex = (StarGraph.StarVertex) attachable.get();
			starVertex.edges(Direction.IN).forEachRemaining(e -> {
				// can't use a standard Attachable attach method here because we
				// have to use the cache for those
				// graphs that don't support userSuppliedIds on edges. note that
				// outVertex/inVertex methods return
				// StarAdjacentVertex whose equality should match StarVertex.

				Long outID = (Long) e.outVertex().id();
				Long inID = (Long) e.inVertex().id();

				Long currentOutID = idMapping.getCurrentID(outID);
				Long currentInID = idMapping.getCurrentID(inID);

				Vertex outV = traversal.V(currentOutID).next();
				Vertex inV = traversal.V(currentInID).next();

				final Edge newEdge = edgeFeatures.willAllowId(e.id()) ? outV.addEdge(e.label(), inV, T.id, e.id())
						: outV.addEdge(e.label(), inV);
				e.properties().forEachRemaining(p -> newEdge.property(p.key(), p.value()));
				if (supportsTx && counter.incrementAndGet() % batchSize == 0) {
					graphToWriteTo.tx().commit();
					log.warn("Total relations committed {}", counter.get());
				}
			});

			return starVertex;
		}, null, null));

		if (supportsTx)
			graphToWriteTo.tx().commit();
	}

	/**
	 * Read data into a {@link Graph} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing an entire graph of vertices and edges as
	 *            defined by the accompanying
	 *            {@link GraphWriter#writeGraph(OutputStream, Graph)}.
	 * @param graphToWriteTo
	 *            the graph to write to when reading from the stream.
	 * @throws IOException
	 */
	@Override
	public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
		// dual pass - create all vertices and store to cache the ids. then
		// create edges. as long as we don't
		// have vertex labels in the output we can't do this single pass
		final Map<StarGraph.StarVertex, Vertex> cache = new HashMap<>();
		final AtomicLong counter = new AtomicLong(0);

		final Graph.Features.EdgeFeatures edgeFeatures = graphToWriteTo.features().edge();
		final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();

		IteratorUtils.iterate(new VertexInputIterator(new Input(inputStream), attachable -> {
			final Vertex v = cache.put((StarGraph.StarVertex) attachable.get(),
					attachable.attach(Attachable.Method.create(graphToWriteTo)));
			if (supportsTx && counter.incrementAndGet() % batchSize == 0)
				graphToWriteTo.tx().commit();
			return v;
		}, null, null));
		cache.entrySet().forEach(kv -> kv.getKey().edges(Direction.IN).forEachRemaining(e -> {
			// can't use a standard Attachable attach method here because we
			// have to use the cache for those
			// graphs that don't support userSuppliedIds on edges. note that
			// outVertex/inVertex methods return
			// StarAdjacentVertex whose equality should match StarVertex.
			final Vertex cachedOutV = cache.get(e.outVertex());
			final Vertex cachedInV = cache.get(e.inVertex());
			final Edge newEdge = edgeFeatures.willAllowId(e.id())
					? cachedOutV.addEdge(e.label(), cachedInV, T.id, e.id()) : cachedOutV.addEdge(e.label(), cachedInV);
			e.properties().forEachRemaining(p -> newEdge.property(p.key(), p.value()));
			if (supportsTx && counter.incrementAndGet() % batchSize == 0)
				graphToWriteTo.tx().commit();
		}));

		if (supportsTx)
			graphToWriteTo.tx().commit();
	}

	/**
	 * Read {@link Vertex} objects from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Vertex} as defined by
	 *            the accompanying
	 *            {@link GraphWriter#writeVertices(OutputStream, Iterator, Direction)}
	 *            or {@link GraphWriter#writeVertices(OutputStream, Iterator)}
	 *            methods.
	 * @param vertexAttachMethod
	 *            a function that creates re-attaches a {@link Vertex} to a
	 *            {@link Host} object.
	 * @param edgeAttachMethod
	 *            a function that creates re-attaches a {@link Edge} to a
	 *            {@link Host} object.
	 * @param attachEdgesOfThisDirection
	 *            only edges of this direction are passed to the
	 *            {@code edgeMaker}.
	 */
	@Override
	public Iterator<Vertex> readVertices(final InputStream inputStream,
			final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
			final Function<Attachable<Edge>, Edge> edgeAttachMethod, final Direction attachEdgesOfThisDirection)
			throws IOException {
		return new VertexInputIterator(new Input(inputStream), vertexAttachMethod, attachEdgesOfThisDirection,
				edgeAttachMethod);
	}

	/**
	 * Read a {@link Vertex} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing at least a single vertex as defined by the
	 *            accompanying
	 *            {@link GraphWriter#writeVertex(OutputStream, Vertex)}.
	 * @param vertexAttachMethod
	 *            a function that creates re-attaches a {@link Vertex} to a
	 *            {@link Host} object.
	 */
	@Override
	public Vertex readVertex(final InputStream inputStream,
			final Function<Attachable<Vertex>, Vertex> vertexAttachMethod) throws IOException {
		return readVertex(inputStream, vertexAttachMethod, null, null);
	}

	/**
	 * Read a {@link Vertex} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Vertex} as defined by
	 *            the accompanying
	 *            {@link GraphWriter#writeVertices(OutputStream, Iterator, Direction)}
	 *            method.
	 * @param vertexAttachMethod
	 *            a function that creates re-attaches a {@link Vertex} to a
	 *            {@link Host} object.
	 * @param edgeAttachMethod
	 *            a function that creates re-attaches a {@link Edge} to a
	 *            {@link Host} object.
	 * @param attachEdgesOfThisDirection
	 *            only edges of this direction are passed to the
	 *            {@code edgeMaker}.
	 */
	@Override
	public Vertex readVertex(final InputStream inputStream,
			final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
			final Function<Attachable<Edge>, Edge> edgeAttachMethod, final Direction attachEdgesOfThisDirection)
			throws IOException {
		final Input input = new Input(inputStream);
		return readVertexInternal(vertexAttachMethod, edgeAttachMethod, attachEdgesOfThisDirection, input);
	}

	/**
	 * Read an {@link Edge} from output generated by
	 * {@link GryoWriter#writeEdge(OutputStream, Edge)} or via an {@link Edge}
	 * passed to {@link GryoWriter#writeObject(OutputStream, Object)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Edge} as defined by
	 *            the accompanying
	 *            {@link GraphWriter#writeEdge(OutputStream, Edge)} method.
	 * @param edgeAttachMethod
	 *            a function that creates re-attaches a {@link Edge} to a
	 *            {@link Host} object.
	 */
	@Override
	public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeAttachMethod)
			throws IOException {
		final Input input = new Input(inputStream);
		readHeader(input);
		final Attachable<Edge> attachable = kryo.readObject(input, DetachedEdge.class);
		return edgeAttachMethod.apply(attachable);
	}

	/**
	 * Read a {@link VertexProperty} from output generated by
	 * {@link GryoWriter#writeVertexProperty(OutputStream, VertexProperty)} or
	 * via an {@link VertexProperty} passed to
	 * {@link GryoWriter#writeObject(OutputStream, Object)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link VertexProperty} as
	 *            written by the accompanying
	 *            {@link GraphWriter#writeVertexProperty(OutputStream, VertexProperty)}
	 *            method.
	 * @param vertexPropertyAttachMethod
	 *            a function that creates re-attaches a {@link VertexProperty}
	 *            to a {@link Host} object.
	 */
	@Override
	public VertexProperty readVertexProperty(final InputStream inputStream,
			final Function<Attachable<VertexProperty>, VertexProperty> vertexPropertyAttachMethod) throws IOException {
		final Input input = new Input(inputStream);
		readHeader(input);
		final Attachable<VertexProperty> attachable = kryo.readObject(input, DetachedVertexProperty.class);
		return vertexPropertyAttachMethod.apply(attachable);
	}

	/**
	 * Read a {@link Property} from output generated by
	 * {@link GryoWriter#writeProperty(OutputStream, Property)} or via an
	 * {@link Property} passed to
	 * {@link GryoWriter#writeObject(OutputStream, Object)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Property} as written
	 *            by the accompanying
	 *            {@link GraphWriter#writeProperty(OutputStream, Property)}
	 *            method.
	 * @param propertyAttachMethod
	 *            a function that creates re-attaches a {@link Property} to a
	 *            {@link Host} object.
	 */
	@Override
	public Property readProperty(final InputStream inputStream,
			final Function<Attachable<Property>, Property> propertyAttachMethod) throws IOException {
		final Input input = new Input(inputStream);
		readHeader(input);
		final Attachable<Property> attachable = kryo.readObject(input, DetachedProperty.class);
		return propertyAttachMethod.apply(attachable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <C> C readObject(final InputStream inputStream, final Class<? extends C> clazz) throws IOException {
		return clazz.cast(this.kryo.readClassAndObject(new Input(inputStream)));
	}

	private Vertex readVertexInternal(final Function<Attachable<Vertex>, Vertex> vertexMaker,
			final Function<Attachable<Edge>, Edge> edgeMaker, final Direction d, final Input input) throws IOException {
		readHeader(input);
		final StarGraph starGraph = kryo.readObject(input, StarGraph.class);

		// read the terminator
		kryo.readClassAndObject(input);

		final Vertex v = vertexMaker.apply(starGraph.getStarVertex());
		if (edgeMaker != null)
			starGraph.getStarVertex().edges(d).forEachRemaining(e -> edgeMaker.apply((Attachable<Edge>) e));
		return v;
	}

	private void readHeader(final Input input) throws IOException {
		if (!Arrays.equals(GryoMapper.GIO, input.readBytes(3)))
			throw new IOException("Invalid format - first three bytes of header do not match expected value");

		// skip the next 13 bytes - for future use
		input.readBytes(13);
	}

	public static Builder build() {
		return new Builder();
	}

	public final static class Builder implements ReaderBuilder<JanusGraphGryoReader> {

		private long batchSize = 10000;
		/**
		 * Always use the most recent gryo version by default
		 */
		private Mapper<Kryo> gryoMapper = GryoMapper.build().create();

		private Builder() {
		}

		/**
		 * Number of mutations to perform before a commit is executed when using
		 * {@link JanusGraphGryoReader#readGraph(InputStream, Graph)}.
		 */
		public Builder batchSize(final long batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		/**
		 * Supply a mapper {@link GryoMapper} instance to use as the serializer
		 * for the {@code KryoWriter}.
		 */
		public Builder mapper(final Mapper<Kryo> gryoMapper) {
			this.gryoMapper = gryoMapper;
			return this;
		}

		public JanusGraphGryoReader create() {
			return new JanusGraphGryoReader(batchSize, this.gryoMapper);
		}

	}

	private class VertexInputIterator implements Iterator<Vertex> {
		private final Input input;
		private final Function<Attachable<Vertex>, Vertex> vertexMaker;
		private final Direction d;
		private final Function<Attachable<Edge>, Edge> edgeMaker;

		public VertexInputIterator(final Input input, final Function<Attachable<Vertex>, Vertex> vertexMaker,
				final Direction d, final Function<Attachable<Edge>, Edge> edgeMaker) {
			this.input = input;
			this.d = d;
			this.edgeMaker = edgeMaker;
			this.vertexMaker = vertexMaker;
		}

		@Override
		public boolean hasNext() {
			return !input.eof();
		}

		@Override
		public Vertex next() {
			try {
				return readVertexInternal(vertexMaker, edgeMaker, d, input);
			} catch (Exception ex) {
				log.error(ex.getMessage());
				ex.printStackTrace();
				throw new RuntimeException(ex);
			}
		}
	}
}
