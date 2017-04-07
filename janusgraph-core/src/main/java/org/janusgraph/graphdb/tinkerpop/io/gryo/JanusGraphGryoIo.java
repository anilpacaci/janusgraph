package org.janusgraph.graphdb.tinkerpop.io.gryo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.janusgraph.core.JanusGraph;

/**
 * Gryo IO with JanusGraph Specific Reader Implementation
 * 
 * @author apacaci
 *
 */
public class JanusGraphGryoIo implements Io<JanusGraphGryoReader.Builder, GryoWriter.Builder, GryoMapper.Builder> {
	private final IoRegistry registry;
	private final JanusGraph graph;

	private JanusGraphGryoIo(final IoRegistry registry, final JanusGraph graph) {
		this.registry = registry;
		this.graph = graph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JanusGraphGryoReader.Builder reader() {
		return JanusGraphGryoReader.build().mapper(mapper().create());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GryoWriter.Builder writer() {
		return GryoWriter.build().mapper(mapper().create());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GryoMapper.Builder mapper() {
		return (null == this.registry) ? GryoMapper.build() : GryoMapper.build().addRegistry(this.registry);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeGraph(final String file) throws IOException {
		try (final OutputStream out = new FileOutputStream(file)) {
			writer().create().writeGraph(out, graph);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readGraph(final String file) throws IOException {
		reader().create().readGraph(file, graph);
	}

	public static Io.Builder<JanusGraphGryoIo> build() {
		return new Builder();
	}

	public final static class Builder implements Io.Builder<JanusGraphGryoIo> {

		private IoRegistry registry = null;
		private JanusGraph graph;
		private Consumer<Mapper.Builder> onMapper = null;

		@Override
		public Io.Builder<JanusGraphGryoIo> registry(final IoRegistry registry) {
			this.registry = registry;
			return this;
		}

		@Override
		public Io.Builder<JanusGraphGryoIo> graph(final Graph g) {
			if (!(g instanceof JanusGraph))
				throw new IllegalArgumentException("Graph should be an instance of JanusGraph for JanusGraphGryoIO");
			this.graph = (JanusGraph) g;
			return this;
		}

		@Override
		public JanusGraphGryoIo create() {
			if (null == graph)
				throw new IllegalArgumentException("The graph argument was not specified");
			return new JanusGraphGryoIo(registry, graph);
		}

		public Io.Builder<JanusGraphGryoIo> graph(final JanusGraph g) {
			this.graph = (JanusGraph) g;
			return this;
		}

		@Override
		public org.apache.tinkerpop.gremlin.structure.io.Io.Builder<? extends Io> onMapper(
				Consumer<org.apache.tinkerpop.gremlin.structure.io.Mapper.Builder> onMapper) {
			this.onMapper = onMapper;
			return this;
		}
	}
}