package org.janusgraph.graphdb.tinkerpop.io.gryo;

import org.apache.tinkerpop.gremlin.structure.io.Io;

public class JanusGraphIoCore {

	private JanusGraphIoCore() {
	}

	/**
	 * Creates a JanusGraph-aware Gryo-based Reader
	 * {@link org.apache.tinkerpop.gremlin.structure.io.Io.Builder}.
	 */
	public static Io.Builder<JanusGraphGryoIo> gryo() {
		return JanusGraphGryoIo.build();
	}

	public static Io.Builder createIoBuilder(final String graphFormat)
			throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		final Class<Io.Builder> ioBuilderClass = (Class<Io.Builder>) Class.forName(graphFormat);
		final Io.Builder ioBuilder = ioBuilderClass.newInstance();
		return ioBuilder;
	}

}
