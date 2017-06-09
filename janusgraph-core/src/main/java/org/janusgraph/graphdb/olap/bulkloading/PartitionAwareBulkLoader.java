package org.janusgraph.graphdb.olap.bulkloading;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoader;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.graphdb.database.StandardJanusGraph;

/**
 * {@link org.apache.tinkerpop.gremlin.process.computer.bulkloading.OneTimeBulkLoader}
 * is a
 * {@link org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoader}
 * implementation that should be used for initial bulk loads. In contrast to
 * {@link org.apache.tinkerpop.gremlin.process.computer.bulkloading.IncrementalBulkLoader}
 * it doesn't store temporary identifiers in the write graph nor does it attempt
 * to find existing elements, instead it only clones each element from the
 * source graph.
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class PartitionAwareBulkLoader implements BulkLoader {

	/**
	 * Creates a clone of the given vertex in the given graph.
	 */
	@Override
	public Vertex getOrCreateVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
		GraphTraversal<Vertex, Vertex> iterator = g.V().has(vertex.label(), getVertexIdProperty(),
				vertex.id().toString());
		// here we assume that given vertex is StarVertex
		StarVertex starVertex = StarGraph.of(vertex).getStarVertex();
		// graph is an instance of JanusGraph
		JanusGraph janusGraph = (StandardJanusGraph) graph;
		// keep the user assigned id as the bulk loader id, which will be
		// indexed
		return iterator.hasNext() ? iterator.next()
				: janusGraph.addStarVertex(starVertex, T.label, vertex.label(), getVertexIdProperty(),
						vertex.id().toString());
	}

	/**
	 * Creates a clone of the given edge between the given in- and out-vertices.
	 */
	@Override
	public Edge getOrCreateEdge(final Edge edge, final Vertex outVertex, final Vertex inVertex, final Graph graph,
			final GraphTraversalSource g) {
		return createEdge(edge, outVertex, inVertex, graph, g);
	}

	/**
	 * Creates a clone of the given property for the given vertex.
	 */
	@Override
	public VertexProperty getOrCreateVertexProperty(final VertexProperty<?> property, final Vertex vertex,
			final Graph graph, final GraphTraversalSource g) {
		return createVertexProperty(property, vertex, graph, g);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Vertex getVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
		return getVertexById(vertex.id(), graph, g);
	}

	/**
	 * Always returns false
	 */
	@Override
	public boolean useUserSuppliedIds() {
		return false;
	}

	/**
	 * Always returns false.
	 */
	@Override
	public boolean keepOriginalIds() {
		return false;
	}

	@Override
	public void configure(Configuration configuration) {
		// TODO Auto-generated method stub

	}
}
