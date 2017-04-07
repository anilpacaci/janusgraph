package org.janusgraph.graphdb.tinkerpop.io.gryo;


/**
 * A utility interface to keep track of old-new ID mappings during Titan Gryo Graph Loading
 * @author apacaci
 *
 */
public interface IDMapping {
	
	public static String IN_MEMORY_IDMAPPING = "inmemory";
	public static String MEMCACHED_IDMAPPING = "memcached";
	
	/**
	 * Partition ID of the element specifid by ID
	 * @param originialID
	 * @return negative number if element is not in history
	 */
	public Long getCurrentID(Long originialID);
	
	/**
	 * Set partition ID for the element specified by id
	 * @param originalID
	 * @param currentID
	 */
	public void setCurrentID(Long originialID, Long currentID);

}
