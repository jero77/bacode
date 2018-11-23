import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;

import java.util.*;


public class MyAffinityFunction implements AffinityFunction {


    public static final int DFLT_PARTITION_COUNT = 1024;

    private int parts;


//##################### Constructors ######################


    /**
     * Initializes affinity with specified number of partitions (this are only primary partitions
     * as backups are not considered here).
     * @param parts
     */
    public MyAffinityFunction(int parts) {
        this.parts = parts;
    }

//##################### Overwritten Methods ######################

    @Override
    public void reset() {
        // No-op.
    }

    @Override
    public void removeNode(UUID nodeId) {
        // No-op
    }


    /**
     * Gets the total number of partitions. There should be at least as much partitions as nodes,
     * so that each node can host one partition.
     * @return Number of Partitions
     */
    @Override
    public int partitions() {
        return parts;
    }


    /**
     *
     * @param key
     * @return
     */
    @Override
    public int partition(Object key) {
        if (key == null)
            throw new IllegalArgumentException("The key passed to the MyAffinityFunction's method partition(Object key)" +
                    " was null." );


        // TODO map key to partition (fragment) according to the clustering-based fragmentation


        return 0;
    }


    /**
     * This function maps all the partitions of this cache to nodes of the cluster. The outer list
     * of the returned nested lists is indexed by the partition number and stores the mapping of that
     * partition to a list of nodes of the cluster (inner lists).
     * The current functionality is limited to map each partition only to one primary node without backups.
     * @param affCtx Context to be passed to the function automatically. For details see {@link AffinityFunction}.
     * @return Assignment of partitions to nodes
     */
    @Override
    public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        if (affCtx == null)
            throw new IllegalArgumentException("AffinityFunctionContext passed to the MyAffinityFunction's method " +
                    "assignPartitions(AffinityFunctionContext affCtx) was null.");

        // Resulting list maps each partition to a list of nodes
        List<List<ClusterNode>> result = new ArrayList<>(this.parts);

        // Get all the nodes in the cluster
        List<ClusterNode> allNodes = affCtx.currentTopologySnapshot();

        // Assign each partition
        for (int i = 0; i < this.parts; i++) {
            List<ClusterNode> assignedNodes = this.assignPartition(i, allNodes);
            result.add(i, assignedNodes);
        }

        return result;
    }




//##################### Getter & Setter ######################

    /**
     * Set the number of partitions.
     * @param parts Number of partitions
     */
    public void setParts(int parts) {
        this.parts = parts;
    }



//##################### Clustering-based Fragmentation (Partition-Mappings, Cluster-Algorithm)  ######################


    /**
     * Assign a certain partition to a certain node in the cluster.
     *
     * This functionality can also be optionally extended later to map partitions to several nodes,
     * e.g. primary and backup nodes according to the clustering-based fragmentation.
     * @param partition The number of the partition to assign to a node
     * @param allNodes A list of all nodes in the cluster, e.g. {@see AffinityFunctionContext#currentTopologySnapshot()}
     * @return List of cluster nodes (currently containing only one node) to which the partition is mapped
     */
    private List<ClusterNode> assignPartition(int partition, List<ClusterNode> allNodes) {
        // TODO Assign partition i to node it belongs to according to the clustering-based fragmentation

    }


    /**
     * This method calculates the clustering of the active domain of the relaxation attribute in a table.
     * All the values of the active domain of the relaxation attribute (column) are assigned to a cluster
     * (Note: do not mix up with the cluster of nodes storing data! Here clusters are the partitions). The
     * resulting assignment is a list of clusters. For details see {@link Cluster}
     * @param activeDomain The active domain of the relaxation attribute
     * @param alpha The similarity threshold
     */
    private ArrayList<Cluster<String>> clustering(List<String> activeDomain, double alpha) {
        if (activeDomain.isEmpty())
            throw new IllegalArgumentException("The list containing the active domain is empty.");


        // Initialize the first cluster with all values from the active domain (except head element)
        ArrayList<Cluster<String>> clusters = new ArrayList<>();
        String headElement = activeDomain.remove(0);
        Cluster<String> c = new Cluster<String>(headElement, new HashSet<String>(activeDomain));
        clusters.add(0, c);

        // TODO sim_min, similarity

        int i = 0;
        /*
         while sim_min < alpha
            for (int j = 0; j <= i; j++) {
                set = {b | b aus Cluster_j; b != head_j; sim(b, head_j) == sim_min}
            }
            String nextHead = set.get(0);

            for (int j = 0; j <= i; j++) {
                set = {b | b aus Cluster_j; b != head_j; sim(b, head_j) <= sim(b, nextHead)}
            }
            i = i + 1;
            c = new Cluster<String>(nextHead, set)
            clusters.add(i, c);

            for (int j = 0; j <= i; j++) {
                sim_min = min{sim(d, head_j) | d aus Cluster_j; d != head_j}
            }
         end while
          */


        return clusters;
    }

}
