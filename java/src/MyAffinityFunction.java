import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;

/**
 * This class provides the affinity collocation functionality as it implements the Interface {@link AffinityFunction}.
 * It uses a clustering-based fragmentation on a relaxation attribute (which is of type {@code T}) to obtain a mapping
 * of the horizontal fragmentation of a table to partitions and to a {@link ClusterNode} of the
 * {@link org.apache.ignite.IgniteCluster}.
 * Furthermore, it is also used to derive a fragmentation of the Info-table based on the fragmentation of the primary
 * table.
 *
 * @param <T> The domain of the relaxation attribute (e.g. String or Integer)
 */
public class MyAffinityFunction<T> implements AffinityFunction, Serializable {

    /**
     * Number of partitions (should be same for
     */
    private int parts;


    /**
     * Default value for clustering algorithm threshold.
     */
    private static final double DFLT_ALPHA = 0.2;

    /**
     * Similarity threshold for clustering algorithm and affinity key mapping.
     */
    private final double alpha;

    /**
     * HashMap for the pairwise similarities of predefined MeSH terms (active domain here)
     */
    private HashMap<String, Double> similarities;

    /**
     * Array of all the terms occuring in the active domain of the relaxation attribute
     */
    private T[] terms;

    /**
     * The clusters obtained from the clustering algorithm (will be initialized in constructor). Also provides a
     * mapping of each cluster to a partition with abusing the index of the cluster in this ArrayList.
     */
    private ArrayList<Cluster<T>> clusters;


//##################### Constructors ######################

    /**
     * Constructor using default value for alpha.
     * For details see {@link MyAffinityFunction#MyAffinityFunction(double, Object[])}.
     *
     * @param terms Array containing active domain of relaxation attribute
     */
    public MyAffinityFunction(T[] terms) {
        this(DFLT_ALPHA, terms);
    }

    /**
     * Constructor for a affinity function based on clustering-based fragmentation with similarity calculation.
     * Obtains the clusters from the clustering algorithm for later usage in the affinity collocation and partition
     * assignment.
     *
     * @param alpha Threshold for clustering algorithm
     * @param terms Array containing active domain of relaxation attribute
     */
    public MyAffinityFunction(double alpha, T[] terms) {
        this.alpha = alpha;
        this.terms = terms;

        // Init similarities HashMap
        // TODO read similarities from csv (prepare csv)
        // TODO maybe test other implementations (e.g. other data structure, as cache, ...)
        similarities = new HashMap<>(terms.length);
        similarities.put("C0004096+C0010200", 0.2);      //0.2<>Asthma(C0004096)<>Cough(C0010200)
        similarities.put("C0004096+C0021400", 0.2);      //0.2<>Asthma(C0004096)<>Influenza(C0021400)
        similarities.put("C0004096+C0040185", 0.1429);   //0.1429<>Asthma(C0004096)<>Tibial Fracture(C0040185)
        similarities.put("C0004096+C0041601", 0.1429);   //0.1429<>Asthma(C0004096)<>Ulna Fracture(C0041601)
        similarities.put("C0010200+C0021400", 0.2);      //0.2<>Cough(C0010200)<>Influenza(C0021400)
        similarities.put("C0010200+C0040185", 0.1429);   //0.1429<>Cough(C0010200)<>Tibial Fracture(C0040185)
        similarities.put("C0010200+C0041601", 0.1429);   //0.1429<>Cough(C0010200)<>Ulna Fracture(C0041601)
        similarities.put("C0021400+C0040185", 0.1429);   //0.1429<>Influenza(C0021400)<>Tibial Fracture(C0040185)
        similarities.put("C0021400+C0041601", 0.1429);   //0.1429<>Influenza(C0021400)<>Ulna Fracture(C0041601)
        similarities.put("C0040185+C0041601", 0.3333);   //.3333<>Tibial Fracture(C0040185)<>Ulna Fracture(C0041601)

        // Calculate clustering (index of cluster implies mapping of cluster to partition)
        // TODO read terms from csv instead of as predefined String array
        List<T> termList = new LinkedList<>(Arrays.asList(terms));
        clusters = clustering(termList);

        // each cluster is assigned to a partition and for each cluster the same partition is used also for the
        // derived fragmentation
        // TODO instead of same partition as the cluster use one extra partition per node to store ALL derived fragments
        parts = clusters.size();

        // Debug
        System.out.println("Clustering: Size=" + clusters.size() + ", Partitions=" + parts);
        for (Cluster<T> c : clusters)
            System.out.println(c);
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
     *
     * @return Number of Partitions
     */
    @Override
    public int partitions() {
        return parts;
    }


    /**
     * Returns the partition mapping for the given key.
     * See {@link AffinityFunction#partition(Object)} for more details.
     * @param key Key
     * @return Partition
     */
    @Override
    public int partition(Object key) {
        if (key == null)
            throw new IllegalArgumentException("The key passed to the MyAffinityFunction's method " +
                    "partition(Object key) was null.");

        int partition = -1;

        // If the key is of type BinaryObject, then deserialize it first
        if (key instanceof BinaryObject) {
            BinaryObject binary = (BinaryObject) key;
            key = binary.deserialize();
            System.out.println("Deserialized the BinaryObject: " + key);        // Debug
        }

        // If the key is of type IllKey, then find the partition based on the clustering (i-th cluster = i-th partition)
        // If the key is of type InfoKey, then find the partition based on the derived fragmentation
        if (key instanceof IllKey) {
            System.out.println("partition(key) got IllKey! Assigning partition based on clustering ...");   //Debug
            IllKey illKey = (IllKey) key;
            partition = identifyCluster((T) illKey.getDisease());
        }
        else if (key instanceof InfoKey) {
            System.out.println("partition(key) got InfoKey! Assigning partition based on derived fragmentation ..."); //Debug
            partition = ((InfoKey) key).getAffinityPartition();
        } else
            System.out.println("!!!ERROR!!! key object is of some other type: " + key.getClass() + "!!!!!!!!!!!!!!!!!!!");

        System.out.println("partition(key) returns partition number " + partition);
        return partition;
    }


    /**
     * This function maps all the partitions of this cache to nodes of the cluster. The outer list
     * of the returned nested lists is indexed by the partition number and stores the mapping of that
     * partition to a list of nodes of the cluster (inner lists).
     * The current functionality is limited to map each partition only to one primary node without backups.
     *
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
        System.out.println("assignPartitions: parts=" + parts + ", allNodes=" + Arrays.toString(allNodes.toArray()));   //Debug
        // Assign each partition to a node (no replication)
        for (int i = 0; i < this.parts; i++) {
            List<ClusterNode> assignedNodes = this.assignPartition(i, allNodes);
            result.add(i, assignedNodes);
            System.out.println("Assigned partition " + i + " to node " + assignedNodes.get(0).id());    // Debug
        }

        return result;
    }




//##################### Clustering-based Fragmentation (Partition-Mappings, Cluster-Algorithm)  ######################


    /**
     * Assign a certain partition to a certain node in the cluster.
     * <p>
     * This functionality can also be optionally extended later to map partitions to several nodes,
     * e.g. primary and backup nodes according to the clustering-based fragmentation.
     *
     * @param partition The number of the partition to assign to a node
     * @param allNodes  A list of all nodes in the cluster, can be provided by e.g.
     *                  {@see AffinityFunctionContext#currentTopologySnapshot()}
     * @return List of cluster nodes (currently containing only one node) to which the partition is mapped
     */
    private List<ClusterNode> assignPartition(int partition, List<ClusterNode> allNodes) {
        // TODO Assign partition i to the node it belongs to according to the clustering-based fragmentation
        // TODO or according to the bin packing problem?!?!
        // TODO adapt assingment to depend on number of nodes and  (currently mod)
        /* (assuming i partitions and i nodes)
           The i-th partition is assigned to the (i % number of nodes)-th node
           Example: 2 Nodes
                - partition 0 with respiratory diseases --> node 0, partition 1 with fractures --> node 1
        */
        System.out.print("Assigning partition " + partition + " to node " + allNodes.get(partition).id());     // Debug
        List<ClusterNode> result = new ArrayList<>();
        result.add(allNodes.get(partition % allNodes.size()));
        return result;
    }


    /**
     * This method calculates the clustering of the active domain of the relaxation attribute in a table.
     * All the values of the active domain of the relaxation attribute (column) are assigned to a cluster
     * (Note: do not mix up with the cluster of nodes storing data! Here clusters are the partitions). The
     * resulting assignment is a list of clusters. For details see {@link Cluster}
     *
     * @param activeDomain The active domain of the relaxation attribute
     */
    private ArrayList<Cluster<T>> clustering(@NotNull List<T> activeDomain) {
        if (activeDomain.isEmpty())
            throw new IllegalArgumentException("The list containing the active domain is empty.");


        // Initialize the first cluster with all values from the active domain (except head element)
        ArrayList<Cluster<T>> clusters = new ArrayList<>();
        T headElement = activeDomain.remove(0);
        Cluster<T> c = new Cluster<>(headElement, new HashSet<>(activeDomain));
        clusters.add(0, c);

        // Initial minimal similarity for next head
        double sim_min = 1.0;
        for (T term : c.getAdom()) {       // note: adom does not contain head!
            double sim = similarity(term, headElement);
            if (sim < sim_min)
                sim_min = sim;
        }


        // Clustering procedure
        int i = 0;
        while (sim_min < alpha) {

            // Get the element with the smallest similarity to any cluster head (argmin of similarity)
            T nextHead = null;
            for (int j = 0; j <= i; j++) {
                c = clusters.get(j);
                T head_j = c.getHead();
                Set<T> adom = c.getAdom();
                for (T term : adom) {
                    if (similarity(term, head_j) == sim_min) {
                        nextHead = term;
                        adom.remove(term);      // remove new found head from the old cluster
                        break;
                    }
                }
                // Already found next head?
                if (nextHead != null)
                    break;
            }


            // Create next cluster
            HashSet<T> set = new HashSet<>();
            for (int j = 0; j <= i; j++) {

                c = clusters.get(j);
                T head_j = c.getHead();
                Set<T> adom = c.getAdom();

                // Move terms from the old clusters to the new cluster if they are more similar to nextHead
                for (T term : adom) {
                    if (similarity(term, head_j) <= similarity(term, nextHead)) {
                        set.add(term);          // add to new cluster
                        adom.remove(term);      // remove from current cluster
                    }
                }
            }

            i = i + 1;
            c = new Cluster<>(nextHead, set);
            clusters.add(i, c);


            // Update minimal similarity
            double min = 1;
            for (int j = 0; j <= i; j++) {

                c = clusters.get(j);
                T head_j = c.getHead();
                for (T term : c.getAdom()) {
                    double s = similarity(term, head_j);
                    if (min >= s)
                        min = s;
                }
            }
            sim_min = min;
        }


        return clusters;
    }


    private double similarity(T term1, T term2) {

        // TODO ??? ??? ???

        // Map term to CUI (Concept Unique Identifier)
        HashMap<String, String> map = new HashMap<>();
        map.put("Asthma", "C0004096");
        map.put("Cough", "C0010200");
        map.put("Influenza", "C0021400");
        map.put("Tibial Fracture", "C0040185");
        map.put("Ulna Fracture", "C0041601");

        // Get key and return similarity value
        String key1 = map.get(term1) + "+" + map.get(term2);
        String key2 = map.get(term2) + "+" + map.get(term1);

        double sim;
        if (similarities.containsKey(key1))
            sim = similarities.get(key1);
        else
            sim = similarities.get(key2);
        return sim;
    }


    /**
     * This method identifies the cluster to a given term.
     * @param term The term to match to a cluster
     * @return Number of the cluster
     */
    private int identifyCluster(T term) {

        // Identify the cluster to which this term belongs
        // First check if term is equal to the head of i-th cluster (store heads during check for further identification)
        T[] head = (T[]) new Object[clusters.size()];
        for (int i = 0; i < clusters.size(); i++) {
            head[i] = clusters.get(i).getHead();
            if (term.equals(head[i])) {
                System.out.println("return cluster=partition=" + i);        // Debug
                return i;
            }
        }

        // No head matched -> calculate similarity of t to each of the heads and find maximum similarity
        double max = -1;
        int argMax = -1;
        for (int i = 0; i < head.length; i++) {
            double sim = similarity(term, head[i]);
            if (max < sim) {
                max = sim;
                argMax = i;
            }
        }

        // Debug
        System.out.println("return cluster=partition=" + argMax);
        return argMax;
    }


//################################# Derived Fragmentation  ########################################




//##################### MAIN-Method  ######################

    /**
     * Test unit
     *
     * @param args
     */
    public static void main(String[] args) {

        String[] terms = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};

        // Prepare clustering test
        MyAffinityFunction<String> maf = new MyAffinityFunction<>(0.2, terms);

        // Test clustering
        ArrayList<Cluster<String>> clusters = maf.clusters;
        for (Cluster<String> c : clusters) {
            System.out.println(c);
        }
    }


}
