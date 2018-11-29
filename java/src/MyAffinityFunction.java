import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;

import java.util.*;

/**
 * This class provides the affinity collocation functionality as it implements the Interface {@link AffinityFunction}.
 * It uses a clustering-based fragmentation on a relaxation attribute (which is of type {@code T}) to obtain a mapping
 * of the horizontal fragmentation of a table to partitions and to a {@link ClusterNode} of the
 * {@link org.apache.ignite.IgniteCluster}.
 * @param <T> The domain of the relaxation attribute (e.g. String or Integer)
 */
public class MyAffinityFunction<T> implements AffinityFunction {

    /**
     * Maximum partition count
     */
    public static final int DFLT_PARTITION_COUNT = 1024;

    /**
     * Number of partitions (should be same for
     */
    private int parts;


    /**
     * HashMap for the pairwise similarities of predefined MeSH terms (active domain here)
     */
    private HashMap<String, Double> similarities;

    /**
     * Array of all the terms occuring in the active domain of the relaxation attribute
     */
    private T[] terms;

    /**
     * The clusters obtained from the clustering algorithm (will be initialized in constructor).
     * TODO maybe Cluster<?> more generic
     */
    private ArrayList<Cluster<T>> clusters;


//##################### Constructors ######################


    /**
     * Initializes affinity with specified number of partitions (this are only primary partitions
     * as backups are not considered here). Also obtains the clusters from the clustering algorithm
     * for later usage in the affinity collocation
     *
     * @param parts Number of partitions
     * @param alpha Threshold for clustering algorithm
     * @param
     */
    public MyAffinityFunction(int parts, double alpha, T[] terms) {
        this.parts = parts;
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

        // Clustering
        // TODO read terms from csv instead as predefined String array
        List<T> termList = new LinkedList<T>();
        for (T term : terms)
            termList.add(term);
        clusters = clustering(termList, alpha);
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
     * @param key
     * @return
     */
    @Override
    public int partition(Object key) {
        if (key == null)
            throw new IllegalArgumentException("The key passed to the MyAffinityFunction's method " +
                    "partition(Object key) was null.");

        // Assume we have the affinity key of the Ill-Cache here which is a String describing a disease
        // TODO adapt to ghenerification
        String disease = null;
        if (key instanceof String)
            disease = (String) key;
        System.out.println(disease);

        // TODO map key to partition (fragment) according to the clustering-based fragmentation
        // For sake of simplicity: Respiratory --> partition 0, Fracture --> partition 1
        int partition = -1;
        if (disease.matches("("+ terms[0] +")|(" + terms[1] + ")|(" + terms[2] + ")"))
            partition = 0;
        else
            if (disease.matches("("+ terms[3] +")|(" + terms[4] + ")"))
                partition = 1;

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

        // Assign each partition to a node (no replication)
        for (int i = 0; i < this.parts; i++) {
            List<ClusterNode> assignedNodes = this.assignPartition(i, allNodes);
            result.add(i, assignedNodes);
        }

        return result;
    }


//##################### Getter & Setter ######################

    /**
     * Set the number of partitions.
     *
     * @param parts Number of partitions
     */
    public void setParts(int parts) {
        this.parts = parts;
    }


//##################### Clustering-based Fragmentation (Partition-Mappings, Cluster-Algorithm)  ######################


    /**
     * Assign a certain partition to a certain node in the cluster.
     * <p>
     * This functionality can also be optionally extended later to map partitions to several nodes,
     * e.g. primary and backup nodes according to the clustering-based fragmentation.
     *
     * @param partition The number of the partition to assign to a node
     * @param allNodes  A list of all nodes in the cluster, e.g. {@see AffinityFunctionContext#currentTopologySnapshot()}
     * @return List of cluster nodes (currently containing only one node) to which the partition is mapped
     */
    private List<ClusterNode> assignPartition(int partition, List<ClusterNode> allNodes) {
        // TODO Assign partition i to the node it belongs to according to the clustering-based fragmentation
        // For sake of simplicity:
        // partition 0 with respiratory diseases --> first node, partitions 1 with fractures --> second node
        List<ClusterNode> result = new ArrayList<ClusterNode>();
        result.add(allNodes.get(partition));
        return result;
    }

    /**
     * This method calculates the clustering of the active domain of the relaxation attribute in a table.
     * All the values of the active domain of the relaxation attribute (column) are assigned to a cluster
     * (Note: do not mix up with the cluster of nodes storing data! Here clusters are the partitions). The
     * resulting assignment is a list of clusters. For details see {@link Cluster}
     *  @param activeDomain The active domain of the relaxation attribute
     * @param alpha        The similarity threshold
     */
    private ArrayList<Cluster<T>> clustering(List<T> activeDomain, double alpha) {
        if (activeDomain.isEmpty())
            throw new IllegalArgumentException("The list containing the active domain is empty.");


        // Initialize the first cluster with all values from the active domain (except head element)
        ArrayList<Cluster<T>> clusters = new ArrayList<>();
        T headElement = activeDomain.remove(0);
        Cluster<T> c = new Cluster<T>(headElement, new HashSet<T>(activeDomain));
        clusters.add(0, c);

        double sim_min = 1.0;
        for (T term : c.getAdom()) {       // note: adom does not contain head!
            double sim = similarity(term, headElement);
            if (sim < sim_min)
                sim_min = sim;
        }


        // Clustering procedure
        int i = 0;
        while (sim_min < alpha) {

            // Get the element with the smallest similarity to any cluster head (argmin)
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
            c = new Cluster<T>(nextHead, set);
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

        double sim = 0;
        if (similarities.containsKey(key1)) {
            sim = similarities.get(key1);
        } else {
            sim = similarities.get(key2);
        }
        return sim;
    }


//##################### MAIN-Method  ######################

    /**
     * Test unit
     *
     * @param args
     */
    public static void main(String[] args) {

        String[] terms = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};

        // Prepare clustering test
        MyAffinityFunction<String> maf = new MyAffinityFunction<String>(2, 0.2, terms);

        // Test clustering
        ArrayList<Cluster<String>> clusters = maf.clusters;
        for (Cluster<String> c : clusters) {
            System.out.println(c);
        }
    }


}
