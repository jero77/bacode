import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * This class provides the affinity collocation functionality as it implements the Interface {@link AffinityFunction}.
 * It uses a clustering-based fragmentation on a relaxation attribute (which is of type {@link String}) to obtain a
 * mapping of the horizontal fragmentation of a table to partitions and to a {@link ClusterNode} of the
 * {@link org.apache.ignite.IgniteCluster}.
 * Furthermore, it is also used to derive a fragmentation of the Info-table based on the fragmentation of the primary
 * table.
 */
public class MyAffinityFunction implements AffinityFunction, Serializable {

    /**
     * Number of partitions (should be same for
     */
    private int parts;

    /**
     * Default value for clustering algorithm threshold.
     */
    private static final double DFLT_ALPHA = 0.2;

    /**
     * Default terms (String)
     */
    private static final String[] DFLT_TERMS = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};

    /**
     * Similarity threshold for clustering algorithm and affinity key mapping.
     */
    private final double alpha;

    /**
     * HashMap for the pairwise similarities of predefined MeSH terms (active domain here), stored with key
     * "term1+term2" and value as double
     */
    private HashMap<String, Double> similarities;

    /**
     * ArrayList of all the terms occuring in the active domain of the relaxation attribute
     */
    private ArrayList<String> terms;

    /**
     * The clusters obtained from the clustering algorithm (will be initialized in constructor). Also provides a
     * mapping of each cluster to a partition with abusing the index of the cluster in this ArrayList.
     */
    private ArrayList<Cluster<String>> clusters;




//##################### Constructors ######################

    /**
     * Constructor using default value for alpha and testing constructor.
     * For details see {@link MyAffinityFunction#MyAffinityFunction(double)}.
     */
    public MyAffinityFunction() {
        this(DFLT_ALPHA);
    }

    /**
     * Constructor for a affinity function based on clustering-based fragmentation with similarity calculation.
     * Obtains the clusters from the clustering algorithm for later usage in the affinity collocation and partition
     * assignment.
     * NOTE: This constructor should only be used for code testing purposes, it only has the active domain {Cough,
     * Asthma, Ulna Fracture, Tibial Fracture, Influenza} with predefined similarities (obtained from UMLS::Similarity).
     * @param alpha Threshold for clustering algorithm
     */
    public MyAffinityFunction(double alpha) {
        this.alpha = alpha;
        this.terms = new ArrayList<>();
        for (String s : DFLT_TERMS)
            terms.add(s);

        // Init similarities HashMap with predefined active domain DFLT_TERMS
        this.similarities = new HashMap<>();
        this.similarities.put("Asthma+Cough", 0.2);                         //0.2<>Asthma<>Cough
        this.similarities.put("Asthma+Influenza", 0.2);                     //0.2<>Asthma<>Influenza
        this.similarities.put("Asthma+Tibial Fracture", 0.1429);            //0.1429<>Asthma<>Tibial Fracture
        this.similarities.put("Asthma+Ulna Fracture", 0.1429);              //0.1429<>Asthma<>Ulna Fracture
        this.similarities.put("Cough+Influenza", 0.2);                      //0.2<>Cough<>Influenza
        this.similarities.put("Cough+Tibial Fracture", 0.1429);             //0.1429<>Cough)<>Tibial Fracture
        this.similarities.put("Cough+Ulna Fracture", 0.1429);               //0.1429<>Cough<>Ulna Fracture
        this.similarities.put("Influenza+Tibial Fracture", 0.1429);         //0.1429<>Influenza<>Tibial Fracture
        this.similarities.put("Influenza+Ulna Fracture", 0.1429);           //0.1429<>Influenza<>Ulna Fracture
        this.similarities.put("Tibial Fracture+Ulna Fracture", 0.3333);     //.3333<>Tibial Fracture<>Ulna Fracture

        // Calculate clustering (index of cluster implies mapping of cluster to partition)
        ArrayList<String> termList = new ArrayList<>(this.terms);
        this.clusters = clustering(termList);

        // each cluster is assigned to a partition and for each cluster the same partition is used also for the
        // derived fragmentation
        // TODO instead of same partition as the cluster use one extra partition per node to store ALL derived fragments
        this.parts = clusters.size();
    }

    /**
     *
     * @param alpha
     * @param termsFile
     * @param similaritiesFile
     */
    public MyAffinityFunction(double alpha, String termsFile, String similaritiesFile) {
        this.alpha = alpha;

        // init terms from file
        String line;
        this.terms = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(termsFile))) {
            while ((line = reader.readLine()) != null) {
                terms.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // TODO maybe test other implementations (e.g. other data structure, as cache, ...)
        // init similarities HashMap from file (form: similarity<>term1(CUI1)<>term2(CUI2)\n...)
        this.similarities = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(similaritiesFile))) {
            while ((line = reader.readLine()) != null) {
                // slice line by '<>' & remove (CUI)
                String[] things = line.split("<>");
                String key = things[1].replaceAll("\\(.*\\)", "");
                key = key + "+" + things[2].replaceAll("\\(.*\\)", "");
                this.similarities.put(key, Double.parseDouble(things[0]));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // calculate clustering (index of clusters implies mapping of cluster to partition)
        this.clusters = clustering(this.terms);
        this.parts = this.clusters.size();
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
        }

        // If the key is of type IllKey, then find the partition based on the clustering (i-th cluster = i-th partition)
        // If the key is of type InfoKey, then find the partition based on the derived fragmentation
        if (key instanceof IllKey) {
            IllKey illKey = (IllKey) key;
            partition = identifyCluster(illKey.getDisease());
        }
        else if (key instanceof InfoKey) {
            partition = ((InfoKey) key).getAffinityPartition();
        } else {
            throw new IllegalArgumentException("ERROR: The key object " + key + " is of some other type: "
                    + key.getClass() + "!");
        }
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
    private ArrayList<Cluster<String>> clustering(ArrayList<String> activeDomain) {
        if (activeDomain.isEmpty())
            throw new IllegalArgumentException("The list containing the active domain is empty.");


        // Initialize the first cluster with all values from the active domain (except head element)
        ArrayList<Cluster<String>> clusters = new ArrayList<>();
        String headElement = activeDomain.remove(0);
        Cluster<String> c = new Cluster<>(headElement, new HashSet<>(activeDomain));
        clusters.add(0, c);

        // Initial minimal similarity for next head
        double sim_min = 1.0;
        for (String term : c.getAdom()) {       // NOTE: adom does not contain head!
            double sim = similarity(term, headElement);
            if (sim < sim_min)
                sim_min = sim;
        }


        // Clustering procedure
        int i = 0;
        while (sim_min < alpha) {

            // Get the element with the smallest similarity to any cluster head (argmin of similarity)
            String nextHead = null;
            for (int j = 0; j <= i; j++) {
                c = clusters.get(j);
                String head_j = c.getHead();
                Set<String> adom = c.getAdom();
                for (String term : adom) {
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
            HashSet<String> nextAdom = new HashSet<>();
            for (int j = 0; j <= i; j++) {

                // Get head and adom
                c = clusters.get(j);
                String head_j = c.getHead();
                HashSet<String> adom_j = new HashSet<>(c.getAdom());

                // Move terms from old cluster's adom to new cluster's adom if they are more similar to nextHead
                Iterator<String> it = adom_j.iterator();    // for each trouble with ConcurrentModificationException
                while (it.hasNext()) {
                    String term = it.next();
                    if (similarity(term, head_j) <= similarity(term, nextHead)) {
                        it.remove();              // remove from current cluster's adom
                        nextAdom.add(term);             // add to new cluster's adom
                    }
                }

                // Set (possibly modified) adom_j for old cluster
                c.setAdom(adom_j);
            }

            // Add the new cluster to the clustering
            i = i + 1;
            c = new Cluster<String>(nextHead, nextAdom);
            clusters.add(i, c);

            // Update minimal similarity
            double min = 1;
            for (int j = 0; j <= i; j++) {

                c = clusters.get(j);
                String head_j = c.getHead();
                for (String term : c.getAdom()) {
                    double s = similarity(term, head_j);
                    if (min >= s)
                        min = s;
                }
            }
            sim_min = min;
        }

        return clusters;
    }


    private double similarity(String term1, String term2) {
        Double sim;
        sim = similarities.get(term1 + "+" + term2);
        if (sim == null)            // key is not contained (or value is null but this should NOT occur)
            sim = similarities.get(term2 + "+" + term1);
        return sim;
    }


    /**
     * This method identifies the cluster id to a given term.
     * @param term The term to match to a cluster
     * @return Number of the cluster
     */
    public int identifyCluster(String term) {

        // Identify the cluster to which this term belongs
        // First check if term is equal to the head of i-th cluster (store heads during check for further identification)
        String[] head = new String[clusters.size()];
        for (int i = 0; i < clusters.size(); i++) {
            head[i] = clusters.get(i).getHead();
            if (term.equals(head[i]))
                return i;

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

        return argMax;
    }


//################################# Getter & Setter  ########################################

    public ArrayList<Cluster<String>> getClusters() {
        return clusters;
    }

    public ArrayList<String> getTerms() {
        return terms;
    }


    //##################### MAIN-Method  ######################

    /**
     * Test unit for clustering for different datasets finetuning the parameter alpha
     *
     * @param args Not used
     */
    public static void main(String[] args) {

        // Prepare clustering test for simple default adom
        MyAffinityFunction maf = new MyAffinityFunction(0.2);

        // Test clustering
        ArrayList<Cluster<String>> clusters = maf.clusters;
        System.out.println("Clustering size: " + clusters.size());
        for (Cluster c : clusters) {
            System.out.println(c);
        }
        System.out.println("-----------------------------------------------------------------");

        // Now test for small dataset (alpha 0.12 is relatively good)
        // alpha = 0.1 --> 1 cluster, alpha = 0.2 --> 9 clusters, alpha = 0.15 --> 7 clusters,
        // alpha = 0.13 --> 6 clusters, alpha = 0.12 --> 5 clusters, alpha = 0.1111 --> 1 cluster (minimal sim)
        String separ = File.separator;
        String termsFile = "out" + separ + "csv" + separ + "terms10.txt";
        String simFile = "out" + separ + "csv" + separ + "result10.csv";
        maf = new MyAffinityFunction(0.12, termsFile, simFile);

        // Test clustering
        clusters = maf.clusters;
        System.out.println("Clustering size: " + clusters.size());
        for (Cluster c : clusters) {
            System.out.println(c);
        }
        System.out.println("-----------------------------------------------------------------");

        // bigger data set (alpha > 0.1111 and <= 0.15)
        // alpha = 0.2 --> many small clusters (often only head), alpha = 0.1 --> 1 cluster, alpha = 0.15 --> 12,
        // alpha = 0.12 --> 8 (not bad), alpha = 0.13 & 0.14 --> 9 (not bad), alpha = 0.11 & 0.1111--> 2 (bad, min sim),
        // alpha = 0.115 & 0.1125 & 0.1112 --> 8 (not bad)
        termsFile = "out" + separ + "csv" + separ + "terms30.txt";
        simFile = "out" + separ + "csv" + separ + "result30.csv";
        maf = new MyAffinityFunction(0.13, termsFile, simFile);

        // Test clustering
        clusters = maf.clusters;
        System.out.println("Clustering size: " + clusters.size()
                + ", avg. Terms/Cluster: " + maf.terms.size() / clusters.size());
        for (Cluster c : clusters) {
            System.out.println(c);
        }
        System.out.println("-----------------------------------------------------------------");


        // buiggest data set
        // alpha    | clusters  | terms/cluster         (interesting values)
        // 0.2      |    51     |       1
        // 0.15     |    34     |       2
        // 0.14     |    22     |       4
        // 0.13     |    22     |       4
        // 0.125    |    15     |       6
        // 0.115    |    15     |       6
        // 0.11     |    6      |       16
        // < 0.10 there is 3 clusters and 1 cluster (btw 0.001 steps do not produce other values than in the table)
        termsFile = "out" + separ + "csv" + separ + "terms100.txt";
        simFile = "out" + separ + "csv" + separ + "result100.csv";
        // Test clustering (with statistics)
        for (double alpha = 0.15; alpha >= 0.10; alpha=alpha-0.005) {
            maf = new MyAffinityFunction(alpha, termsFile, simFile);
            clusters = maf.clusters;
            System.out.println("alpha: " + alpha + "\t\tClustering size: " + clusters.size()
                    + "\t\tavg. Terms/Cluster: " + maf.terms.size() / clusters.size());
        }
        double alpha = 0.1111000000000001;
        maf = new MyAffinityFunction(alpha, termsFile, simFile);
        clusters = maf.clusters;
        System.out.println("alpha: " + alpha + "\t\tClustering size: " + clusters.size()
                + "\t\tavg. Terms/Cluster: " + maf.terms.size() / clusters.size());
    }


}
