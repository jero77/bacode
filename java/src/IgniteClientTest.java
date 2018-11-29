import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

public class IgniteClientTest {


    public static void main(String[] args) {

        // Client configuration
        final String ADDRESSES = "127.0.0.1:47500..47509";
        ClientConfiguration cliConfig = new ClientConfiguration().setAddresses(ADDRESSES);

        // Cache configurations
        String[] terms = {"Asthma", "Cough", "Influenza", "Ulna Fracture", "Tibial Fracture"};      // active domain
        CacheConfiguration cacheConfigIll = new CacheConfiguration();
        cacheConfigIll.setName("ill")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setTypes(IllKey.class, Ill.class)
                .setAffinity(new MyAffinityFunction<String>(2, 0.2, terms)); // TODO Test affinity function

        CacheConfiguration cacheConfigInfo = new CacheConfiguration();
        cacheConfigInfo.setName("info")
                .setBackups(0)
                .setCacheMode(CacheMode.PARTITIONED)
                .setTypes(Integer.class, Info.class);

        // Discovery
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        spi.setIpFinder(ipFinder);

        // Ignite Configuration
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true)
            .setDiscoverySpi(spi)
            .setCacheConfiguration(cacheConfigIll, cacheConfigInfo);


        // Start client that connects to the cluster
        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println("Client started!");

            // create or get cache (key-value store)
            IgniteCache<IllKey, Ill> cacheIll = ignite.getOrCreateCache(cacheConfigIll);
            System.out.format("Created/Got cache [%s]!\n", cacheConfigIll.getName());

            IgniteCache<Integer, Info> cacheInfo = ignite.getOrCreateCache(cacheConfigInfo);
            System.out.format("Created/Got cache [%s]!\n", cacheConfigInfo.getName());

            // put some test data
            // TODO generate random data
            for (int i = 0; i < 5; i++) {
                // Some info
                Info info = new Info(i);
                cacheInfo.put(info.getId(), info);

                // Some disease
                String disease = terms[(new Random()).nextInt(terms.length)];
                IllKey illKey = new IllKey(i, disease);
                Ill ill = new Ill(illKey, "diseaseID"+(112*i));
                cacheIll.put(illKey, ill);
            }


            // Get affinities for both caches
//            Affinity<IllKey> affIll = ignite.affinity("ill");
//            Affinity<Integer> affInfo = ignite.affinity("info");

            // Get the affinity key mappings:
//            IllKey key = cacheIll.;
//            ClusterNode node = affIll.mapKeyToNode(key);
////            System.out.println("Addresses: " + Arrays.toString(node.addresses().toArray()));
//            System.out.println("NodeID: " + node.id() + ", Addresses: " + Arrays.toString(node.addresses().toArray()));
//            System.out.println("Partition ID: " + affIll.partition(key));
//            System.out.println("Number of partitions: " + affIll.partitions());

        } catch (ClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
