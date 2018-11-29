import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IgniteQueryCachesTest {


    public static void main(String[] args) {

        // Discovery
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        spi.setIpFinder(ipFinder);

        // Ignite Configuration
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true)
                .setDiscoverySpi(spi)
                .setPeerClassLoadingEnabled(true);

        try (Ignite ignite = Ignition.start(cfg)) {
            // create or get cache (key-value store)
            IgniteCache<IllKey, Ill> cacheIll = ignite.getOrCreateCache("ill");
            System.out.format("Created/Got cache [%s]!\n", cacheIll.getName());


        }
    }

}
