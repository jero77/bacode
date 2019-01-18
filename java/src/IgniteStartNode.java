import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.io.File;
import java.util.Arrays;

public class IgniteStartNode {

    public static void main(String[] args) {

        // Discovery
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("192.168.1.1:47500..47501", "192.168.1.2:47500..47501"));
        spi.setIpFinder(ipFinder);

        // Configuration
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(spi)
                .setClientMode(false);
//                .setPeerClassLoadingEnabled(true);

        // Start the node
        Ignition.start(cfg);

//        String separ = File.separator;
//        Ignition.start(System.getenv("IGNITE_HOME") + separ + "examples" + separ + "config" + separ
//                + "myconfig.xml");
    }
}
