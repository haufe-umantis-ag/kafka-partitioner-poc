package com.umantis.poc.config;

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author David Espinosa.
 */
@Configuration
public class ZookeeperConfig {

    @Value("${zookeeper.server}")
    private String zookeeper;

    @Value("${zookeeper.session_timeout}")
    private int sessionTimeOut;

    @Value("${zookeeper.connection_timeout}")
    private int connectionTimeout;

    @Bean
    public ZkConnection getZookeeperConnection() {
        ZkConnection zkConnection = new ZkConnection(zookeeper, sessionTimeOut);
        return zkConnection;
    }

    @Bean
    public ZkClient getZookeeperClient() {
        ZkClient zkClient = new ZkClient(getZookeeperConnection(), connectionTimeout, ZKStringSerializer$.MODULE$);
        return zkClient;
    }
}
