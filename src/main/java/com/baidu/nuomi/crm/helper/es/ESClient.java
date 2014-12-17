package com.baidu.nuomi.crm.helper.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: mazhen01
 * Date: 2014/12/12
 * Time: 17:18
 */
public class ESClient {

    private static Client client;
    private static Lock lock = new ReentrantLock();
    private static final String key = "LOCK";

    public static Client getClient() {

        if (client != null) {
            return client;
        }

        lock.lock();
        try {
            if (client != null) {
                return client;
            }

            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", "elasticsearch")
                    .put("client.transport.sniff", true)
                    .build();

            client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
            return client;
        } finally {
            lock.unlock();
        }
    }

}
