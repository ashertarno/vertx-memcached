package net.atarno.vertx.memcached.client;

import net.spy.memcached.MemcachedClient;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Callable;

public class GetstatsCallable implements Callable<Map<SocketAddress, Map<String, String>>> {

    private MemcachedClient memClient;

    public GetstatsCallable(MemcachedClient memClient) {
        this.memClient = memClient;
    }

    @Override
    public Map<SocketAddress, Map<String, String>> call() throws Exception {
        return memClient.getStats();
    }
}
