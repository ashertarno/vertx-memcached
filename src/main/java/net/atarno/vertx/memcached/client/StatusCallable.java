package net.atarno.vertx.memcached.client;

import net.spy.memcached.MemcachedClient;
import org.vertx.java.core.json.JsonArray;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;

public class StatusCallable implements Callable<JsonArray[]> {

    private MemcachedClient memClient;

    public StatusCallable( MemcachedClient memClient ) {
        this.memClient = memClient;
    }

    @Override
    public JsonArray[] call() throws Exception {
        Collection<SocketAddress> available = memClient.getAvailableServers();
        Collection<SocketAddress> unavailable = memClient.getUnavailableServers();
        JsonArray aArr = new JsonArray();
        for ( SocketAddress sa : available ) {
            aArr.addString( ( ( InetSocketAddress ) sa ).getHostString() + ":" + ( ( InetSocketAddress ) sa ).getPort() );
        }
        JsonArray uArr = new JsonArray();
        for ( SocketAddress sa : unavailable ) {
            uArr.addString( ( ( InetSocketAddress ) sa ).getHostString() + ":" + ( ( InetSocketAddress ) sa ).getPort() );
        }
        return new JsonArray[]{ aArr, uArr };
    }
}
