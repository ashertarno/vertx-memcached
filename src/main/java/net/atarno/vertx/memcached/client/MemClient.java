package net.atarno.vertx.memcached.client;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.io.IOException;

/**
 * spymemcached client for vert.x<p>
 * Please see the manual for a full description<p>
 *
 * @author <a href="mailto:atarno@gmail.com">Asher Tarnopolski</a>
 *         <p/>
 *         <p/>
 */
public class MemClient extends Verticle {
    private String address;
    private String memServers;
    private long timeOutMillis;

    private EventBus eb;
    private Logger logger;
    private MemcachedClient[] memClients;

    @Override
    public void start() {
        eb = vertx.eventBus();
        logger = container.logger();
        address = container.config().getString( "address", "vertx.memcached" );
        memServers = container.config().getString( "memcached.servers" );
        timeOutMillis = container.config().getLong( "memcached.timeout.ms", BinaryConnectionFactory.DEFAULT_OPERATION_TIMEOUT ).longValue();
        int connections = container.config().getNumber( "memcached.connections", 1 ).intValue();
        // init connection pool
        try {
            initMemClients( connections );
        }
        catch ( IOException e ) {
            logger.error( e );
            return;
        }

        // register verticle
        eb.registerHandler( address, memHandler, new AsyncResultHandler<Void>() {
            @Override
            public void handle( AsyncResult<Void> voidAsyncResult ) {
                logger.info( this.getClass().getSimpleName() + " verticle is started" );
            }
        } );
    }

    Handler<Message<JsonObject>> memHandler = new Handler<Message<JsonObject>>() {
        public void handle( Message<JsonObject> message ) {
            String command = MemCommand.voidNull( message.body().getString( "command" ) );

            if ( command.isEmpty() ) {
                MemCommand.sendError( message, "\"command\" property is mandatory for request" );
                return;
            }
            try {
                MemCommand mc = getByName( command );
                mc.submitQuery( getMemClient(), message, vertx.currentContext() );
            }
            catch ( IllegalArgumentException e ) {
                MemCommand.sendError( message, "unknown command: '" + command + "'" );
            }
            catch ( Exception e ) {
                MemCommand.sendError( message, e.getMessage() );
            }
        }
    };


    private void initMemClients( int connections ) throws IOException {
        memClients = new MemcachedClient[ connections < 1 ? 1 : connections ];
        for ( int i = 0; i < memClients.length; i++ ) {
            BinaryConnectionFactoryTO bf = new BinaryConnectionFactoryTO( timeOutMillis );
            memClients[ i ] = new MemcachedClient( bf, AddrUtil.getAddresses( memServers ) );
        }
        logger.info( "pool of " + memClients.length + " memcached clients was successfully initialized" );
    }

    private MemcachedClient getMemClient() {
        if ( memClients.length == 1 ) {
            return memClients[ 0 ];
        }
        return memClients[ ( int ) ( Math.random() * memClients.length ) ];
    }

    private MemCommand getByName( String name ) {

        return MemCommand.valueOf( MemCommand.voidNull( name.toUpperCase() ) );
    }

    public void stop() {

        for ( int i = 0; i < memClients.length; i++ ) {
            memClients[ i ].shutdown();
        }
        logger.info( "== Memcached clients were closed successfully" );
    }
}
