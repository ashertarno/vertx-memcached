package net.atarno.vertx.memcached.tests;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.ReplyException;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.HashMap;

public class MemClientTest extends Verticle {
    String address;

    @Override
    public void start() {
        EventBus eb = vertx.eventBus();
        address = "vertx.memcached";

        JsonObject config = new JsonObject();
        config.putString( "address", address );
        config.putString( "memcached.servers", "localhost:11211" );
        config.putNumber( "memcached.timeout.ms", 1000 );
        config.putNumber( "memcached.connections", 2 );
        config.putBoolean( "validate-on-connect", true );

        container.deployVerticle( "net.atarno.vertx.memcached.client.MemClient", config, 1, new Handler<AsyncResult<String>>() {
            @Override
            public void handle( AsyncResult<String> stringAsyncResult ) {
                HashMap<String, Object> cacheCall = new HashMap<String, Object>();

                cacheCall.put( "command", "getbulk" );
                cacheCall.put( "keys", new JsonArray().addString( "AAA" ).addString( "BBB" ).addString( "CCC" ) );
                /*
                build any other command here
                 */
                act( cacheCall );

            }
        } );
    }

    public void act( HashMap<String, Object> cmd ) {
        if ( cmd == null ) {
            return;
        }

        JsonObject notif = new JsonObject();

        for ( String key : cmd.keySet() ) {
            Object value = cmd.get( key );

            if ( value != null ) {
                if ( value instanceof byte[] ) {
                    notif.putBinary( key, ( byte[] ) value );
                }
                else if ( value instanceof Boolean ) {
                    notif.putBoolean( key, ( Boolean ) value );
                }
                else if ( value instanceof Number ) {
                    notif.putNumber( key, ( Number ) value );
                }
                else if ( value instanceof String ) {
                    notif.putString( key, ( String ) value );
                }
                else if ( value instanceof JsonArray ) {
                    notif.putArray( key, ( JsonArray ) value );
                }
            }
        }
        System.out.println( "sent: \n" + notif.encodePrettily() );
        push( notif );
    }

    private void push( JsonObject notif ) {
        Handler<AsyncResult<Message<JsonObject>>> replyHandler = new Handler<AsyncResult<Message<JsonObject>>>() {
            public void handle( AsyncResult<Message<JsonObject>> result ) {
                if ( result.succeeded() ) {
                    System.out.println( "received: \n" + result.result().body().encodePrettily() );
                }
                else {
                    System.out.println( "time out " + ( ( ReplyException ) result.cause() ).failureType().name() );
                }
            }
        };
        vertx.eventBus().sendWithTimeout( address, notif, 1500L, replyHandler );
    }
}
