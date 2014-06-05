package net.atarno.vertx.memcached.client;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.*;
import net.spy.memcached.ops.OperationStatus;
import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@SuppressWarnings( "unchecked" )
public enum MemCommand {

    SET() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {
            String key = getKey( message );
            Object value = message.body().getField( "value" );
            int exp = message.body().getInteger( "exp" ) == null ? 0 : message.body().getInteger( "exp" );
            memClient.set( key, exp, value ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    GET() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {
            String key = getKey( message );
            memClient.asyncGet( key ).addListener( new GetCompletionListener() {

                @Override
                public void onComplete( final GetFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {
            try {
                checkTimeOut( message, future, status );
                JsonObject response = new JsonObject();
                Object value = future.get();
                response = parseForJson( response, "key", getKey( message ) );
                response = parseForJson( response, "value", value );
                sendOk( message, response );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    GETBULK() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {
            JsonArray keys = message.body().getArray( "keys" );
            if ( keys == null || keys.size() == 0 ) {
                throw new Exception( "missing mandatory non-empty field 'keys'" );
            }
            List<String> keysList = new ArrayList<>();
            for ( Object o : keys.toArray() ) {
                keysList.add( ( String ) o );
            }
            memClient.asyncGetBulk( keysList ).addListener( new BulkGetCompletionListener() {

                @Override
                public void onComplete( final BulkGetFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                JsonObject response = new JsonObject();
                JsonArray keys = new JsonArray();
                JsonArray values = new JsonArray();
                Map<String, Object> result = ( Map<String, Object> ) future.get();
                for ( String k : result.keySet() ) {
                    Object value = result.get( k );
                    keys.add(k);
                    values.add(value);
                }
                response = parseForJson( response, "keys", keys );
                response = parseForJson( response, "values", values );
                sendOk( message, response );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    STATUS() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {
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
            sendOk( message, new JsonObject().putArray( "available", aArr ).putArray( "unavailable", uArr ) );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {
            // this is a sync call
        }
    },
    GAT() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            int exp = message.body().getInteger( "exp" ) == null ? 0 : message.body().getInteger( "exp" );

            memClient.asyncGetAndTouch( key, exp ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                JsonObject response = new JsonObject();
                CASValue<Object> value = ( CASValue<Object> ) future.get();
                response = parseForJson( response, "key", getKey( message ) );
                response = parseForJson( response, "value", value == null ? null : value.getValue() );
                response = parseForJson( response, "cas", value == null ? null : value.getCas() );
                sendOk( message, response );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    APPEND() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            long cas = message.body().getLong( "cas" ) == null ? 0 : message.body().getLong( "cas" );
            Object value = message.body().getField( "value" );

            memClient.append( cas, key, value ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    PREPEND() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            long cas = message.body().getLong( "cas" ) == null ? 0 : message.body().getLong( "cas" );
            Object value = message.body().getField( "value" );

            memClient.prepend( cas, key, value ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    ADD() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            int exp = message.body().getInteger( "exp" ) == null ? 0 : message.body().getInteger( "exp" );
            Object value = message.body().getField( "value" );

            memClient.add( key, exp, value ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    REPLACE() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            int exp = message.body().getInteger( "exp" ) == null ? 0 : message.body().getInteger( "exp" );
            Object value = message.body().getField( "value" );

            memClient.replace( key, exp, value ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    TOUCH() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {
            String key = getKey( message );
            int exp = message.body().getInteger( "exp" ) == null ? 0 : message.body().getInteger( "exp" );

            memClient.touch( key, exp ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );

        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    GETSTATS() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            Map<SocketAddress, Map<String, String>> stats = memClient.getStats();
            JsonObject response = new JsonObject();
            for ( SocketAddress sa : stats.keySet() ) {
                JsonObject s = new JsonObject();
                response.putObject( "server", s );
                s.putString( "address", ( ( InetSocketAddress ) sa ).getHostString() + ":" + ( ( InetSocketAddress ) sa ).getPort() );
                Map<String, String> info = stats.get( sa );
                for ( String i : info.keySet() ) {
                    s.putString( i, info.get( i ) );
                }
            }
            sendOk( message, response );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {
            // this is a sync call
        }
    },
    INCR() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            long by = message.body().getLong( "by" ) == null ? 0 : message.body().getLong( "by" );

            memClient.asyncIncr( key, by ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    DECR() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            long by = message.body().getLong( "by" ) == null ? 0 : message.body().getLong( "by" );

            memClient.asyncDecr( key, by ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    DELETE() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            String key = getKey( message );
            memClient.delete( key ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    },
    FLUSH() {
        @Override
        public void submitQuery( MemcachedClient memClient, final Message<JsonObject> message, final Context ctx ) throws Exception {

            int delay = message.body().getInteger( "delay" ) == null ? 0 : message.body().getInteger( "delay" );
            memClient.flush( delay ).addListener( new OperationCompletionListener() {

                @Override
                public void onComplete( final OperationFuture<?> f ) throws Exception {

                    ctx.runOnContext( new Handler<Void>() {

                        @Override
                        public void handle( Void v ) {

                            reply( message, f, f.getStatus() );
                        }
                    } );
                }
            } );
        }

        @Override
        public void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) {

            try {
                checkTimeOut( message, future, status );
                sendOk( message, null );
            }
            catch ( Exception e ) {
                sendError( message, e.getMessage() );
            }
        }
    };

    public static String voidNull( String s ) {
        return s == null ? "" : s;
    }

    private static void checkTimeOut( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) throws Exception {

        if ( future == null || !future.isDone() ) {
            throw new Exception( "operation time out" );
        }
        if ( status == null || !status.isSuccess() ) {
            throw new Exception( status == null ? "system error" : status.getMessage() );
        }
    }

    private static String getKey( Message<JsonObject> message ) throws Exception {
        String key = voidNull( message.body().getString( "key" ) );
        if ( key.isEmpty() ) {
            throw new Exception( "missing mandatory non-empty field 'key'" );
        }
        return key;
    }

    private static JsonObject parseForJson( JsonObject jsonObject, String key, Object value ) throws Exception {
        if ( value != null ) {

            if ( value instanceof JsonArray ) {
                jsonObject.putArray( key, ( JsonArray ) value );
            }
            else if ( value instanceof JsonObject ) {
                jsonObject.putObject( key, ( JsonObject ) value );
            }
            else if ( value instanceof byte[] ) {
                jsonObject.putBinary( key, ( byte[] ) value );
            }
            else if ( value instanceof Boolean ) {
                jsonObject.putBoolean( key, ( Boolean ) value );
            }
            else if ( value instanceof Number ) {
                jsonObject.putNumber( key, ( Number ) value );
            }
            else if ( value instanceof String ) {
                jsonObject.putString( key, ( String ) value );
            }
            else {
                throw new Exception( "unsupported object type" );
            }
        }
        return jsonObject;
    }

    public static void sendError( Message<JsonObject> message, String errMsg ) {
        JsonObject reply = new JsonObject();
        reply.putString( "command", message.body().getString( "command" ) );
        reply.putString( "status", "error" );
        reply.putString( "message", errMsg );
        message.reply( reply );
    }

    public static void sendOk( Message<JsonObject> message, JsonObject response ) {
        JsonObject reply = new JsonObject();
        reply.putString( "command", message.body().getString( "command" ) );
        reply.putString( "status", "ok" );
        reply.putObject( "response", response );
        message.reply( reply );
    }

    public abstract void submitQuery( MemcachedClient memClient, Message<JsonObject> message, Context ctx ) throws Exception;

    public abstract void reply( Message<JsonObject> message, AbstractListenableFuture future, OperationStatus status ) throws Exception;

}
