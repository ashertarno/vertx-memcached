package net.atarno.vertx.memcached.tests;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.deploy.Verticle;

import java.util.HashMap;

public class MemClientTest extends Verticle
{
    String address;
    @Override
    public void start() throws Exception
    {
        EventBus eb = vertx.eventBus();
        address = "vertx.memcached";

        JsonObject config = new JsonObject();
        config.putString("address", "vertx.memcached");
        config.putString("address", address);
        config.putString("memcached.servers", "localhost:11211");
        config.putNumber("memcached.timeout.ms", 1000);
        config.putNumber("memcached.tasks.check.ms", 10);
        config.putNumber("memcached.connections", 2);

        container.deployVerticle("net.atarno.vertx.memcached.client.MemClient", config, 1, new Handler<String>()
        {
            @Override
            public void handle(String s)
            {
                HashMap<String, Object> cacheCall = new HashMap<String, Object>();

                cacheCall.put("shouldReply", true);
                cacheCall.put("command", "set");
                cacheCall.put("key", "CCC");
                cacheCall.put("value", 100);
                /*
                build any other command here
                 */
                act(cacheCall);

            }
        });
    }

    public void act(HashMap<String, Object> cmd)
    {
        if(cmd == null)
            return;

        JsonObject notif = new JsonObject();

        for(String key : cmd.keySet())
        {
            Object value = cmd.get(key);

            if(value != null)
            {
                if(value instanceof byte[])
                    notif.putBinary(key, (byte[]) value);
                else if(value instanceof Boolean)
                    notif.putBoolean(key, (Boolean) value);
                else if(value instanceof Number)
                    notif.putNumber(key, (Number) value);
                else if(value instanceof String)
                    notif.putString(key, (String) value);
                else if(value instanceof JsonArray)
                    notif.putArray(key, (JsonArray) value);
            }
        }
        System.out.println("sent: \n" + notif.encode());
        push(notif);
    }

    private void push(JsonObject notif)
    {
        Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                System.out.println("received: \n" +message.body.encode());
            }
        };
        vertx.eventBus().send(address, notif, replyHandler);
    }
}
