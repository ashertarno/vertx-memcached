package net.atarno.vertx.memcached.tests;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.framework.TestClientBase;

import java.util.HashMap;

/**
 * Date: 12/13/12
 * Time: 6:21 PM
 */
public class MemClientTest  extends TestClientBase
{
   String address;
   @Override
   public void start()
   {
      super.start();
      JsonObject config = new JsonObject();
      address = "vertx.memcached";
      config.putString("address", address); 
      config.putString("memcached_servers", "localhost:11211");
      //config.putNumber("operation_timeout", 10000L);
      container.deployWorkerVerticle("net.atarno.vertx.memcached.MemClient", config, 1, new Handler<String>()
      {
         public void handle(String res)
         {
            tu.appReady();

            HashMap<String, Object> cacheCall = new HashMap<String, Object>();
            cacheCall.put("command", "set");
            cacheCall.put("key", "AAA");
            cacheCall.put("value", 1234);
            act(cacheCall);

            cacheCall = new HashMap<String, Object>();
            cacheCall.put("command", "get");
            cacheCall.put("key", "AAA");
            act(cacheCall);
         }
      });
   }

   @Override
   public void stop()
   {
      super.stop();
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

      push(notif);
   }

   private void push(JsonObject notif)
   {
      Handler<Message<JsonObject>> replyHandler = new Handler<Message<JsonObject>>()
      {
         public void handle(Message<JsonObject> message)
         {
            tu.checkContext();
            tu.trace(message.body.encode());
            System.out.println(message.body.encode());
            tu.testComplete();
         }
      };
      vertx.eventBus().send(address, notif, replyHandler);
   }
}
