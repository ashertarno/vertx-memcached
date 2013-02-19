package net.atarno.vertx.memcached.client;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;

@SuppressWarnings("unchecked")
public enum MemCommand {
    SET() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Object value = message.body.getField("value");
            int exp = message.body.getInteger("exp") == null ? 0 : message.body.getInteger("exp");
            OperationFuture<Boolean> future = memClient.set(key, exp, value);
            return future;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            response.putBoolean("success", future != null);
            if (future == null) {
                response.putString("reason", "operation timed out");
            }
            return response;
        }
    },
    GET() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            GetFuture<Object> f = memClient.asyncGet(key);
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            Object value = future.get();
            data = parseForJson(data, "key", value);
            response.putObject("data", data);
            response.putBoolean("success", true);
            return response;
        }
    },
    GETBULK() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            JsonArray keys = message.body.getArray("keys");
            if (keys == null || keys.size() == 0) {
                throw new Exception("missing mandatory non-empty field 'keys'");
            }
            List<String> keysList = new ArrayList<>();
            for (Object o : keys.toArray()) {
                keysList.add((String) o);
            }
            BulkFuture<Map<String, Object>> bulkFuture = memClient.asyncGetBulk(keysList);
            return bulkFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            Map<String, Object> result = (Map<String, Object>) future.get();
            for (String k : result.keySet()) {
                Object value = result.get(k);
                data = parseForJson(data, k, value);
            }
            response.putBoolean("success", true);

            return response;
        }
    },
    STATUS() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            Future<JsonArray[]> f = syncExecutor.submit(new StatusCallable(memClient));
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            checkTimeout(future);
            JsonArray[] status = (JsonArray[]) future.get();
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            response.putBoolean("success", true);
            data.putArray("available", status[0]);
            data.putArray("unavailable", status[1]);
            return response;
        }
    },
    GAT() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Integer exp = message.body.getInteger("exp");
            if (exp == null) {
                throw new Exception("missing mandatory non-empty field 'exp'");
            }
            OperationFuture<CASValue<Object>> operationFuture = memClient.asyncGetAndTouch(key, exp);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            CASValue<Object> value = (CASValue<Object>) future.get();
            if (value != null) {
                data = parseForJson(data, "key", value.getValue());
                Long c = value.getCas();
                if (c != null) {
                    data.putNumber("cas", value.getCas());
                }
                response.putBoolean("success", true);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },
    APPEND() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Long cas = message.body.getLong("cas");
            if (cas == null) {
                throw new Exception("missing mandatory non-empty field 'cas'");
            }
            Object value = message.body.getField("value");
            OperationFuture<Boolean> operationFuture = memClient.append(cas, key, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },
    PREPEND() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Long cas = message.body.getLong("cas");
            if (cas == null) {
                throw new Exception("missing mandatory non-empty field 'cas'");
            }
            Object value = message.body.getField("value");
            OperationFuture<Boolean> operationFuture = memClient.prepend(cas, key, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },
    ADD() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            int exp = message.body.getInteger("exp") == null ? 0 : message.body.getInteger("exp");
            Object value = message.body.getField("value");
            OperationFuture<Boolean> operationFuture = memClient.add(key, exp, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }

            return response;
        }
    },
    REPLACE() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            int exp = message.body.getInteger("exp") == null ? 0 : message.body.getInteger("exp");
            Object value = message.body.getField("value");
            OperationFuture<Boolean> operationFuture = memClient.replace(key, exp, value);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean) future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    TOUCH() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Integer exp = message.body.getInteger("exp");
            if (exp == null) {
                throw new Exception("missing mandatory non-empty field 'exp'");
            }
            OperationFuture<Boolean> operationFuture = memClient.touch(key, exp.intValue());
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean) future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    GETSTATS() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            Future<Map<SocketAddress, Map<String, String>>> f = syncExecutor.submit(new GetstatsCallable(memClient));
            return f;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            checkTimeout(future);
            Map<SocketAddress, Map<String, String>> stats = (Map<SocketAddress, Map<String, String>>) future.get();
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            for (SocketAddress sa : stats.keySet()) {
                JsonObject s = new JsonObject();
                data.putObject("server", s);
                s.putString("address", ((InetSocketAddress) sa).getHostString() + ":" + ((InetSocketAddress) sa).getPort());
                Map<String, String> info = stats.get(sa);
                for (String i : info.keySet()) {
                    s.putString(i, info.get(i));
                }
            }
            response.putObject("data", data);
            response.putBoolean("success", true);
            return response;
        }
    },
    INCR() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Long by = message.body.getLong("by");
            if (by == null) {
                throw new Exception("missing mandatory non-empty field 'by'");
            }
            OperationFuture<Long> operationFuture = memClient.asyncIncr(key, by);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            Long incr_val = (Long) future.get();
            if (incr_val != null) {
                response.putBoolean("success", true);
                data.putNumber("value", incr_val);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    DECR() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            Long by = message.body.getLong("by");
            if (by == null) {
                throw new Exception("missing mandatory non-empty field 'by'");
            }
            OperationFuture<Long> operationFuture = memClient.asyncDecr(key, by);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            Long decr_val = (Long) future.get();
            if (decr_val != null) {
                response.putBoolean("success", true);
                data.putNumber("value", decr_val);
            } else {
                response.putBoolean("success", false);
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    DELETE() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            String key = getKey(message);
            OperationFuture<Boolean> operationFuture = memClient.delete(key);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean) future.get();
            response.putBoolean("success", success);
            if (!success) {
                response.putString("reason", "failed to fetch key '" + getKey(message) + "'");
            }
            return response;
        }
    },
    FLUSH() {
        @Override
        public Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception {
            int delay = message.body.getInteger("delay") == null ? 0 : message.body.getInteger("delay");
            OperationFuture<Boolean> operationFuture = memClient.flush(delay);
            return operationFuture;
        }

        @Override
        public JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception {
            if(!shouldReply) {
                return null;
            }
            checkTimeout(future);
            JsonObject response = new JsonObject();
            JsonObject data = new JsonObject();
            response.putObject("data", data);
            boolean success = (Boolean)future.get();
            response.putBoolean("success", success);
            return response;
        }
    };

    public static String voidNull(String s) {
        return s == null ? "" : s;
    }
    
    private static void checkTimeout(Future f) throws TimeoutException {
        if(f == null) {
            throw new TimeoutException();
        }
    }
    
    private static String getKey(Message<JsonObject> message) throws Exception {
        String key = voidNull(message.body.getString("key"));
        if (key.isEmpty()) {
            throw new Exception("missing mandatory non-empty field 'key'");
        }
        return key;
    }

    private static JsonObject parseForJson(JsonObject jsonObject, String key, Object value) throws Exception {
        if (value != null) {
            // not serializable in current version of vert.x
            /*
            * if(value instanceof JsonArray) jsonObject.putArray("value", (JsonArray) value); else if(value instanceof JsonObject) jsonObject.putObject("value", (JsonObject) value); else
            */

            if (value instanceof byte[]) {
                jsonObject.putBinary(key, (byte[]) value);
            } else if (value instanceof Boolean) {
                jsonObject.putBoolean(key, (Boolean) value);
            } else if (value instanceof Number) {
                jsonObject.putNumber(key, (Number) value);
            } else if (value instanceof String) {
                jsonObject.putString(key, (String) value);
            } else {
                throw new Exception("unsupported object type");
            }
        }
        return jsonObject;
    }

    private static ExecutorService syncExecutor = Executors.newFixedThreadPool(2);

    //no default implementation
    public abstract Future query(MemcachedClient memClient, Message<JsonObject> message) throws Exception;

    public abstract JsonObject buildResponse(Message<JsonObject> message, Future future, boolean shouldReply) throws Exception;
}
