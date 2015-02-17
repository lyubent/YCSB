package com.yahoo.ycsb.memcached;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class MemcachedCompatibleClient extends DB {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final String QUALIFIED_KEY = "{0}-{1}";

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected MemcachedClient client;

    protected boolean checkOperationStatus;
    protected long shutdownTimeoutMillis;
    protected int objectExpirationTime;

    private static final String TEMPORARY_FAILURE_MESSAGE = "Temporary failure";
    private static final String CANCELLED_MESSAGE = "cancelled";

    public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY = "couchbase.shutdownTimeoutMillis";
    public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";
    public static final String OBJECT_EXPIRATION_TIME_PROPERTY = "couchbase.objectExpirationTime";
    public static final String DEFAULT_OBJECT_EXPIRATION_TIME = String.valueOf(Integer.MAX_VALUE);
    public static final String CHECK_OPERATION_STATUS_PROPERTY = "couchbase.checkOperationStatus";
    public static final String CHECK_OPERATION_STATUS_DEFAULT = "true";

    @Override
    public void init() throws DBException {
        try {
            client = createMemcachedClient();
            checkOperationStatus = Boolean.parseBoolean(getProperties().getProperty(CHECK_OPERATION_STATUS_PROPERTY, CHECK_OPERATION_STATUS_DEFAULT));
            objectExpirationTime = Integer.parseInt(getProperties().getProperty(OBJECT_EXPIRATION_TIME_PROPERTY, DEFAULT_OBJECT_EXPIRATION_TIME));
            shutdownTimeoutMillis = Integer.parseInt(getProperties().getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    protected abstract MemcachedClient createMemcachedClient() throws Exception;

    @Override
    public void readOne(String table, String key, String field, Map<String,ByteIterator> result) {

        read(table, key, Collections.singleton(field), result);
    }

    @Override
    public void readAll(String table, String key, Map<String,ByteIterator> result) {
        read(table, key, null, result);
    }

    public void read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            GetFuture<Object> future = client.asyncGet(createQualifiedKey(table, key));
            Object document = future.get();
            if (document != null) {
                fromJson((String) document, fields, result);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error encountered", e);
        }
    }

    @Override
    public void scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result) {
        throw new IllegalStateException("Range scan is not supported");
    }

    @Override
    public void scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result) {
        throw new IllegalStateException("Range scan is not supported");
    }

    @Override
    public void updateOne(String table, String key, String field, ByteIterator value) {

        update(table, key, Collections.singletonMap(field, value));
    }

    @Override
    public void updateAll(String table, String key, Map<String,ByteIterator> values) {

        update(table, key, values);
    }

    public void update(String table, String key, Map<String,ByteIterator> values) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.replace(key, objectExpirationTime, toJson(values));
            processFuture(future);
        } catch (Exception e) {
            throw new RuntimeException("Error updating value with key: " + key, e);
        }
    }

    @Override
    public void insert(String table, String key, Map<String, ByteIterator> values) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.add(key, objectExpirationTime, toJson(values));
            processFuture(future);
        } catch (Exception e) {
            throw new RuntimeException("Error inserting value", e);
        }
    }

    @Override
    public void delete(String table, String key) {
        key = createQualifiedKey(table, key);
        try {
            OperationFuture<Boolean> future = client.delete(key);
            processFuture(future);
        } catch (Exception e) {
            throw new RuntimeException("Error deleting value", e);
        }
    }

    public void query(String table, String key, int limit) {
        throw new UnsupportedOperationException("Query not implemented");
    };

    protected void processFuture(OperationFuture<Boolean> future) {
        if (checkOperationStatus) {
            if(future.getStatus().isSuccess()) {
                if (log.isInfoEnabled())
                    log.info("Future operation successful.");
            }
            if(TEMPORARY_FAILURE_MESSAGE.equals(future.getStatus().getMessage()) || CANCELLED_MESSAGE.equals(future.getStatus().getMessage())) {
                if(log.isWarnEnabled())
                    log.warn("Future operation requires retry.");
                return;
            }
            if(log.isErrorEnabled())
                log.error("Future operation failed.");
        } else {
            log.info("Future operation successful.");
        }
    }

    @Override
    public void cleanup() throws DBException {
        if (client != null) {
            client.shutdown(shutdownTimeoutMillis, MILLISECONDS);
        }
    }

    protected static String createQualifiedKey(String table, String key) {
        return MessageFormat.format(QUALIFIED_KEY, table, key);
    }

    protected static void fromJson(String value, Set<String> fields, Map<String, ByteIterator> result) throws IOException {
        JsonNode json = MAPPER.readTree(value);
        boolean checkFields = fields != null && fields.size() > 0;
        for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields(); jsonFields.hasNext(); ) {
            Map.Entry<String, JsonNode> jsonField = jsonFields.next();
            String name = jsonField.getKey();
            if (checkFields && fields.contains(name)) {
                continue;
            }
            JsonNode jsonValue = jsonField.getValue();
            if (jsonValue != null && !jsonValue.isNull()) {
                result.put(name, new StringByteIterator(jsonValue.asText()));
            }
        }
    }

    protected static String toJson(Map<String, ByteIterator> values) throws IOException {
        ObjectNode node = MAPPER.createObjectNode();
        HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
        for (Map.Entry<String, String> pair : stringMap.entrySet()) {
            node.put(pair.getKey(), pair.getValue());
        }
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
        MAPPER.writeTree(jsonGenerator, node);
        return writer.toString();
    }
}
