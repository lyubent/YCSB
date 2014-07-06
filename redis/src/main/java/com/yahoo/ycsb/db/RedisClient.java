/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisClient extends DB {

    private Jedis jedis;

    public static final String HOST_PROPERTY = "redis.host";
    public static final String PORT_PROPERTY = "redis.port";
    public static final String PASSWORD_PROPERTY = "redis.password";

    public static final String INDEX_KEY = "_indices";

    public void init() throws DBException {
        Properties props = getProperties();
        int port;

        String portString = props.getProperty(PORT_PROPERTY);
        if (portString != null) {
            port = Integer.parseInt(portString);
        }
        else {
            port = Protocol.DEFAULT_PORT;
        }
        String host = props.getProperty(HOST_PROPERTY);

        jedis = new Jedis(host, port);
        jedis.connect();

        String password = props.getProperty(PASSWORD_PROPERTY);
        if (password != null) {
            jedis.auth(password);
        }
    }

    public void cleanup() throws DBException {
        jedis.disconnect();
    }

    /* Calculate a hash for a key to store it in an index.  The actual return
     * value of this function is not interesting -- it primarily needs to be
     * fast and scattered along the whole space of doubles.  In a real world
     * scenario one would probably use the ASCII values of the keys.
     */
    private double hash(String key) {
        return key.hashCode();
    }

    //XXX jedis.select(int index) to switch to `table`

    @Override
    public int read(String table, String key, String field,
            HashMap<String, ByteIterator> result) {
        if (field == null) {
            StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
        }
        else {
            Set<String> fields = new HashSet<String>(1);
            fields.add(field);
            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
            List<String> values = jedis.hmget(key, fieldArray);

            Iterator<String> fieldIterator = fields.iterator();
            Iterator<String> valueIterator = values.iterator();

            while (fieldIterator.hasNext() && valueIterator.hasNext()) {
                result.put(fieldIterator.next(),
			   new StringByteIterator(valueIterator.next()));
            }
            assert !fieldIterator.hasNext() && !valueIterator.hasNext();
        }
        return result.isEmpty() ? 1 : 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        if (jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
            jedis.zadd(INDEX_KEY, hash(key), key);
            return 0;
        }
        return 1;
    }

    @Override
    public int delete(String table, String key) {
        return jedis.del(key) == 0
            && jedis.zrem(INDEX_KEY, key) == 0
               ? 1 : 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            String field, Vector<HashMap<String, ByteIterator>> result) {
        Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
                                Double.POSITIVE_INFINITY, 0, recordcount);

        HashMap<String, ByteIterator> values;
        for (String key : keys) {
            values = new HashMap<String, ByteIterator>();
            read(table, key, field, values);
            result.add(values);
        }

        return 0;
    }

}
