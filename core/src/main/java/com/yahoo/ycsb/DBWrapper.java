/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB {
    DB _db;
    Measurements _measurements;

    private static final String READ_RETRY_PROPERTY = "readretrycount";
    private static final String UPDATE_RETRY_PROPERTY = "updateretrycount";
    private static final String INSERT_RETRY_PROPERTY = "insertretrycount";
    private static final String RETRY_DELAY = "retrydelay";

    private int readRetryCount;
    private int updateRetryCount;
    private int insertRetryCount;
    private int retryDelay;

    interface DBOperation {
        String name();
        int maxRetries();
        void go();
    }

    public DBWrapper(DB db, Properties p) {
        _db = db;
        _measurements = Measurements.getMeasurements();

        readRetryCount = Integer.parseInt(p.getProperty(READ_RETRY_PROPERTY, "0"));
        updateRetryCount = Integer.parseInt(p.getProperty(UPDATE_RETRY_PROPERTY, "0"));
        insertRetryCount = Integer.parseInt(p.getProperty(INSERT_RETRY_PROPERTY, "0"));
        retryDelay = Integer.parseInt(p.getProperty(RETRY_DELAY, "0"));
    }

    /**
     * Set the properties for this DB.
     */
    public void setProperties(Properties p) {
        _db.setProperties(p);
    }

    /**
     * Get the set of properties for this DB.
     */
    public Properties getProperties() {
        return _db.getProperties();
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
        _db.init();
    }

    public void reinit() throws DBException, InstantiationException, IllegalAccessException {
        Properties p = _db.getProperties();
        _db.cleanup();
        _db = _db.getClass().newInstance();
        _db.setProperties(p);
        _db.init();
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
        long st=System.nanoTime();
        _db.cleanup();
        long en=System.nanoTime();
        _measurements.measure("CLEANUP", (int)((en-st)/1000));
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public void readAll(final String table, final String key, final Map<String, ByteIterator> result) {
        operation(new DBOperation() {
            @Override
            public String name() {
                return "READ";
            }

            @Override
            public int maxRetries() {
                return readRetryCount;
            }

            @Override
            public void go() {
                _db.readAll(table, key, result);
            }
        });
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param field The field to read
     * @param result A Map of field/value pairs for the result
     */
    @Override
    public void readOne(final String table, final String key, final String field, final Map<String, ByteIterator> result) {
        operation(new DBOperation() {
            @Override
            public String name() {
                return "READ";
            }

            @Override
            public int maxRetries() {
                return readRetryCount;
            }

            @Override
            public void go() {
                _db.readOne(table, key, field, result);
            }
        });
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param result A List of Maps, where each Map is a set field/value pairs for one record
     */
    public void scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result) {
        long st = System.nanoTime();
        _db.scanAll(table, startkey, recordcount, result);
        long en = System.nanoTime();
        _measurements.measure("SCAN", (int) ((en - st) / 1000));
        _measurements.reportReturnCode("SCAN");
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a Map.
     *
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param field       The field to read
     * @param result      A List of Maps, where each Map is a set field/value pairs for one record
     */
    public void scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result) {
        long st = System.nanoTime();
        _db.scanOne(table, startkey, recordcount, field, result);
        long en = System.nanoTime();
        _measurements.measure("SCAN", (int) ((en - st) / 1000));
        _measurements.reportReturnCode("SCAN");
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param value  The value to update in the key record
     */
    public void updateOne(final String table, final String key, final String field, final ByteIterator value) {
        operation(new DBOperation() {
            @Override
            public String name() {
                return "UPDATE";
            }

            @Override
            public int maxRetries() {
                return updateRetryCount;
            }

            @Override
            public void go() {
                _db.updateOne(table, key, field, value);
            }
        });
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A Map of field/value pairs to update in the record
     */
    public void updateAll(final String table, final String key, final Map<String,ByteIterator> values) {
        operation(new DBOperation() {
            @Override
            public String name() {
                return "UPDATE";
            }

            @Override
            public int maxRetries() {
                return updateRetryCount;
            }

            @Override
            public void go() {
                _db.updateAll(table, key, values);
            }
        });
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key.
     *
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A Map of field/value pairs to insert in the record
     */
    public void insert(final String table, final String key, final Map<String, ByteIterator> values) {
        operation(new DBOperation() {
            @Override
            public String name() {
                return "INSERT";
            }

            @Override
            public int maxRetries() {
                return insertRetryCount;
            }

            @Override
            public void go() {
                _db.insert(table, key, values);
            }
        });
    }

    private void operation(DBOperation op) {
        long st = System.nanoTime();
        int retryCount = 0;
        boolean success = false;

        while (!success) {
            if (retryCount < op.maxRetries())
                break;

            try {
                if(retryDelay > 0)
                    delay(retryDelay);
                op.go();
                success = true;
            } catch (Throwable t) {
                t.printStackTrace();
                if (op.maxRetries() > 0 && retryCount < op.maxRetries())
                    System.out.println("Retrying for " + retryCount + "/" + op.maxRetries() + " attempt.");
                retryCount++;
            }
        }
        long en = System.nanoTime();
        _measurements.measure(op.name(), (int) ((en - st) / 1000));
        _measurements.reportRetryCount(op.name(), retryCount);
        _measurements.reportReturnCode(op.name());
    }

    private void delay(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            //do nothing
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     */
    public void delete(String table, String key) {
        long st = System.nanoTime();
        _db.delete(table, key);
        long en = System.nanoTime();
        _measurements.measure("DELETE", (int) ((en - st) / 1000));
        _measurements.reportReturnCode("DELETE");
    }
}