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

package com.yahoo.ycsb.db;


import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * HBase client for YCSB framework
 *
 * hbase.durability = {async_wal,fsync_wal,skip_wal,sync_wal,use_default}
 */
public class HBaseClient extends com.yahoo.ycsb.DB
{
    private static final Configuration config = HBaseConfiguration.create();

    private boolean _debug=false;

    private String _table="";
    private HTable _hTable=null;
    private Durability durability;
    private String _columnFamily="";
    private byte _columnFamilyBytes[];

    private static final int Ok=0;
    private static final int ServerError=-1;
    private static final int HttpError=-2;
    private static final int NoMatchingRecord=-3;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException
    {
        _debug = Boolean.parseBoolean("debug");

        durability = Durability.valueOf(getProperties().getProperty("hbase.durability", "FSYNC_WAL").toUpperCase());

        _columnFamily = getProperties().getProperty("columnfamily");
        if (_columnFamily == null)
        {
            System.err.println("Error, must specify a columnfamily for HBase table");
            throw new DBException("No columnfamily specified");
        }
      _columnFamilyBytes = Bytes.toBytes(_columnFamily);
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
        if (_hTable == null)
            return;
        try {
            _hTable.close();
        } catch (IOException e) {
            throw new DBException(e);
        }
    }

    public void initHTable(String table) throws IOException
    {
        _hTable = new HTable(config, table);
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param field The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, String field, HashMap<String,ByteIterator> result)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try
            {
                initHTable(table);
                _table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        Result r = null;
        try
        {
        if (_debug) {
        System.out.println("Doing read from HBase columnfamily "+_columnFamily);
        System.out.println("Doing read for key: "+key);
        }
            Get g = new Get(Bytes.toBytes(key));
          if (field == null) {
            g.addFamily(_columnFamilyBytes);
          } else {
            g.addColumn(_columnFamilyBytes, Bytes.toBytes(field));
          }
            r = _hTable.get(g);
        }
        catch (IOException e)
        {
            System.err.println("Error doing get: "+e);
            return ServerError;
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...need to understand HBase concurrency model better
            return ServerError;
        }

  for (KeyValue kv : r.raw()) {
    result.put(
        Bytes.toString(kv.getQualifier()),
        new ByteArrayByteIterator(kv.getValue()));
    if (_debug) {
      System.out.println("Result for field: "+Bytes.toString(kv.getQualifier())+
          " is: "+Bytes.toString(kv.getValue()));
    }

  }
    return Ok;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param field The field to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, String field, Vector<HashMap<String,ByteIterator>> result)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try
            {
                initHTable(table);
                _table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        Scan s = new Scan(Bytes.toBytes(startkey));
        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
        //We get back recordcount records
        s.setCaching(recordcount);

        //add specified fields or else all fields
        if (field == null)
        {
            s.addFamily(_columnFamilyBytes);
        }
        else
        {
            s.addColumn(_columnFamilyBytes,Bytes.toBytes(field));
        }

        //get results
        ResultScanner scanner = null;
        try {
            scanner = _hTable.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next())
            {
                //get row key
                String key = Bytes.toString(rr.getRow());
                if (_debug)
                {
                    System.out.println("Got scan result for key: "+key);
                }

                HashMap<String,ByteIterator> rowResult = new HashMap<String, ByteIterator>();

                for (KeyValue kv : rr.raw()) {
                  rowResult.put(
                      Bytes.toString(kv.getQualifier()),
                      new ByteArrayByteIterator(kv.getValue()));
                }
                //add rowResult to result vector
                result.add(rowResult);
                numResults++;
                if (numResults >= recordcount) //if hit recordcount, bail out
                {
                    break;
                }
            } //done with row

        }

        catch (IOException e) {
            if (_debug)
            {
                System.out.println("Error in getting/parsing scan result: "+e);
            }
            return ServerError;
        }

        finally {
            scanner.close();
        }

        return Ok;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String,ByteIterator> values)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try
            {
                initHTable(table);
                _table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }


        if (_debug) {
            System.out.println("Setting up put for key: "+key);
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, ByteIterator> entry : values.entrySet())
        {
            if (_debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/"+
                  entry.getValue() + " to put request");
            }
            p.add(_columnFamilyBytes,Bytes.toBytes(entry.getKey()),entry.getValue().toArray());
        }

        try
        {
            _hTable.put(p);
        }
        catch (IOException e)
        {
            if (_debug) {
                System.err.println("Error doing put: "+e);
            }
            return ServerError;
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...hope this is rare
            return ServerError;
        }

        return Ok;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String,ByteIterator> values)
    {
        return update(table,key,values);
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try
            {
                initHTable(table);
                _table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ServerError;
            }
        }

        if (_debug) {
            System.out.println("Doing delete for key: "+key);
        }

        Delete d = new Delete(Bytes.toBytes(key));
        try
        {
            _hTable.delete(d);
        }
        catch (IOException e)
        {
            if (_debug) {
                System.err.println("Error doing delete: "+e);
            }
            return ServerError;
        }

        return Ok;
    }

    public static void main(String[] args)
    {
        if (args.length!=3)
        {
            System.out.println("Please specify a threadcount, columnfamily and operation count");
            System.exit(0);
        }

        final int keyspace=10000; //120000000;

        final int threadcount=Integer.parseInt(args[0]);

        final String columnfamily=args[1];


        final int opcount=Integer.parseInt(args[2])/threadcount;

        Vector<Thread> allthreads=new Vector<Thread>();

        for (int i=0; i<threadcount; i++)
        {
            Thread t=new Thread()
            {
                public void run()
                {
                    try
                    {
                        Random random=new Random();

                        HBaseClient cli=new HBaseClient();

                        Properties props=new Properties();
                        props.setProperty("columnfamily",columnfamily);
                        props.setProperty("debug","true");
                        cli.setProperties(props);

                        cli.init();

                        long accum=0;

                        for (int i=0; i<opcount; i++)
                        {
                            int keynum=random.nextInt(keyspace);
                            String key="user"+keynum;
                            long st=System.currentTimeMillis();
                            int rescode;
                            HashSet<String> scanFields = new HashSet<String>();
                            scanFields.add("field1");
                            scanFields.add("field3");
                            Vector<HashMap<String,ByteIterator>> scanResults = new Vector<HashMap<String,ByteIterator>>();
                            rescode = cli.scan("table1","user2",20,null,scanResults);

                            long en=System.currentTimeMillis();

                            accum+=(en-st);

                            if (rescode!=Ok)
                            {
                                System.out.println("Error "+rescode+" for "+key);
                            }

                            if (i%1==0)
                            {
                                System.out.println(i+" operations, average latency: "+(((double)accum)/((double)i)));
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            };
            allthreads.add(t);
        }

        long st=System.currentTimeMillis();
        for (Thread t: allthreads)
        {
            t.start();
        }

        for (Thread t: allthreads)
        {
            try
            {
                t.join();
            }
            catch (InterruptedException e)
            {
            }
        }
        long en=System.currentTimeMillis();

        System.out.println("Throughput: "+((1000.0)*(((double)(opcount*threadcount))/((double)(en-st))))+" ops/sec");

    }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
*/

