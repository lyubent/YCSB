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

import java.util.*;


/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class BasicDB extends DB
{
	public static final String VERBOSE="basicdb.verbose";
	public static final String VERBOSE_DEFAULT="true";
	
	public static final String SIMULATE_DELAY="basicdb.simulatedelay";
	public static final String SIMULATE_DELAY_DEFAULT="0";
	
	
	boolean verbose;
	int todelay;

	public BasicDB()
	{
		todelay=0;
	}

	
	void delay()
	{
		if (todelay>0)
		{
			try
			{
				Thread.sleep((long)Utils.random().nextInt(todelay));
			}
			catch (InterruptedException e)
			{
				//do nothing
			}
		}
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	@SuppressWarnings("unchecked")
	public void init()
	{
		verbose=Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
		todelay=Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
		
		if (verbose)
		{
			System.out.println("***************** properties *****************");
			Properties p=getProperties();
			if (p!=null)
			{
				for (Enumeration e=p.propertyNames(); e.hasMoreElements(); )
				{
					String k=(String)e.nextElement();
					System.out.println("\""+k+"\"=\""+p.getProperty(k)+"\"");
				}
			}
			System.out.println("**********************************************");
		}
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a Map.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param result A Map of field/value pairs for the result
	 */
	public void readAll(String table, String key, Map<String,ByteIterator> result) {
        delay();

        if (verbose)
        {
            System.out.print("READ "+table+" "+key+" [ ");
            System.out.print("<all fields>");
            System.out.println("]");
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param field The field to read
     * @param result A Map of field/value pairs for the result
     */
    public void readOne(String table, String key, String field, Map<String,ByteIterator> result)
    {
        delay();

        if (verbose)
        {
            System.out.print(field+" ");
            System.out.println("]");
		}
    }

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param result A List of Maps, where each Map is a set field/value pairs for one record
	 */
    public void scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result)
    {
        delay();

        if (verbose)
        {
            System.out.print("SCAN "+table+" "+startkey+" "+recordcount+" [ ");
            System.out.print("<all fields>");
            System.out.println("]");
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param field The field to read
     * @param result A List of Maps, where each Map is a set field/value pairs for one record
     */
    public void scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result)
    {
        delay();

        if (verbose)
        {
            System.out.print("SCAN "+table+" "+startkey+" "+recordcount+" [ ");
            System.out.print(field+" ");
            System.out.println("]");
        }
    }

	public void scan(String table, String startkey, int recordcount, Set<String> fields, List<Map<String,ByteIterator>> result) {
		delay();

		if (verbose)
		{
			System.out.print("SCAN "+table+" "+startkey+" "+recordcount+" [ ");
			if (fields!=null)
			{
				for (String f : fields)
				{
					System.out.print(f+" ");
				}
			}
			else
			{
				System.out.print("<all fields>");
			}

			System.out.println("]");
		}
	}

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param value The value to update in the key record
     */
    @Override
    public void updateOne(String table, String key, String field, ByteIterator value)
    {
        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>(1);
        values.put(field, value);
        update(table, key, values);
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A Map of field/value pairs to update in the record
     */
    @Override
    public void updateAll(String table, String key, Map<String,ByteIterator> values)
    {
        update(table, key, values);
    }

	private void update(String table, String key, Map<String,ByteIterator> values) {
		delay();

		if (verbose)
		{
			System.out.print("UPDATE "+table+" "+key+" [ ");
			if (values!=null)
			{
				for (String k : values.keySet())
				{
					System.out.print(k+"="+values.get(k)+" ");
				}
			}
			System.out.println("]");
		}
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
	 * record key.
	 *
	 *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A Map of field/value pairs to insert in the record
	 */
	public void insert(String table, String key, Map<String, ByteIterator> values)
	{
		delay();

		if (verbose)
		{
			System.out.print("INSERT "+table+" "+key+" [ ");
			if (values!=null)
			{
				for (String k : values.keySet())
				{
					System.out.print(k+"="+values.get(k)+" ");
				}
			}

			System.out.println("]");
		}
	}


	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 */
	public void delete(String table, String key)
	{
		delay();

		if (verbose)
		{
			System.out.println("DELETE "+table+" "+key);
		}
	}

	/**
	 * Short test of BasicDB
	 */
	/*
	public static void main(String[] args)
	{
		BasicDB bdb=new BasicDB();

		Properties p=new Properties();
		p.setProperty("Sky","Blue");
		p.setProperty("Ocean","Wet");

		bdb.setProperties(p);

		bdb.init();

		HashMap<String,String> fields=new HashMap<String,String>();
		fields.put("A","X");
		fields.put("B","Y");

		bdb.read("table","key",null,null);
		bdb.insert("table","key",fields);

		fields=new HashMap<String,String>();
		fields.put("C","Z");

		bdb.update("table","key",fields);

		bdb.delete("table","key");
	}*/
}
