package com._4paradigm.rtidb.client.impl;

import java.util.concurrent.ConcurrentHashMap;

import com._4paradigm.rtidb.client.schema.Table;

public class GTableSchema {

	public final static ConcurrentHashMap<Integer, Table> tableSchema = new ConcurrentHashMap<Integer, Table>();
}
