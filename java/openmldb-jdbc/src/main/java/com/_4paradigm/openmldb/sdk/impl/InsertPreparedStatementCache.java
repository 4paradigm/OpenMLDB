package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.SqlException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class InsertPreparedStatementCache {
    static final int CACHE_SIZE = 100000;
    static final int EXPIRE_TIME = 1000 * 60;  // 1 minute

    private Cache<AbstractMap.SimpleImmutableEntry<String, String>, InsertPreparedStatementMeta> cache;

    private ZKClient zkClient;
    private NodeCache nodeCache;
    private String tablePath;

    public InsertPreparedStatementCache(ZKClient zkClient) throws SqlException {
        cache = Caffeine.newBuilder().maximumSize(CACHE_SIZE).expireAfterAccess(EXPIRE_TIME, TimeUnit.MILLISECONDS).build();
        this.zkClient = zkClient;
        if (zkClient != null) {
            tablePath = zkClient.getConfig().getNamespace() + "/table/db_table_data";
            nodeCache = new NodeCache(zkClient.getClient(), zkClient.getConfig().getNamespace() + "/table/notify");
            try {
                nodeCache.start();
                nodeCache.getListenable().addListener(new NodeCacheListener() {
                    @Override
                    public void nodeChanged() throws Exception {
                        checkAndInvalid();
                    }
                });
            } catch (Exception e) {
                throw new SqlException("NodeCache exception: " + e.getMessage());
            }
        }
    }

    public InsertPreparedStatementMeta get(String db, String sql) {
        return cache.getIfPresent(new AbstractMap.SimpleImmutableEntry<>(db, sql));
    }

    public void put(String db, String sql, InsertPreparedStatementMeta meta) {
        cache.put(new AbstractMap.SimpleImmutableEntry<>(db, sql), meta);
    }

    public void checkAndInvalid() throws Exception {
        if (!zkClient.checkExists(tablePath)) {
            return;
        }
        List<String> children = zkClient.getChildren(tablePath);
        Map<AbstractMap.SimpleImmutableEntry<String, String>, InsertPreparedStatementMeta> view = cache.asMap();
        Map<AbstractMap.SimpleImmutableEntry<String, String>, Integer> tableMap = new HashMap<>();
        for (String path : children) {
            byte[] bytes = zkClient.getClient().getData().forPath(tablePath + "/" + path);
            NS.TableInfo tableInfo = NS.TableInfo.parseFrom(bytes);
            tableMap.put(new AbstractMap.SimpleImmutableEntry<>(tableInfo.getDb(), tableInfo.getName()), tableInfo.getTid());
        }
        Iterator<Map.Entry<AbstractMap.SimpleImmutableEntry<String, String>, InsertPreparedStatementMeta>> iterator
                = view.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<AbstractMap.SimpleImmutableEntry<String, String>, InsertPreparedStatementMeta> entry = iterator.next();
            String db = entry.getKey().getKey();
            InsertPreparedStatementMeta meta = entry.getValue();
            String name = meta.getName();
            Integer tid = tableMap.get(new AbstractMap.SimpleImmutableEntry<>(db, name));
            if (tid != null && tid != meta.getTid()) {
                cache.invalidate(entry.getKey());
            }
        }
    }
}
