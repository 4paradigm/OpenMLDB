/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.sdk.SqlException;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import com._4paradigm.openmldb.proto.SQLProcedure;
import com._4paradigm.openmldb.proto.Type;
import org.xerial.snappy.Snappy;

import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DeploymentManager {

    private ConcurrentHashMap<String, ConcurrentHashMap<String, Deployment>> deployments = new ConcurrentHashMap<>();
    // private ConcurrentHashMap<AbstractMap.SimpleImmutableEntry<String, String>, Deployment> deployments;
    private ZKClient zkClient;
    private NodeCache nodeCache;
    private String spPath;

    public DeploymentManager(ZKClient zkClient) throws SqlException {
        this.zkClient = zkClient;
        if (zkClient != null) {
            spPath = zkClient.getConfig().getNamespace() + "/store_procedure/db_sp_data";
            nodeCache = new NodeCache(zkClient.getClient(), zkClient.getConfig().getNamespace() + "/table/notify");
            try {
                parseAllDeployment();
                nodeCache.start();
                nodeCache.getListenable().addListener(new NodeCacheListener() {
                    @Override
                    public void nodeChanged() throws Exception {
                        parseAllDeployment();
                    }
                });
            } catch (Exception e) {
                throw new SqlException("start NodeCache failed. " + e.getMessage());
            }
        }
    }

    public void parseAllDeployment() throws Exception {
        if (!zkClient.checkExists(spPath)) {
            return;
        }
        List<String> children = zkClient.getChildren(spPath);
        for (String path : children) {
            byte[] bytes = zkClient.getClient().getData().forPath(spPath + "/" + path);
            byte[] data = Snappy.uncompress(bytes);
            SQLProcedure.ProcedureInfo procedureInfo = SQLProcedure.ProcedureInfo.parseFrom(data);
            if (procedureInfo.getType() != Type.ProcedureType.kReqDeployment) {
                continue;
            }
            Deployment deployment = getDeployment(procedureInfo.getDbName(), procedureInfo.getSpName());
            if (deployment != null) {
                if (deployment.getSQL().equals(procedureInfo.getSql())) {
                    continue;
                }
            }
            addDeployment(procedureInfo.getDbName(), procedureInfo.getSpName(), new Deployment(procedureInfo));
        }
    }

    public boolean hasDeployment(String db, String name) {
        ConcurrentHashMap<String, Deployment> innerMap = deployments.get(db);
        if (innerMap == null) {
            return false;
        }
        return innerMap.containsKey(name);
    }

    public Deployment getDeployment(String db, String name) {
        ConcurrentHashMap<String, Deployment> innerMap = deployments.get(db);
        if (innerMap == null) {
            return null;
        }
        return innerMap.get(name);
    }

    public synchronized void addDeployment(String db, String name, Deployment deployment) {
        ConcurrentHashMap<String, Deployment> deployMap = deployments.get(db);
        if (deployMap == null) {
            deployMap = new ConcurrentHashMap<>();
            deployments.put(db, deployMap);
        }
        Deployment curDeployment = deployMap.get(name);
        if (curDeployment == null || !curDeployment.getSQL().equals(deployment.getSQL())) {
            deployMap.put(name, deployment);
        }
    }
}
