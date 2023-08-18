package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.sdk.Common;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class DeploymentManager {

    private ConcurrentHashMap<String, ConcurrentHashMap<String, Deployment>> deployments;
    private SQLRouter sqlRouter;

    public DeploymentManager(SQLRouter sqlRouter) {
        deployments = new ConcurrentHashMap<String, ConcurrentHashMap<String, Deployment>>();
        this.sqlRouter = sqlRouter;
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

    public void addDeployment(String db, String name, Deployment deployment) {
        ConcurrentHashMap<String, Deployment> deployMap = deployments.get(db);
        if (deployMap == null) {
            deployMap = deployments.put(db, new ConcurrentHashMap<String, Deployment>());
        }
        deployMap.put(name, deployment);
    }
}
