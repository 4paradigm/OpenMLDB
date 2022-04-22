package com._4paradigm.openmldb.taskmanager.udf;

import com._4paradigm.openmldb.taskmanager.server.TaskManagerServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalFunctionManager {
    private static final Log logger = LogFactory.getLog(ExternalFunctionManager.class);

    static private Map<String, String> namePathMap = new ConcurrentHashMap<>();

    static public void addFunction(String fnName, String libraryFilePath) {
        if (hasFunction(fnName)) {
            logger.warn(String.format("The function %s exists, ignore adding function", fnName));
        } else {
            namePathMap.put(fnName, libraryFilePath);
        }
    }

    static public void dropFunction(String fnName) {
        if (hasFunction(fnName)) {
            namePathMap.remove(fnName);
        } else {
            logger.warn(String.format("The function %s does not exist, ignore dropping function", fnName));
        }
    }

    static public boolean hasFunction(String fnName) {
        return namePathMap.containsKey(fnName);
    }

    static public String getLibraryFilePath(String fnName) {
        return namePathMap.get(fnName);
    }

    static public Set<String> getAllFnNames() {
        return namePathMap.keySet();
    }

    static public Collection<String> getAllLibraryFilePaths() {
        return namePathMap.values();
    }

}
