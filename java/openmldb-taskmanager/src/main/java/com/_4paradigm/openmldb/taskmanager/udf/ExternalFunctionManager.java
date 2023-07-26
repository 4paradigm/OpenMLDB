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

package com._4paradigm.openmldb.taskmanager.udf;

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalFunctionManager {
    private static final Log logger = LogFactory.getLog(ExternalFunctionManager.class);

    // The map of UDF name and library file name
    static private Map<String, String> nameFileMap = new ConcurrentHashMap<>();

    static public String getLibraryFilePath(String libraryFileName) {
        return Paths.get(TaskManagerConfig.EXTERNAL_FUNCTION_DIR, libraryFileName).toString();
    }

    static public void addFunction(String fnName, String libraryFileName) throws Exception {
        if (hasFunction(fnName)) {
            logger.warn(String.format("The function %s exists, replace", fnName));
        } 
        String libraryFilePath = getLibraryFilePath(libraryFileName);
        if(!(new File(libraryFilePath).exists())) {
            throw new Exception("The library file does not exist in path: " + libraryFilePath);
        }
        nameFileMap.put(fnName, libraryFileName);
    }

    static public void dropFunction(String fnName) {
        if (hasFunction(fnName)) {
            nameFileMap.remove(fnName);
        } else {
            logger.warn(String.format("The function %s does not exist, ignore dropping function", fnName));
        }
    }

    static public boolean hasFunction(String fnName) {
        return nameFileMap.containsKey(fnName);
    }

    static public Set<String> getAllFnNames() {
        return nameFileMap.keySet();
    }

    static public List<String> getAllLibraryFilePaths() {
        List<String> libraryFilePaths = new ArrayList<>();
        for (String libraryFileName: nameFileMap.values()) {
            String libraryFilePath = getLibraryFilePath(libraryFileName);
            if(new File(libraryFilePath).exists()) {
                libraryFilePaths.add(libraryFilePath);
            } else {
                logger.warn("The library file does not exist and do not submit: " + libraryFilePath);
            }

        }
        return libraryFilePaths;
    }

}
