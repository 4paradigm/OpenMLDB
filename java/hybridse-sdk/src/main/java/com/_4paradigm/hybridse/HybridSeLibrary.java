/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.hybridse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: This class is deprecated and we should load the library by SqlClusterExecutor.initJavaSdkLibrary()
 * Library loader for hybridse jsdk core.
 */
public class HybridSeLibrary {
    private static final Logger logger = LoggerFactory.getLogger(HybridSeLibrary.class.getName());
    private static final String DEFAULT_HYBRIDSE_JSDK_CORE_NAME = "hybridse_jsdk_core";

    private static boolean initialized = false;

    /**
     * Load hybridse jsdk core if it hasn't loaded before.
     */
    public static synchronized void initCore() {
        if (initialized) {
            return;
        }
        LibraryLoader.loadLibrary(DEFAULT_HYBRIDSE_JSDK_CORE_NAME);
        initialized = true;
    }

    /**
     * Load hybridse jsdk core if it hasn't loaded before.
     */
    public static synchronized void initCore(String jsdkCoreLibraryPath) {
        if (initialized) {
            return;
        }
        LibraryLoader.loadLibrary(jsdkCoreLibraryPath);
        initialized = true;
    }

    static synchronized boolean isInitialized() {
        return initialized;
    }

}
