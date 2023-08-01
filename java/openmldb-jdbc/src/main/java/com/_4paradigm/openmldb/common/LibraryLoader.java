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

package com._4paradigm.openmldb.common;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;


public class LibraryLoader {

    private static final Logger logger = LoggerFactory.getLogger(LibraryLoader.class.getName());

    synchronized public static void loadLibrary(String libraryPath) {
        boolean isPath = libraryPath.endsWith(".so") ||
                libraryPath.endsWith(".dylib");
        if (!isPath) {
            // try load from environment
            try {
                System.loadLibrary(libraryPath);
                logger.info("Successfully load library {}", libraryPath);
                return;
            } catch (Throwable t) {
                logger.debug(String.format("Failed to load %s", libraryPath));
            }
        }
        if (!isPath) {
            String osName = System.getProperty("os.name").toLowerCase();
            if (osName.equals("mac os x")) {
                libraryPath = "lib" + libraryPath + ".dylib";
            } else if (osName.contains("linux")) {
                libraryPath = "lib" + libraryPath + ".so";
            } else {
                throw new IllegalArgumentException("Do not support os type: " + osName);
            }
        }

        // load from local filesystem
        try {
            System.load(libraryPath);
            logger.info("Successfully load library from {}", libraryPath);
            return;
        } catch (Throwable t) {
            logger.debug(String.format("Failed to load from %s", libraryPath));
        }

        // load from resource
        logger.info("Can not find {} from environment, try find in resources", libraryPath);
        try {
            URL resource = LibraryLoader.class.getClassLoader().getResource(libraryPath);
            if (resource != null) {
                try {
                    System.load(resource.getPath());
                } catch (UnsatisfiedLinkError e) {
                    // maybe extract out of jar
                    String localPath = extractResource(libraryPath, true);
                    logger.info("Extract resource to {}", localPath);
                    System.load(localPath);
                }
                logger.info("Successfully load {} in local resource", resource.getPath());
            } else {
                logger.error(String.format("Fail to find %s in resources", libraryPath));
            }
        } catch (IOException | UnsatisfiedLinkError e) {
            String msg = String.format("Error while load %s from local resource", libraryPath);
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }


    /**
     * Extract library in resource into filesystem
     * @param path the path that will be extracted.
     * @param isTemp whether to create a temp file.
     * @return the absolute path of the resources.
     * @throws IOException if the resources cannot be found.
     */
    public static String extractResource(String path, boolean isTemp) throws IOException {
        InputStream inputStream = LibraryLoader.class.getClassLoader().getResourceAsStream(path);
        if (inputStream != null) {
            logger.info("Found {} in local resource", path);
            File localFile;
            if (isTemp) {
                String suffix = path.replace("/", "-");  // do not make temp directory
                localFile = File.createTempFile("temp-", suffix);
            } else {
                localFile = new File("./", path);
                if (localFile.exists()) {
                    logger.warn("Existing path {}, will overwrite", localFile.getPath());
                }
                File parent = localFile.getParentFile();
                if (parent != null && !parent.exists()) {
                    parent.mkdirs();
                }
            }
            localFile.deleteOnExit();
            String absolutePath = localFile.getAbsolutePath();
            try (FileOutputStream outputStream = new FileOutputStream(absolutePath)) {
                IOUtils.copy(inputStream, outputStream);
                return absolutePath;
            }
        } else {
            throw new IOException(String.format("Can not find %s in local resource", path));
        }
    }

}
