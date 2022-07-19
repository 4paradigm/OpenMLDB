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
package com._4paradigm.openmldb.batch.utils;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.List;

public class VersionCli {

    public static void main(String[] argv) {
        try {
            System.out.println(VersionCli.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getVersion() throws Exception {

        InputStream stream = VersionCli.class.getClassLoader().getResourceAsStream("openmldb_git.properties");
        if (stream == null) {
            throw new Exception("Fail to get version from file of openmldb_git.properties");
        }
        List<String> gitVersionStrList = IOUtils.readLines(stream, "UTF-8");

        // Only get build version and git commit abbrev
        String version = "";
        String gitCommit = "";
        for (String line : gitVersionStrList) {
            if (line.startsWith("git.build.version=")) {
                version = line.split("=")[1];
            }
            if (line.startsWith("git.commit.id.abbrev=")) {
                gitCommit = line.split("=")[1];
            }
        }

        return version + "-" + gitCommit;
    }
}
