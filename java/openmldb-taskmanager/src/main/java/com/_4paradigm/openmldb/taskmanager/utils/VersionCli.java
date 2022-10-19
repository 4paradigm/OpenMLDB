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
package com._4paradigm.openmldb.taskmanager.utils;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class VersionCli {

    public static void main(String[] argv) {
        try {
            System.out.println(VersionCli.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String readInputStreamAsString(InputStream in) throws IOException {
        BufferedInputStream bis = new BufferedInputStream(in);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int result = bis.read();
        while(result != -1) {
            byte b = (byte)result;
            buf.write(b);
            result = bis.read();
        }
        return buf.toString();
    }
    
    public static String getVersion() throws Exception {
        InputStream stream = VersionCli.class.getClassLoader().getResourceAsStream("git.properties");
        if (stream == null) {
            throw new Exception("Fail to get version from file of openmldb_git.properties");
        }
        // Do not use apache IOUtils to get rid of the dependency
        //List<String> gitVersionStrList = IOUtils.readLines(stream, "UTF-8");
        String versionStr = readInputStreamAsString(stream);
        List<String> gitVersionStrList = Arrays.asList(versionStr.split("\n"));

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
