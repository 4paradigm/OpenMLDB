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

package com._4paradigm.openmldb.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/*
 * Path may include wildcard, like 2018[8-9]*, but these characters are not valid in URI,
 * So we first encode the path, except '/' and ':'.
 * When we get path, we need to decode the path first.
 * eg:
 * hdfs://host/testdata/20180[8-9]*;
 * -->
 * hdfs://host/testdata/20180%5B8-9%5D*
 *
 * getPath() will return the decoded path, it's the same as input uri: /testdata/20180[8-9]*
 */
public class WildcardURI {
    private static Logger logger = LoggerFactory.getLogger(WildcardURI.class);
    private boolean valid = false;

    private URI uri;

    public WildcardURI(String path) {
        try {
            // 1. call URLEncoder.encode to encode all special character, like /, *, [, %
            // 2. recover the : and /
            // 3. the space(" ") will be encoded to "+", we have to change it to "%20"
            // example can be found in WildcardURITest.java
            String encodedPath = URLEncoder.encode(path, StandardCharsets.UTF_8.toString()).replaceAll("%3A",
                    ":").replaceAll("%2F", "/").replaceAll("\\+", "%20");
            uri = new URI(encodedPath);
            uri.normalize();
            valid = true;
            logger.info(uri.getRawPath());
        } catch (UnsupportedEncodingException | URISyntaxException e) {
            logger.warn("failed to encoded uri: " + path, e);
            e.printStackTrace();
        }
    }

    public URI getUri() {
        if (!valid) {
            return null;
        }
        return uri;
    }

    public String getAuthority() {
        if (!valid) {
            return null;
        }
        return uri.getAuthority();
    }

    public String getPath() {
        if (!valid) {
            return null;
        }
        return uri.getPath();
    }
}
