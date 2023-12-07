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
package com._4paradigm.openmldb.test_common.restful.model;

import lombok.Data;
import org.apache.http.cookie.Cookie;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class HttpResult implements Serializable {
    private int httpCode;
    private Map<String,String> headers;
    private List<Cookie> cookies;
    private long beginTime;
    private long endTime;
    private String data;

}
