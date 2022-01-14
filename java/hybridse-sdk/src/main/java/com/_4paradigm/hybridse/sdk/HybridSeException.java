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

package com._4paradigm.hybridse.sdk;

/**
 * The general exception class throw when something goes wrong during compiling SQL queries.
 *
 * <p>This includes, but is not limited to, SQL syntax error, Non-support Plan and so on.</p>
 */
public class HybridSeException extends Exception {

    private String message;
    private Throwable cause;

    public HybridSeException(String message) {
        this.message = message;
        this.cause = null;
    }

    public HybridSeException(String message, Throwable cause) {
        this.message = message;
        this.cause = cause;
    }

    public synchronized Throwable getCause() {
        return this.cause;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
