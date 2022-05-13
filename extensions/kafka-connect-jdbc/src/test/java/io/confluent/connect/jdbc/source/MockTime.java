/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A clock that you can manually advance by calling sleep
 */
public class MockTime implements Time {

    private long nanos = 0;
    private long autoTickMs = 0;

    public MockTime() {
        this.nanos = System.nanoTime();
    }

    public MockTime(long autoTickMs) {
        this.nanos = System.nanoTime();
        this.autoTickMs = autoTickMs;
    }

    @Override
    public long milliseconds() {
        this.sleep(autoTickMs);
        return TimeUnit.MILLISECONDS.convert(this.nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    @Override
    public long nanoseconds() {
        this.sleep(autoTickMs);
        return nanos;
    }

    @Override
    public void sleep(long ms) {
        this.nanos += TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
    }

    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long timeoutMs) {
        throw new UnsupportedOperationException();
    }
}
