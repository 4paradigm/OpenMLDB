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

package com._4paradigm.openmldb.java_sdk_test.yarn;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


@Slf4j
@Feature("ExternalFunction")
public class TestExternalFunction extends OpenMLDBTest {

    @Story("ExternalFunction")
    @Test(enabled = false)
    public void testFunctionMethods() {
        Statement statement = executor.getStatement();

        try {
            statement.execute("CREATE FUNCTION cut2(x STRING) RETURNS STRING OPTIONS (FILE='/tmp/libtest_udf.so')");

            statement.execute("SHOW FUNCTIONS");

            statement.execute("set @@execute_mode='online'");
            statement.execute("select cut2('hello')");
            ResultSet resultset = statement.getResultSet();
            resultset.next();
            String result = resultset.getString(1);
            assert(result.equals("he"));

            statement.execute("DROP FUNCTION cut2");
            assert(true);
        } catch (SQLException e) {
            e.printStackTrace();
            assert(false);
        }
    }

}
 