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

package io.confluent.connect.jdbc.util;

import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CachedConnectionProviderTest.class})
@PowerMockIgnore("javax.management.*")
public class CachedConnectionProviderTest {

  @Mock
  private ConnectionProvider provider;

  @Test
  public void retryTillFailure() throws SQLException {
    int retries = 15;
    ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
    EasyMock.expect(provider.getConnection()).andThrow(new SQLException("test")).times(retries);
    PowerMock.replayAll();

    try {
      connectionProvider.getConnection();
    }catch(ConnectException ce){
      assertNotNull(ce);
    }

    PowerMock.verifyAll();
  }


  @Test
  public void retryTillConnect() throws SQLException {
    Connection connection = EasyMock.createMock(Connection.class);
    int retries = 15;

    ConnectionProvider connectionProvider = new CachedConnectionProvider(provider, retries, 100L);
    EasyMock.expect(provider.getConnection())
            .andThrow(new SQLException("test"))
            .times(retries-1)
            .andReturn(connection);
    PowerMock.replayAll();

    assertNotNull(connectionProvider.getConnection());

    PowerMock.verifyAll();
  }

}
