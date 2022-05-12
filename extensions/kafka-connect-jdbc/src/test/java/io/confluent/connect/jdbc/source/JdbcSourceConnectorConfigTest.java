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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigValue;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CachedRecommenderValues;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CachingRecommender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Recommender.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceConnectorConfigTest {

  private EmbeddedDerby db;
  private Map<String, String> props;
  private ConfigDef configDef;
  private List<ConfigValue> results;
  @Mock
  private Recommender mockRecommender;
  private MockTime time = new MockTime();

  @Before
  public void setup() throws Exception {
    configDef = null;
    results = null;
    props = new HashMap<>();

    db = new EmbeddedDerby();
    db.createTable("some_table", "id", "INT");

    db.execute("CREATE SCHEMA PUBLIC_SCHEMA");
    db.execute("SET SCHEMA PUBLIC_SCHEMA");
    db.createTable("public_table", "id", "INT");

    db.execute("CREATE SCHEMA PRIVATE_SCHEMA");
    db.execute("SET SCHEMA PRIVATE_SCHEMA");
    db.createTable("private_table", "id", "INT");
    db.createTable("another_private_table", "id", "INT");
  }

  @After
  public void cleanup() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testConnectionAttemptsAtLeastOne() {
    props.put(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG, "0");
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @Test
  public void testConfigTableNameRecommenderWithoutSchemaOrTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    // Should have no recommended values
    assertWhitelistRecommendations();
    assertBlacklistRecommendations();
  }

  @Test
  public void testConfigTableNameRecommenderWitSchemaAndWithoutTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    // Should have no recommended values
    assertWhitelistRecommendations();
    assertBlacklistRecommendations();
  }

  @Test
  public void testConfigTableNameRecommenderWithSchemaAndTableTypes() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, "PRIVATE_SCHEMA");
    props.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, "VIEW");
    configDef = JdbcSourceConnectorConfig.baseConfigDef();
    results = configDef.validate(props);
    assertWhitelistRecommendations();
    assertBlacklistRecommendations();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCachingRecommender() {
    final List<Object> results1 = Collections.singletonList((Object) "xyz");
    final List<Object> results2 = Collections.singletonList((Object) "123");
    // Set up the mock recommender to be called twice, returning different results each time
    EasyMock.expect(mockRecommender.validValues(EasyMock.anyObject(String.class), EasyMock.anyObject(Map.class))).andReturn(results1);
    EasyMock.expect(mockRecommender.validValues(EasyMock.anyObject(String.class), EasyMock.anyObject(Map.class))).andReturn(results2);

    PowerMock.replayAll();

    CachingRecommender recommender = new CachingRecommender(mockRecommender, time, 1000L);

    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    // Populate the cache
    assertSame(results1, recommender.validValues("x", config1));
    // Try the cache before expiration
    time.sleep(100L);
    assertSame(results1, recommender.validValues("x", config1));
    // Wait for the cache to expire
    time.sleep(2000L);
    assertSame(results2, recommender.validValues("x", config1));

    PowerMock.verifyAll();
  }

  @Test
  public void testDefaultConstructedCachedTableValuesReturnsNull() {
    Map<String, Object> config = Collections.singletonMap("k", (Object) "v");
    CachedRecommenderValues cached = new CachedRecommenderValues();
    assertNull(cached.cachedValue(config, 20L));
  }

  @Test
  public void testCachedTableValuesReturnsCachedResultWithinExpiryTime() {
    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    Map<String, Object> config2 = Collections.singletonMap("k", (Object) "v");
    List<Object> results = Collections.singletonList((Object) "xyz");
    long expiry = 20L;
    CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
    assertSame(results, cached.cachedValue(config2, expiry - 1L));
  }

  @Test
  public void testCachedTableValuesReturnsNullResultAtOrAfterExpiryTime() {
    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    Map<String, Object> config2 = Collections.singletonMap("k", (Object) "v");
    List<Object> results = Collections.singletonList((Object) "xyz");
    long expiry = 20L;
    CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
    assertNull(cached.cachedValue(config2, expiry));
    assertNull(cached.cachedValue(config2, expiry + 1L));
  }

  @Test
  public void testCachedTableValuesReturnsNullResultIfConfigurationChanges() {
    Map<String, Object> config1 = Collections.singletonMap("k", (Object) "v");
    Map<String, Object> config2 = Collections.singletonMap("k", (Object) "zed");
    List<Object> results = Collections.singletonList((Object) "xyz");
    long expiry = 20L;
    CachedRecommenderValues cached = new CachedRecommenderValues(config1, results, expiry);
    assertNull(cached.cachedValue(config2, expiry - 1L));
    assertNull(cached.cachedValue(config2, expiry));
    assertNull(cached.cachedValue(config2, expiry + 1L));
  }

  @Test
  public void testSpacesInTopicPrefix() {
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, " withLeadingTailingSpaces ");
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertTrue(connectionAttemptsConfig.errorMessages().isEmpty());

    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "with spaces");
    validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @Test
  public void testInvalidCharsInTopicPrefix() {
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "az_-.09");
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertTrue(connectionAttemptsConfig.errorMessages().isEmpty());

    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "az_-.!@#$%^&*09");
    validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @Test
  public void testTooLongTopicPrefix() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 249; i++) {
      sb.append("a");
    }
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, sb.toString());
    Map<String, ConfigValue> validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    ConfigValue connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertTrue(connectionAttemptsConfig.errorMessages().isEmpty());

    sb.append("a");
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, sb.toString());
    validatedConfig =
        JdbcSourceConnectorConfig.baseConfigDef().validateAll(props);
    connectionAttemptsConfig =
        validatedConfig.get(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
    assertNotNull(connectionAttemptsConfig);
    assertFalse(connectionAttemptsConfig.errorMessages().isEmpty());
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertContains(Collection<T> actual, T... expected) {
    for (T e : expected) {
      assertTrue(actual.contains(e));
    }
    assertEquals(expected.length, actual.size());
  }

  protected ConfigValue namedValue(List<ConfigValue> values, String name) {
    for (ConfigValue value : values) {
      if (value.name().equals(name)) return value;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertRecommendedValues(ConfigValue value, T... recommendedValues) {
    assertContains(value.recommendedValues(), recommendedValues);
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertWhitelistRecommendations(T... recommendedValues) {
    assertContains(namedValue(results, JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG).recommendedValues(), recommendedValues);
  }

  @SuppressWarnings("unchecked")
  protected <T> void assertBlacklistRecommendations(T... recommendedValues) {
    assertContains(namedValue(results, JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG).recommendedValues(), recommendedValues);
  }
}