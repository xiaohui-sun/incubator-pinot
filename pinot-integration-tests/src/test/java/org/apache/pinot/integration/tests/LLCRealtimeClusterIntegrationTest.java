/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableCustomConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TenantConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer.
 */
public class LLCRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {

  public static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  public static final long RANDOM_SEED = System.currentTimeMillis();
  public static final Random RANDOM = new Random(RANDOM_SEED);

  public final boolean _isDirectAlloc = RANDOM.nextBoolean();
  public final boolean _isConsumerDirConfigured = RANDOM.nextBoolean();
  private final long _startTime = System.currentTimeMillis();

  private static final String TEST_UPDATED_INVERTED_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS =
      Arrays.asList("FlightNum", "Origin", "Quarter", "DivActualElapsedTime");

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    // TODO Avoid printing to stdout. Instead, we need to add the seed to every assert in this (and super-classes)
    System.out.println("========== Using random seed value " + RANDOM_SEED);
    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }

    super.setUp();
  }

  @Override
  public void startController() {
    ControllerConf controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.setHLCTablesAllowed(false);
    startController(controllerConfig);
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Nullable
  @Override
  protected String getLoadMode() {
    return "MMAP";
  }

  @Override
  protected void overrideServerConf(Configuration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
  }

  @Test
  public void testConsumerDirectoryExists() {
    File consumerDirectory = new File(CONSUMER_DIRECTORY, "mytable_REALTIME");
    Assert.assertEquals(consumerDirectory.exists(), _isConsumerDirConfigured,
        "The off heap consumer directory does not exist");
  }

  @Test
  public void testSegmentFlushSize()
      throws Exception {

    String zkSegmentsPath = "/SEGMENTS/" + TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<String> segmentNames = _propertyStore.getChildNames(zkSegmentsPath, 0);
    for (String segmentName : segmentNames) {
      ZNRecord znRecord = _propertyStore.get(zkSegmentsPath + "/" + segmentName, null, 0);
      Assert.assertEquals(znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE),
          Integer.toString(getRealtimeSegmentFlushSize() / getNumKafkaPartitions()),
          "Segment: " + segmentName + " does not have the expected flush size");
    }
  }

  @Test
  public void testInvertedIndexTriggering()
      throws Exception {

    final long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
    Assert.assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    // TODO: investigate why assert for a specific value fails intermittently
    Assert.assertNotSame(queryResponse.get("numEntriesScannedInFilter").asLong(), 0);

    updateRealtimeTableConfig(getTableName(), UPDATED_INVERTED_INDEX_COLUMNS, null);

    sendPostRequest(_controllerRequestURLBuilder.forTableReload(getTableName(), "realtime"), null);

    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Override
      public Boolean apply(@javax.annotation.Nullable Void aVoid) {
        try {
          JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
          // Total docs should not change during reload
          Assert.assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
          Assert.assertEquals(queryResponse.get("numConsumingSegmentsQueried").asLong(), 2);
          Assert.assertTrue(queryResponse.get("minConsumingFreshnessTimeMs").asLong() > _startTime);
          Assert.assertTrue(queryResponse.get("minConsumingFreshnessTimeMs").asLong() < System.currentTimeMillis());
          return queryResponse.get("numEntriesScannedInFilter").asLong() == 0;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }, 600_000L, "Failed to generate inverted index");
  }

  @Test
  public void testAddHLCTableShouldFail() {
    TableConfig tableConfig = new TableConfig();
    IndexingConfig indexingConfig = new IndexingConfig();
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("stream.kafka.consumer.type", "HIGHLEVEL");
    indexingConfig.setStreamConfigs(streamConfigs);
    tableConfig.setIndexingConfig(indexingConfig);
    tableConfig.setTableName("testTable");
    tableConfig.setTableType(CommonConstants.Helix.TableType.REALTIME);
    SegmentsValidationAndRetentionConfig validationAndRetentionConfig = new SegmentsValidationAndRetentionConfig();
    tableConfig.setValidationConfig(validationAndRetentionConfig);
    TenantConfig tenantConfig = new TenantConfig();
    tableConfig.setTenantConfig(tenantConfig);
    TableCustomConfig tableCustomConfig = new TableCustomConfig();
    tableConfig.setCustomConfig(tableCustomConfig);
    try {
      sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonConfigString());
      Assert.fail();
    } catch (IOException e) {
      // Expected
    }
  }
}

