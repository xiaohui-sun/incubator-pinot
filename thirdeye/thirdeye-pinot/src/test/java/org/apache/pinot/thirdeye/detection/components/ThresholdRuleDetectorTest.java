/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.detection.components;

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionTimeSeries;
import org.apache.pinot.thirdeye.detection.wrapper.AnomalyDetectorWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class ThresholdRuleDetectorTest {
  private DataProvider testDataProvider;
  private DetectionPipeline detectionPipeline;
  private final double delta = 0.000001;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    timeSeries.put(MetricSlice.from(123L, 0, 10),
        new DataFrame().addSeries(COL_VALUE, 0, 100, 200, 500, 1000).addSeries(COL_TIME, 0, 2, 4, 6, 8));

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(123L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setId(124L);
    datasetConfigDTO.setDataset("thirdeye-test-dataset");
    datasetConfigDTO.setTimeDuration(2);
    datasetConfigDTO.setTimeUnit(TimeUnit.MILLISECONDS);
    datasetConfigDTO.setTimezone("UTC");

    DetectionConfigDTO detectionConfigDTO = new DetectionConfigDTO();
    detectionConfigDTO.setId(125L);
    Map<String, Object> detectorSpecs = new HashMap<>();
    detectorSpecs.put("min", 100);
    detectorSpecs.put("max", 500);
    detectorSpecs.put("className", ThresholdRuleDetector.class.getName());
    Map<String, Object> properties = new HashMap<>();
    properties.put("metricUrn", "thirdeye:metric:123");
    properties.put("detector", "$threshold");
    detectionConfigDTO.setProperties(properties);
    Map<String, Object> componentSpecs = new HashMap<>();
    componentSpecs.put("threshold", detectorSpecs);
    detectionConfigDTO.setComponentSpecs(componentSpecs);

    this.testDataProvider = new MockDataProvider()
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setTimeseries(timeSeries);
    this.detectionPipeline = new AnomalyDetectorWrapper(this.testDataProvider, detectionConfigDTO, 0, 10);
  }

  @Test
  public void testThresholdAlgorithmRun() throws Exception {
    DetectionPipelineResult result = this.detectionPipeline.run();
    List<MergedAnomalyResultDTO> anomalies = result.getAnomalies();
    Assert.assertEquals(result.getLastTimestamp(), 10);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0).getStartTime(), 0);
    Assert.assertEquals(anomalies.get(0).getEndTime(), 2);
    Assert.assertEquals(anomalies.get(1).getStartTime(), 8);
    Assert.assertEquals(anomalies.get(1).getEndTime(), 10);

    DetectionTimeSeries timeSeries = result.getPredictions();
    double[] upperBound = timeSeries.getPredictedUpperBound().values();
    double[] lowerBound = timeSeries.getPredictedLowerBound().values();
    Assert.assertEquals(upperBound.length, lowerBound.length);
    for (int i = 0; i < upperBound.length; i++) {
      Assert.assertEquals(upperBound[i], 500.0, delta);
      Assert.assertEquals(lowerBound[i], 100.0, delta);
    }
  }

}