/*
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

package org.apache.pinot.thirdeye.detection.components;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.anomalydetection.context.TimeSeries;
import org.apache.pinot.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.Pattern;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.annotation.PresentationOption;
import org.apache.pinot.thirdeye.detection.spec.PercentageChangeRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionTimeSeries;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import org.joda.time.Interval;

import static org.apache.pinot.thirdeye.dataframe.DoubleSeries.*;
import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;

@Components(title = "Percentage change rule detection",
    type = "PERCENTAGE_RULE",
    tags = {DetectionTag.RULE_DETECTION},
    description = "Computes a multi-week aggregate baseline and compares the current value "
        + "based on relative change.",
    presentation = {
        @PresentationOption(name = "percentage change", template = "comparing ${offset} is ${pattern} more than ${change}")
    },
    params = {
        @Param(name = "offset", defaultValue = "wo1w"),
        @Param(name = "change", placeholder = "value"),
        @Param(name = "pattern", allowableValues = {"up", "down"})
    })
public class PercentageChangeRuleDetector implements AnomalyDetector<PercentageChangeRuleDetectorSpec> {
  private double percentageChange;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private Pattern pattern;
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_CHANGE = "change";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_PATTERN = "pattern";
  private static final String COL_CHANGE_VIOLATION = "change_violation";

  @Override
  public DetectionResult runDetection(Interval window, String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), window.getEndMillis(), me.getFilters());
    List<MetricSlice> slices = new ArrayList<>(this.baseline.scatter(slice));
    slices.add(slice);
    InputData data = this.dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(slices).withMetricIdsForDataset(Collections.singletonList(slice.getMetricId())));
    DataFrame dfCurr = data.getTimeseries().get(slice).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = this.baseline.gather(slice, data.getTimeseries()).renameSeries(COL_VALUE, COL_BASE);

    DataFrame df = new DataFrame(dfCurr).addSeries(dfBase);

    // calculate predicted timeseries
    DetectionTimeSeries timeSeries = new DetectionTimeSeries();
    timeSeries.addTimeStamps(df.getLongs(COL_TIME));
    DoubleSeries baseline = df.getDoubles(COL_BASE);
    timeSeries.addPredictedBaseline(baseline);

    // calculate percentage change
    df.addSeries(COL_CHANGE, map((Series.DoubleFunction) values -> {
      if (values[1] == 0) {
        return values[0] > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
      }
      return values[0] / values[1];
    }, df.getDoubles(COL_CURR), df.get(COL_BASE)).subtract(1));

    // defaults
    df.addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false));

    // relative change
    if (!Double.isNaN(this.percentageChange)) {
      DoubleSeries upper = baseline.multiply(1 + this.percentageChange);
      DoubleSeries lower = baseline.multiply(1 - this.percentageChange);

      // consistent with pattern
      if (pattern.equals(Pattern.UP_OR_DOWN) ) {
        df.addSeries(COL_PATTERN, BooleanSeries.fillValues(df.size(), true));
        timeSeries.addPredictedUpperBound(upper);
        timeSeries.addPredictedLowerBound(lower);
      } else if (pattern.equals(Pattern.UP)) {
        df.addSeries(COL_PATTERN, df.getDoubles(COL_CHANGE).gt(0));
        timeSeries.addPredictedUpperBound(upper);
      } else {
        df.addSeries(COL_PATTERN, df.getDoubles(COL_CHANGE).lt(0));
        timeSeries.addPredictedLowerBound(lower);
      }
      df.addSeries(COL_CHANGE_VIOLATION, df.getDoubles(COL_CHANGE).abs().gte(this.percentageChange));
      df.mapInPlace(BooleanSeries.ALL_TRUE, COL_ANOMALY, COL_PATTERN, COL_CHANGE_VIOLATION);
    }

    // anomalies
    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());
    List<MergedAnomalyResultDTO> anomalies = DetectionUtils.makeAnomalies(slice, df, COL_ANOMALY, window.getEndMillis(), datasetConfig);
    return new DetectionResult(anomalies, timeSeries);
  }

  @Override
  public void init(PercentageChangeRuleDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.percentageChange = spec.getPercentageChange();
    this.dataFetcher = dataFetcher;
    String timezone = spec.getTimezone();
    String offset = spec.getOffset();
    this.baseline = BaselineParsingUtils.parseOffset(offset, timezone);
    this.pattern = Pattern.valueOf(spec.getPattern().toUpperCase());
  }
}
