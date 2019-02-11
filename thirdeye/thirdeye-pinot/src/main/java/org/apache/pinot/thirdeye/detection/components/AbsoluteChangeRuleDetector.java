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

import org.apache.pinot.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import org.apache.pinot.thirdeye.dataframe.BooleanSeries;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
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
import org.apache.pinot.thirdeye.detection.spec.AbsoluteChangeRuleDetectorSpec;
import org.apache.pinot.thirdeye.detection.spi.components.AnomalyDetector;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionResult;
import org.apache.pinot.thirdeye.detection.spi.model.DetectionTimeSeries;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.joda.time.Interval;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


@Components(title = "Absolute change rule detection",
    type = "ABSOLUTE_CHANGE_RULE",
    tags = {DetectionTag.RULE_DETECTION},
    presentation = {
        @PresentationOption(name = "absolute value", template = "comparing ${offset} is ${pattern} more than ${difference}"),
    },
    params = {
        @Param(name = "offset", defaultValue = "wo1w"),
        @Param(name = "change", placeholder = "value"),
        @Param(name = "pattern", allowableValues = {"up", "down"})
    })
public class AbsoluteChangeRuleDetector implements AnomalyDetector<AbsoluteChangeRuleDetectorSpec> {
  private double absoluteChange;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private Pattern pattern;
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_ANOMALY = "anomaly";
  private static final String COL_DIFF = "diff";
  private static final String COL_PATTERN = "pattern";
  private static final String COL_DIFF_VIOLATION = "diff_violation";

  @Override
  public DetectionResult runDetection(Interval window, String metricUrn) {
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    MetricSlice slice = MetricSlice.from(me.getId(), window.getStartMillis(), window.getEndMillis(), me.getFilters());
    List<MetricSlice> slices = new ArrayList<>(this.baseline.scatter(slice));
    slices.add(slice);
    InputData data = this.dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(slices).withMetricIdsForDataset(
        Collections.singletonList(slice.getMetricId())));
    DataFrame dfCurr = data.getTimeseries().get(slice).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = this.baseline.gather(slice, data.getTimeseries()).renameSeries(COL_VALUE, COL_BASE);

    DataFrame df = new DataFrame(dfCurr).addSeries(dfBase);

    // calculate predicted timeseries
    DetectionTimeSeries timeSeries = new DetectionTimeSeries();
    timeSeries.addTimeStamps(df.getLongs(COL_TIME));
    DoubleSeries baseline = df.getDoubles(COL_BASE);
    timeSeries.addPredictedBaseline(baseline);

    // calculate absolute change
    df.addSeries(COL_DIFF, df.getDoubles(COL_CURR).subtract(df.get(COL_BASE)));

    // defaults
    df.addSeries(COL_ANOMALY, BooleanSeries.fillValues(df.size(), false));
    // absolute change
    if (!Double.isNaN(this.absoluteChange)) {
      DoubleSeries upper = baseline.add(this.absoluteChange);
      DoubleSeries lower = baseline.subtract(this.absoluteChange);

      // consistent with pattern
      if (pattern.equals(Pattern.UP_OR_DOWN) ) {
        df.addSeries(COL_PATTERN, BooleanSeries.fillValues(df.size(), true));
        timeSeries.addPredictedUpperBound(upper);
        timeSeries.addPredictedLowerBound(lower);
      } else if (pattern.equals(Pattern.UP)) {
        df.addSeries(COL_PATTERN, df.getDoubles(COL_DIFF).gt(0));
        timeSeries.addPredictedUpperBound(upper);
      } else {
        df.addSeries(COL_PATTERN, df.getDoubles(COL_DIFF).lt(0));
        timeSeries.addPredictedLowerBound(lower);
      }
      df.addSeries(COL_DIFF_VIOLATION, df.getDoubles(COL_DIFF).abs().gte(this.absoluteChange));
      df.mapInPlace(BooleanSeries.ALL_TRUE, COL_ANOMALY, COL_PATTERN, COL_DIFF_VIOLATION);
    }

    // make anomalies
    DatasetConfigDTO datasetConfig = data.getDatasetForMetricId().get(me.getId());
    List<MergedAnomalyResultDTO> anomalies = DetectionUtils.makeAnomalies(slice, df, COL_ANOMALY, window.getEndMillis(), datasetConfig);
    return new DetectionResult(anomalies, timeSeries);
  }

  @Override
  public void init(AbsoluteChangeRuleDetectorSpec spec, InputDataFetcher dataFetcher) {
    this.absoluteChange = spec.getAbsoluteChange();
    this.dataFetcher = dataFetcher;
    String timezone = spec.getTimezone();
    String offset = spec.getOffset();
    this.baseline = BaselineParsingUtils.parseOffset(offset, timezone);
    this.pattern = Pattern.valueOf(spec.getPattern().toUpperCase());
  }
}
