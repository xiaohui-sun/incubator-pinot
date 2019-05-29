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

package org.apache.pinot.thirdeye.anomaly.alert.util;

import org.apache.pinot.thirdeye.alert.content.EmailContentFormatterConfiguration;
import org.apache.pinot.thirdeye.anomaly.SmtpConfiguration;
import org.apache.pinot.thirdeye.anomaly.utils.EmailUtils;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.anomaly.SmtpConfiguration.SMTP_CONFIG_KEY;


public class EmailScreenshotHelper {

  private static final String TEMP_PATH = "/tmp/graph";
  private static final String SCREENSHOT_FILE_SUFFIX = ".png";
  private static final String GRAPH_SCREENSHOT_GENERATOR_SCRIPT = "/getGraphPnj.js";
  private static final Logger LOG = LoggerFactory.getLogger(EmailScreenshotHelper.class);
  private static final ExecutorService executorService = Executors.newCachedThreadPool();

  public static String takeGraphScreenShot(final String anomalyId, final EmailContentFormatterConfiguration configuration) throws JobExecutionException {
    return takeGraphScreenShot(anomalyId, configuration.getDashboardHost(), configuration.getRootDir(),
        configuration.getPhantomJsPath(), configuration.getSmtpConfiguration(), configuration.getFailureFromAddress(),
        configuration.getFailureToAddress());
  }

  public static String takeGraphScreenShot(final String anomalyId, final ThirdEyeConfiguration configuration) throws JobExecutionException {
    return takeGraphScreenShot(anomalyId, configuration.getDashboardHost(), configuration.getRootDir(),
        configuration.getPhantomJsPath(),
        SmtpConfiguration.createFromProperties(configuration.getAlerterConfiguration().get(SMTP_CONFIG_KEY)),
        configuration.getFailureFromAddress(), configuration.getFailureToAddress());
  }

  public static String takeGraphScreenShot(final String anomalyId, final String dashboardHost, final String rootDir,
      final String phantomJsPath, final SmtpConfiguration smtpConfiguration, final String failureFromAddress,
      final String failureToAddress) throws JobExecutionException{
    Callable<String> callable = new Callable<String>() {
      public String call() throws Exception {
        return takeScreenshot(anomalyId, dashboardHost, rootDir, phantomJsPath);
      }
    };
    Future<String> task = executorService.submit(callable);
    String result = null;
    try {
      result = task.get(3, TimeUnit.MINUTES);
      LOG.info("Finished with result: {}", result);
    } catch (Exception e) {
      LOG.error("Exception in fetching screenshot for anomaly id {}", anomalyId, e);
      EmailHelper.sendFailureEmailForScreenshot(anomalyId, e.fillInStackTrace(), smtpConfiguration, failureFromAddress,
          new DetectionAlertFilterRecipients(EmailUtils.getValidEmailAddresses(failureToAddress)));
    }
    return result;
  }

  private static String takeScreenshot(String anomalyId, String dashboardHost, String rootDir, String phantomJsPath) throws Exception {

    String imgRoute = dashboardHost + "/app/#/screenshot/" + anomalyId;
    LOG.info("imgRoute {}", imgRoute);
    String phantomScript = rootDir + GRAPH_SCREENSHOT_GENERATOR_SCRIPT;
    LOG.info("Phantom JS script {}", phantomScript);
    String imgPath = TEMP_PATH + anomalyId + SCREENSHOT_FILE_SUFFIX;
    LOG.info("imgPath {}", imgPath);
    Process proc = Runtime.getRuntime().exec(new String[]{phantomJsPath, "phantomjs", "--ssl-protocol=any", "--ignore-ssl-errors=true",
        phantomScript, imgRoute, imgPath});

    StringBuilder sbError = new StringBuilder();
    try (
      InputStream stderr = proc.getErrorStream();
      InputStreamReader isr = new InputStreamReader(stderr);
      BufferedReader br = new BufferedReader(isr);
    ) {
      // exhaust the error stream before waiting for the process to exit
      String line = br.readLine();
      if (line != null) {
        do {
          sbError.append(line);
          sbError.append('\n');

          line = br.readLine();
        } while (line != null);
      }
    }
    boolean isComplete = proc.waitFor(2, TimeUnit.MINUTES);
    if (!isComplete) {
      proc.destroyForcibly();
      throw new Exception("PhantomJS process timeout");
    }
    return imgPath;
  }

}
