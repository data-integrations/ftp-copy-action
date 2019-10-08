/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.action.ftp;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link FTPCopyActionConfig}.
 */
public class FTPCopyActionConfigTest {
  private static final String MOCK_STAGE = "mockStage";

  private static final FTPCopyActionConfig VALID_CONFIG = new FTPCopyActionConfig(
    "localhost",
    21,
    "user",
    "password",
    "data/in",
    "data/out",
    "c.*",
    true
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testEmptyHost() {
    FTPCopyActionConfig config = FTPCopyActionConfig.builder(VALID_CONFIG)
      .setHost("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, FTPCopyActionConfig.HOST);
  }

  @Test
  public void testEmptyDestDirectory() {
    FTPCopyActionConfig config = FTPCopyActionConfig.builder(VALID_CONFIG)
      .setDestDirectory("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, FTPCopyActionConfig.DEST_DIRECTORY);
  }

  @Test
  public void testInvalidPortNumber() {
    FTPCopyActionConfig config = FTPCopyActionConfig.builder(VALID_CONFIG)
      .setPort(100000)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, FTPCopyActionConfig.PORT);
  }

  @Test
  public void testEmptySourceDirectory() {
    FTPCopyActionConfig config = FTPCopyActionConfig.builder(VALID_CONFIG)
      .setSrcDirectory("")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector);
    ValidationAssertions.assertPropertyValidationFailed(failureCollector, FTPCopyActionConfig.SOURCE_DIRECTORY);
  }
}
