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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * Configurations for {@link FTPPutAction}.
 */
public class FTPPutActionConfig extends FTPActionConfig {
  public static final String SOURCE_PATH = "srcPath";

  @Name(SOURCE_PATH)
  @Description("Directory or File on the Filesystem which needs to be copied to the FTP Server.")
  @Macro
  private final String srcPath;

  public FTPPutActionConfig(String host, Integer port, String userName, String password,
                            String srcPath, String destDirectory, String fileNameRegex) {
    super(host, port, userName, password, destDirectory, fileNameRegex);
    this.srcPath = srcPath;
  }

  private FTPPutActionConfig(Builder builder) {
    super(builder.host, builder.port, builder.userName, builder.password, builder.destDirectory, builder.fileNameRegex);
    srcPath = builder.srcPath;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(FTPPutActionConfig copy) {
    Builder builder = new Builder();
    builder.setHost(copy.getHost());
    builder.setPort(copy.getPort());
    builder.setUserName(copy.getUserName());
    builder.setPassword(copy.getPassword());
    builder.setDestDirectory(copy.getDestDirectory());
    builder.setFileNameRegex(copy.getFileNameRegex());
    builder.setSrcPath(copy.getSrcPath());
    return builder;
  }

  public String getSrcPath() {
    return srcPath;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(SOURCE_PATH) && Strings.isNullOrEmpty(srcPath)) {
      collector.addFailure("Source path must be specified.", null)
        .withConfigProperty(SOURCE_PATH);
    }
  }

  /**
   * Builder for creating a {@link FTPPutActionConfig}.
   */
  public static final class Builder {
    private String host;
    private Integer port;
    private String userName;
    private String password;
    private String destDirectory;
    private String fileNameRegex;
    private String srcPath;

    private Builder() {
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPort(Integer port) {
      this.port = port;
      return this;
    }

    public Builder setUserName(String userName) {
      this.userName = userName;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setDestDirectory(String destDirectory) {
      this.destDirectory = destDirectory;
      return this;
    }

    public Builder setFileNameRegex(String fileNameRegex) {
      this.fileNameRegex = fileNameRegex;
      return this;
    }

    public Builder setSrcPath(String srcPath) {
      this.srcPath = srcPath;
      return this;
    }

    public FTPPutActionConfig build() {
      return new FTPPutActionConfig(this);
    }
  }
}
