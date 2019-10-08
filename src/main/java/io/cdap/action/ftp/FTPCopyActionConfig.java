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

import javax.annotation.Nullable;

/**
 * Configurations for {@link FTPCopyAction}.
 */
public class FTPCopyActionConfig extends FTPActionConfig {
  public static final String SOURCE_DIRECTORY = "srcDirectory";
  public static final String EXTRACT_ZIP_FILES = "extractZipFiles";

  @Name(SOURCE_DIRECTORY)
  @Description("Directory on the FTP server which is to be copied.")
  @Macro
  private final String srcDirectory;

  @Name(EXTRACT_ZIP_FILES)
  @Description("Boolean flag to determine whether zip files on the FTP server need to be extracted " +
    "on the destination while copying. Defaults to 'true'.")
  @Nullable
  private final Boolean extractZipFiles;

  public FTPCopyActionConfig(String host, Integer port, String userName, String password, String srcDirectory,
                             String destDirectory, String fileNameRegex, boolean extractZipFiles) {
    super(host, port, userName, password, destDirectory, fileNameRegex);
    this.srcDirectory = srcDirectory;
    this.extractZipFiles = extractZipFiles;
  }

  private FTPCopyActionConfig(Builder builder) {
    super(builder.host, builder.port, builder.userName, builder.password, builder.destDirectory, builder.fileNameRegex);
    srcDirectory = builder.srcDirectory;
    extractZipFiles = builder.extractZipFiles;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(FTPCopyActionConfig copy) {
    Builder builder = new Builder();
    builder.setHost(copy.getHost());
    builder.setPort(copy.getPort());
    builder.setUserName(copy.getUserName());
    builder.setPassword(copy.getPassword());
    builder.setDestDirectory(copy.getDestDirectory());
    builder.setFileNameRegex(copy.getFileNameRegex());
    builder.setSrcDirectory(copy.getSrcDirectory());
    builder.setExtractZipFiles(copy.getExtractZipFiles());
    return builder;
  }

  public String getSrcDirectory() {
    return srcDirectory;
  }

  public Boolean getExtractZipFiles() {
    return (extractZipFiles != null) ? extractZipFiles : true;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(SOURCE_DIRECTORY) && Strings.isNullOrEmpty(srcDirectory)) {
      collector.addFailure("Source directory must be specified.", null)
        .withConfigProperty(SOURCE_DIRECTORY);
    }
  }

  /**
   * Builder for creating a {@link FTPCopyActionConfig}.
   */
  public static final class Builder {
    private String host;
    private Integer port;
    private String userName;
    private String password;
    private String destDirectory;
    private String fileNameRegex;
    private String srcDirectory;
    private Boolean extractZipFiles;

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

    public Builder setSrcDirectory(String srcDirectory) {
      this.srcDirectory = srcDirectory;
      return this;
    }

    public Builder setExtractZipFiles(Boolean extractZipFiles) {
      this.extractZipFiles = extractZipFiles;
      return this;
    }

    public FTPCopyActionConfig build() {
      return new FTPCopyActionConfig(this);
    }
  }
}
