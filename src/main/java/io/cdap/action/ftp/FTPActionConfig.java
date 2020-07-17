/*
 * Copyright © 2017-2020 Cask Data, Inc.
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
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

/**
 * Common config properties for FTP Action Plugins.
 */
public abstract class FTPActionConfig extends PluginConfig {
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String USER_NAME = "userName";
  public static final String PASSWORD = "password";
  public static final String DEST_DIRECTORY = "destDirectory";
  public static final String FILE_NAME_REGEX = "fileNameRegex";

  @Name(HOST)
  @Description("Host name of the FTP server.")
  @Macro
  private final String host;

  @Name(PORT)
  @Description("Port on which FTP server is running. Defaults to 21.")
  @Nullable
  @Macro
  private final Integer port;

  @Name(USER_NAME)
  @Description("Name of the user used to login to FTP server. Defaults to 'anonymous'.")
  @Nullable
  @Macro
  private final String userName;

  @Name(PASSWORD)
  @Description("Password used to login to FTP server. Defaults to empty.")
  @Nullable
  @Macro
  private final String password;

  @Name(DEST_DIRECTORY)
  @Description("Destination directory to which the files to be copied. If the directory does not exist," +
    " it will be created.")
  @Macro
  private final String destDirectory;

  @Name(FILE_NAME_REGEX)
  @Description("Regex to copy only the file names that match. By default, all files will be copied.")
  @Nullable
  @Macro
  private final String fileNameRegex;

  public FTPActionConfig(String host, Integer port, String userName, String password, String destDirectory,
                         String fileNameRegex) {
    this.host = host;
    this.port = port;
    this.userName = userName;
    this.password = password;
    this.destDirectory = destDirectory;
    this.fileNameRegex = fileNameRegex;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return (port != null) ? port : 21;
  }

  public String getUserName() {
    return (userName != null) ? userName : "anonymous";
  }

  public String getPassword() {
    return (password != null) ? password : "";
  }

  public String getDestDirectory() {
    return destDirectory;
  }

  public String getFileNameRegex() {
    return (fileNameRegex != null) ? fileNameRegex : ".*";
  }

  public void validate(FailureCollector collector) {
    if (!containsMacro(HOST) && Strings.isNullOrEmpty(host)) {
      collector.addFailure("Host must be specified.", null).withConfigProperty(HOST);
    }

    if (!containsMacro(DEST_DIRECTORY) && Strings.isNullOrEmpty(destDirectory)) {
      collector.addFailure("Destination directory must be specified.", null).withConfigProperty(DEST_DIRECTORY);
    }

    if (!containsMacro(PORT) && (port < 0 || port > 65535)) {
      collector.addFailure("Invalid port: " + port, "Port should be in range [0;65535]")
        .withConfigProperty(PORT);
    }
  }
}
