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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Common config properties for FTP Action Plugins.
 */
public class FTPActionConfig extends PluginConfig {
  @Description("Host name of the FTP server.")
  @Macro
  public String host;

  @Description("Port on which FTP server is running. Defaults to 21.")
  @Nullable
  @Macro
  public String port;

  @Description("Name of the user used to login to FTP server. Defaults to 'anonymous'.")
  @Nullable
  @Macro
  public String userName;

  @Description("Password used to login to FTP server. Defaults to empty.")
  @Nullable
  @Macro
  public String password;

  @Description("Destination directory to which the files to be copied. If the directory does not exist," +
    " it will be created.")
  @Macro
  public String destDirectory;

  @Description("Regex to copy only the file names that match. By default, all files will be copied.")
  @Nullable
  @Macro
  public String fileNameRegex;

  public FTPActionConfig(String host, String port, String userName, String password, String destDirectory,
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
    return (port != null) ? Integer.parseInt(port) : 21;
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
}
