/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * An {@link Action} that will copy files from File System to FTP Server.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("FTPPut")
public class FTPPutAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(FTPPutAction.class);
  private FTPPutActionConfig config;

  public FTPPutAction(FTPPutActionConfig config) {
    this.config = config;
  }

  /**
   * Configurations for the FTP Put Action Plugin.
   */
  public static class FTPPutActionConfig extends FTPActionConfig {

    @Description("Directory or File on the Filesystem which needs to be copied to the FTP Server.")
    @Macro
    public String srcPath;

    public FTPPutActionConfig(String host, String port, String userName, String password,
                              String srcPath, String destDirectory, String fileNameRegex) {
      super(host, port, userName, password, destDirectory, fileNameRegex);
      this.srcPath = srcPath;
    }

    public String getSrcPath() {
      return srcPath;
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path source = new Path(config.getSrcPath());
    FileSystem fileSystem = FileSystem.get(new Configuration());
    if (!fileSystem.exists(source)) {
      throw new RuntimeException(String.format("Source Files don't exist at %s", source));
    }

    FTPClient ftp = null;
    try {
      ftp = FTPUtils.getFTPClient(config.getHost(), config.getPort(), config.getUserName(), config.getPassword());

      // No easy way to check existence of directory, create directory if not present
      boolean dirExists = ftp.changeWorkingDirectory(config.getDestDirectory());
      if (!dirExists) {
        ftp.makeDirectory(config.getDestDirectory());
        ftp.changeWorkingDirectory(config.getDestDirectory());
      }

      // Filter out only the files to copy
      FileStatus[] filesToCopy = fileSystem.listStatus(source, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          String fileName = path.getName();
          return fileName.matches(config.getFileNameRegex());
        }
      });

      for (FileStatus file : filesToCopy) {
        Path filePath = file.getPath();
        try (InputStream inputStream = fileSystem.open(filePath)) {
          boolean success = ftp.storeFile(filePath.getName(), inputStream);
          if (!success) {
            LOG.error("Error copying file : {}", filePath);
          }
        }
      }

      ftp.logout();
    } finally {
      if (ftp != null && ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch (Throwable t) {
          LOG.error("Failure to disconnect the ftp connection.", t);
        }
      }
    }
  }
}
