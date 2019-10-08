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

import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * An {@link Action} that will copy files from FTP server to the destination directory.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("FTPCopy")
public class FTPCopyAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(FTPCopyAction.class);
  private FTPCopyActionConfig config;

  public FTPCopyAction(FTPCopyActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Path destination = new Path(config.getDestDirectory());
    FileSystem fileSystem = FileSystem.get(new Configuration());
    destination = fileSystem.makeQualified(destination);
    if (!fileSystem.exists(destination)) {
      fileSystem.mkdirs(destination);
    }

    FTPClient ftp = null;
    try {
      ftp = FTPUtils.getFTPClient(config.getHost(), config.getPort(), config.getUserName(), config.getPassword());

      FTPFile[] ftpFiles = ftp.listFiles(config.getSrcDirectory());
      LOG.info("listFiles command reply code: {}, {}.", ftp.getReplyCode(), ftp.getReplyString());

      for (FTPFile file : ftpFiles) {
        String source = config.getSrcDirectory() + "/" + file.getName();

        // Ignore files that don't match the given file regex
        String fileName = file.getName();
        if (!fileName.matches(config.getFileNameRegex())) {
          LOG.debug("Skipping file {} since it doesn't match the regex.", fileName);
          continue;
        }

        LOG.info("Current file {}, source {}", file.getName(), source);
        if (config.getExtractZipFiles() && file.getName().endsWith(".zip")) {
          copyZip(ftp, source, fileSystem, destination);
        } else {
          Path destinationPath = fileSystem.makeQualified(new Path(destination, file.getName()));
          LOG.debug("Downloading {} to {}", file.getName(), destinationPath.toString());
          try (OutputStream output = fileSystem.create(destinationPath)) {
            InputStream is = ftp.retrieveFileStream(source);
            ByteStreams.copy(is, output);
          }
        }
        if (!ftp.completePendingCommand()) {
          LOG.error("Error completing command.");
        }
      }
      ftp.logout();
    } finally {
      if (ftp != null && ftp.isConnected()) {
        try {
          ftp.disconnect();
        } catch (Throwable e) {
          LOG.error("Failure to disconnect the ftp connection.", e);
        }
      }
    }
  }

  private void copyZip(FTPClient ftp, String source, FileSystem fs, Path destination) throws IOException {
    InputStream is = ftp.retrieveFileStream(source);
    try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        LOG.debug("Extracting {}", entry);
        Path destinationPath = fs.makeQualified(new Path(destination, entry.getName()));
        try (OutputStream os = fs.create(destinationPath)) {
          LOG.debug("Downloading {} to {}", entry.getName(), destinationPath.toString());
          ByteStreams.copy(zis, os);
        }
      }
    }
  }
}
