/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.action.ftp;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.hydrator.action.common.FTPActionConfig;
import co.cask.hydrator.action.common.FTPConnector;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.ChannelSftp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.Nullable;

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

  /**
   * Configurations for the FTP copy action plugin.
   */
  public class FTPCopyActionConfig extends FTPActionConfig {
    @Description("Directory on the SFTP server which is to be copied.")
    @Macro
    public String srcDirectory;

    @Description("Destination directory to which the files to be copied. If the directory does not exist," +
      " it will be created.")
    @Macro
    public String destDirectory;

    @Description("Boolean flag to determine whether zip files on the FTP server need to be extracted " +
      "on the destination while copying. Defaults to 'true'.")
    @Nullable
    public Boolean extractZipFiles;

    @Description("Name of the variable in which comma separated list of file names that are copied by the " +
      "plugin will be put.")
    @Nullable
    public String variableNameHoldingFileList;


    public String getSrcDirectory() {
      return srcDirectory;
    }

    public String getDestDirectory() {
      return destDirectory;
    }

    public Boolean getExtractZipFiles() {
      return (extractZipFiles != null) ? extractZipFiles : true;
    }

    public String getVariableNameHoldingFileList() {
      return variableNameHoldingFileList != null ? variableNameHoldingFileList : "ftp.files.copied";
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path destination = new Path(config.getDestDirectory());
    FileSystem fileSystem = FileSystem.get(new Configuration());
    destination = fileSystem.makeQualified(destination);
    if (!fileSystem.exists(destination)) {
      fileSystem.mkdirs(destination);
    }

    try (FTPConnector ftpConnector = new FTPConnector(config.getHost(), config.getPort(), config.getUserName(),
                                                      config.getPassword(), config.getSSHProperties())) {
      ChannelSftp channelSftp = ftpConnector.getSftpChannel();

      Vector files = channelSftp.ls(config.getSrcDirectory());

      List<String> filesCopied = new ArrayList<>();
      for (int index = 0; index < files.size(); index++) {
        Object obj = files.elementAt(index);
        if (!(obj instanceof ChannelSftp.LsEntry)) {
          continue;
        }
        ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) obj;
        if (".".equals(entry.getFilename()) || "..".equals(entry.getFilename())) {
          // ignore "." and ".." files
          continue;
        }
        LOG.info("Downloading file {}", entry.getFilename());

        String completeFileName = config.getSrcDirectory() + "/" + entry.getFilename();

        if (config.getExtractZipFiles() && entry.getFilename().endsWith(".zip")) {
          copyJschZip(channelSftp.get(completeFileName), fileSystem, destination);
        } else {
          Path destinationPath = fileSystem.makeQualified(new Path(destination, entry.getFilename()));
          LOG.debug("Downloading {} to {}", entry.getFilename(), destinationPath.toString());
          try (OutputStream output = fileSystem.create(destinationPath)) {
            InputStream is = channelSftp.get(completeFileName);
            ByteStreams.copy(is, output);
          }
        }
        filesCopied.add(completeFileName);
      }
      context.getArguments().set(config.getVariableNameHoldingFileList(), Joiner.on(",").join(filesCopied));
      LOG.info("Variables copied to {}.", Joiner.on(",").join(filesCopied));
    }
  }

  private void copyJschZip(InputStream is, FileSystem fs, Path destination) throws IOException {
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
