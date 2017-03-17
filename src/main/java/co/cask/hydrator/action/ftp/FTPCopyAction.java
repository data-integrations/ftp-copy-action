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
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.hydrator.common.KeyValueListParser;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
  public class FTPCopyActionConfig extends PluginConfig {
    @Description("Host name of the SFTP server.")
    @Macro
    public String host;

    @Description("Port on which SFTP server is running. Defaults to 22.")
    @Nullable
    @Macro
    public String port;

    @Description("Name of the user used to login to SFTP server.")
    public String userName;

    @Description("Password used to login to SFTP server.")
    public String password;

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

    @Description("Properties that will be used to configure the SSH connection to the FTP server. " +
      "For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. " +
      "To enable host key checking set 'StrictHostKeyChecking' to 'yes'. " +
      "SSH can be configured with the properties described here 'https://linux.die.net/man/5/ssh_config'.")
    @Nullable
    public String sshProperties;

    public String getHost() {
      return host;
    }

    public String getSrcDirectory() {
      return srcDirectory;
    }

    public String getDestDirectory() {
      return destDirectory;
    }

    public int getPort() {
      return (port != null) ? Integer.parseInt(port) : 22;
    }

    public String getUserName() {
      return userName;
    }

    public String getPassword() {
      return password;
    }

    public Boolean getExtractZipFiles() {
      return (extractZipFiles != null) ? extractZipFiles : true;
    }

    @Nullable
    public String getSshProperties() {
      return sshProperties;
    }
  }

  private Map<String, String> getSSHProperties(String sshProperties) {
    Map<String, String> properties = new HashMap<>();
    // Default set to no
    properties.put("StrictHostKeyChecking", "no");
    if (sshProperties == null || sshProperties.isEmpty()) {
      return properties;
    }

    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");
    for (KeyValue<String, String> keyVal : kvParser.parse(sshProperties)) {
      String key = keyVal.getKey();
      String val = keyVal.getValue();
      properties.put(key, val);
    }
    return properties;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    Path destination = new Path(config.getDestDirectory());
    FileSystem fileSystem = FileSystem.get(new Configuration());
    destination = fileSystem.makeQualified(destination);
    if (!fileSystem.exists(destination)) {
      fileSystem.mkdirs(destination);
    }

    JSch jsch = new JSch();
    Session session = null;
    Channel channel = null;
    try {
      session = jsch.getSession(config.getUserName(), config.getHost(), config.getPort());
      session.setPassword(config.getPassword());
      Properties properties = new Properties();
      // properties.put("StrictHostKeyChecking", "no");
      properties.putAll(getSSHProperties(config.getSshProperties()));
      LOG.info("Properties {}", properties);
      session.setConfig(properties);
      LOG.info("Connecting to Host: {}, Port: {}, with User: {}", config.getHost(), config.getPort(),
               config.getUserName());
      // Set connect timeout to be 30 seconds
      session.connect(30000);
      channel = session.openChannel("sftp");
      channel.connect();
      ChannelSftp channelSftp = (ChannelSftp) channel;

      Vector files = channelSftp.ls(config.getSrcDirectory());

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
      }
    } finally {
      LOG.info("Closing SFTP session.");
      if (channel != null) {
        try {
          channel.disconnect();
        } catch (Throwable t) {
          LOG.warn("Error while disconnecting sftp channel.", t);
        }
      }

      if (session != null) {
        try {
          session.disconnect();
        } catch (Throwable t) {
          LOG.warn("Error while disconnecting sftp session.", t);
        }
      }
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
