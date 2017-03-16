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
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    @Description("Host name of the FTP server.")
    @Macro
    public String host;

    @Description("Port on which FTP server is running. Defaults to 21.")
    @Nullable
    @Macro
    public String port;

    @Description("Protocol to use. Valid values are 'ftp' and 'sftp'. Defaults to 'ftp'.")
    @Nullable
    public String protocol;

    @Description("Name of the user used to login to FTP server. Defaults to 'anonymous'.")
    @Nullable
    public String userName;

    @Description("Password used to login to FTP server. Defaults to empty.")
    @Nullable
    public String password;

    @Description("Directory on the FTP server which is to be copied.")
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
      return (port != null) ? Integer.parseInt(port) : 21;
    }

    public String getProtocol() {
      return (protocol != null) ? protocol : "ftp";
    }

    public String getUserName() {
      return (userName != null) ? userName : "anonymous";
    }

    public String getPassword() {
      return (password != null) ? password : "";
    }

    public Boolean getExtractZipFiles() {
      return (extractZipFiles != null) ? extractZipFiles : true;
    }
  }

  private static void runJsch(String host, int port, String user, String password, String srcDir) throws Exception {
    JSch jsch = new JSch();
    LOG.info("Adding identity");
    Session session = jsch.getSession(user, host, port);
    session.setPassword(password);
    Properties properties = new Properties();
    properties.put("StrictHostKeyChecking", "no");
    session.setConfig(properties);
    LOG.info("Connecting to Host: {}, Post: {} with User: {}", host, port, user);
    session.connect();
    LOG.info("Connected to sftp host.");
    Channel channel = session.openChannel("sftp");
    channel.connect();
    ChannelSftp channelSftp = (ChannelSftp) channel;
    Vector vv = channelSftp.ls(srcDir);
    if (vv != null) {
      for (int ii = 0; ii < vv.size(); ii++) {
        Object obj = vv.elementAt(ii);
        if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
          LOG.info("Downloading...{}", (((com.jcraft.jsch.ChannelSftp.LsEntry) obj).getLongname()));
          LOG.info("Downloading...{}", (((com.jcraft.jsch.ChannelSftp.LsEntry) obj).getFilename()));
          String fileName = (((com.jcraft.jsch.ChannelSftp.LsEntry) obj).getFilename());
          if (fileName.endsWith(".zip")) {
            // copyJschZip(channelSftp.get(fileName), fileSystem, );
          }
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

  @Override
  public void run(ActionContext context) throws Exception {
    runJsch(config.host, config.getPort(), config.getUserName(), config.getPassword(), config.getSrcDirectory());

    Path destination = new Path(config.getDestDirectory());
    FileSystem fileSystem = FileSystem.get(new Configuration());
    destination = fileSystem.makeQualified(destination);
    if (!fileSystem.exists(destination)) {
      fileSystem.mkdirs(destination);
    }

    FTPClient ftp;
    if ("ftp".equals(config.getProtocol().toLowerCase())) {
      ftp = new FTPClient();
    } else {
      ftp = new FTPSClient();
    }
    ftp.setControlKeepAliveTimeout(5);
    // UNIX type server
    FTPClientConfig ftpConfig = new FTPClientConfig();
    // Set additional parameters required for the ftp
    // for example config.setServerTimeZoneId("Pacific/Pitcairn")
    ftp.configure(ftpConfig);
    try {
      ftp.connect(config.getHost(), config.getPort());
      ftp.enterLocalPassiveMode();
      String replyString = ftp.getReplyString();
      LOG.info("Connected to server {} and port {} with reply from connect as {}.", config.getHost(), config.getPort(),
               replyString);

      // Check the reply code for actual success
      int replyCode = ftp.getReplyCode();

      if (!FTPReply.isPositiveCompletion(replyCode)) {
        ftp.disconnect();
        throw new RuntimeException(String.format("FTP server refused connection with code %s and reply %s.",
                                                 replyCode, replyString));
      }

      if (!ftp.login(config.getUserName(), config.getPassword())) {
        LOG.error("login command reply code {}, {}", ftp.getReplyCode(), ftp.getReplyString());
        ftp.logout();
        throw new RuntimeException(String.format("Login to the FTP server %s and port %s failed. " +
                                                   "Please check user name and password.", config.getHost(),
                                                 config.getPort()));
      }

      FTPFile[] ftpFiles = ftp.listFiles(config.getSrcDirectory());
      LOG.info("listFiles command reply code: {}, {}.", ftp.getReplyCode(), ftp.getReplyString());
      // Check the reply code for listFiles call.
      // If its "522 Data connections must be encrypted" then it means data channel also need to be encrypted
      if (ftp.getReplyCode() == 522 && "sftp".equalsIgnoreCase(config.getProtocol())) {
        // encrypt data channel and listFiles again
        ((FTPSClient) ftp).execPROT("P");
        LOG.info("Attempting command listFiles on encrypted data channel.");
        ftpFiles = ftp.listFiles(config.getSrcDirectory());
      }
      for (FTPFile file : ftpFiles) {
        String source = config.getSrcDirectory() + "/" + file.getName();

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
      if (ftp.isConnected()) {
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
