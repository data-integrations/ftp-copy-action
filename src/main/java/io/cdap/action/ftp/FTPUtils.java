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

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public final class FTPUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FTPUtils.class);

  private FTPUtils() {
    // util class
  }

  public static FTPClient getFTPClient(String host, int port, String user, String password) throws IOException {
    FTPClient ftp = new FTPClient();
    ftp.setControlKeepAliveTimeout(5);
    // UNIX type server
    FTPClientConfig ftpConfig = new FTPClientConfig();
    // Set additional parameters required for the ftp
    // for example config.setServerTimeZoneId("Pacific/Pitcairn")
    ftp.configure(ftpConfig);
    ftp.connect(host, port);
    ftp.enterLocalPassiveMode();
    String replyString = ftp.getReplyString();
    LOG.info("Connected to server {} and port {} with reply from connect as {}.", host, port, replyString);

    // Check the reply code for actual success
    int replyCode = ftp.getReplyCode();

    if (!FTPReply.isPositiveCompletion(replyCode)) {
      ftp.disconnect();
      throw new RuntimeException(String.format("FTP server refused connection with code %s and reply %s.",
                                               replyCode, replyString));
    }

    if (!ftp.login(user, password)) {
      LOG.error("login command reply code {}, {}", ftp.getReplyCode(), ftp.getReplyString());
      ftp.logout();
      throw new RuntimeException(String.format("Login to the FTP server %s and port %s failed. " +
                                                 "Please check user name and password.", host, port));
    }
    return ftp;
  }
}
