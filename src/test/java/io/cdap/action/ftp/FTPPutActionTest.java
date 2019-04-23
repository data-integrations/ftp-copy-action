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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link FTPPutAction}.
 */
public class FTPPutActionTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final String USER = "john";
  private static final String PWD = "abcd";
  private static final String DATA_DIR = "data";

  public static File srcFolder;
  public static File file;
  public static File dummyFile;

  public static File destFolder;
  public static File destFile;

  public static File dataFolder;

  public static int port;

  private static FakeFtpServer ftpServer;

  @BeforeClass
  public static void init() throws Exception {
    srcFolder = TMP_FOLDER.newFolder();
    destFolder = TMP_FOLDER.newFolder();

    dataFolder = new File(destFolder, DATA_DIR);

    file = new File(srcFolder, "check.txt");
    dummyFile = new File(srcFolder, "dummy.txt");

    destFile = new File(destFolder, "some.txt");

    file.createNewFile();
    dummyFile.createNewFile();

    try (FileWriter fileWriter = new FileWriter(file);
         PrintWriter printWriter = new PrintWriter(fileWriter)) {
      printWriter.print("Data From Gokul");
    }

    try (FileWriter fileWriter = new FileWriter(dummyFile);
         PrintWriter printWriter = new PrintWriter(fileWriter)) {
      printWriter.print("From another laptop");
    }

    ftpServer = new FakeFtpServer();
    ftpServer.setServerControlPort(0);

    FileSystem fileSystem = new UnixFakeFileSystem();
    fileSystem.add(new FileEntry(destFile.getAbsolutePath(), "Some Text"));
    ftpServer.setFileSystem(fileSystem);

    ftpServer.addUserAccount(new UserAccount(USER, PWD, destFolder.getAbsolutePath()));
    ftpServer.start();

    int waitPeriod = 5;
    while (waitPeriod > 0) {
      if (ftpServer.isStarted()) {
        break;
      }
      TimeUnit.SECONDS.sleep(1);
      waitPeriod--;
    }

    if (!ftpServer.isStarted()) {
      throw new IOException("FTP Server Failed to start.");
    }
    port = ftpServer.getServerControlPort();
  }

  @AfterClass
  public static void stop() throws Exception {
    if (ftpServer != null) {
      ftpServer.stop();
    }
  }

  @Test
  public void testFTPPutAction() throws Exception {
    FTPPutAction.FTPPutActionConfig actionConfig = new FTPPutAction.FTPPutActionConfig(
      "localhost", Integer.toString(port), USER, PWD, srcFolder.getAbsolutePath(), DATA_DIR, "c.*");
    FTPPutAction action = new FTPPutAction(actionConfig);

    UnixFakeFileSystem fs = (UnixFakeFileSystem) ftpServer.getFileSystem();
    List names = fs.listFiles(dataFolder.getPath());
    Assert.assertEquals(0, names.size());
    action.run(null);
    names = fs.listNames(dataFolder.getPath());
    Assert.assertEquals(1, names.size());
  }
}
