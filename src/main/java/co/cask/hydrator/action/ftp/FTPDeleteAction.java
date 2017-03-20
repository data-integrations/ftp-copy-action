package co.cask.hydrator.action.ftp;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.hydrator.action.common.FTPActionConfig;
import co.cask.hydrator.action.common.FTPConnector;
import com.jcraft.jsch.ChannelSftp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link Action} to delete files on the SFTP server.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("FTPDelete")
public class FTPDeleteAction extends Action {

  private static final Logger LOG = LoggerFactory.getLogger(FTPDeleteAction.class);
  private FTPDeleteActionConfig config;
  public FTPDeleteAction(FTPDeleteActionConfig config) {
    this.config = config;
  }

  public class FTPDeleteActionConfig extends FTPActionConfig {
    @Description("Comma separated list of files to be deleted from FTP server.")
    @Macro
    public String filesToDelete;

    @Description("Boolean flag to determine if execution should continue if there is an error while deleting any file." +
      " Defaults to 'false'.")
    boolean continueOnError;

    public String getFilesToDelete() {
      return filesToDelete;
    }
  }

  @Override
  public void run(ActionContext context) throws Exception {
    String filesToDelete = config.getFilesToDelete();
    try (FTPConnector ftpConnector = new FTPConnector(config.getHost(), config.getPort(), config.getUserName(),
                                                      config.getPassword(), config.getSSHProperties())) {
      ChannelSftp channelSftp = ftpConnector.getSftpChannel();
      for (String fileToDelete : filesToDelete.split(",")) {
        LOG.info("Deleting {}", fileToDelete);
        try {
          channelSftp.rm(fileToDelete);
        } catch (Throwable t) {
          if (config.continueOnError) {
            LOG.warn("Error deleting file {}.", fileToDelete, t);
          } else {
            throw t;
          }
        }
      }
    }
  }
}
