# SFTP delete action


Description
-----------
Deletes files from SFTP server.


Use Case
--------
This plugin is generally used along with the FTPCopy plugin. FTPCopy plugin copies the files from SFTP
server to the desired location such as `HDFS`. Once copied to `HDFS`, files can be processed by pipeline.
When the pipeline is successful, generally we would want to delete the files from SFTP server as they no
longer required. FTPDelete plugin can be used in the end of the pipeline to achieve this.

Properties
----------
**host:** Specifies the host name of the SFTP server. (Macro-enabled)

**port:** Specifies the port on which SFTP server is running. Defaults to `22`. (Macro-enabled)

**userName:** Specifies the name of the user to be used while logging to SFTP server.

**password:** Specifies the password to be used while logging to SFTP server.

**filesToDelete:** Comma separated list of file names to delete from SFTP server. (Macro-enabled)

**continueOnError:** Boolean flag to determine whether to proceed with next files in case there is a failure
in deletion of any particular file. Defaults to 'false'.

**sshProperties:** Specifies the properties that are used to configure SSH connection to the FTP server.
For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. To enable host key checking set
'StrictHostKeyChecking' to 'yes'. SSH can be configured with the properties described here
'https://linux.die.net/man/5/ssh_config'.
