# SFTP copy action


Description
-----------
Copy files from SFTP server to the specified destination.


Use Case
--------
Common use case is to have files uploaded on the SFTP server in compressed format. These files can then
be accessed over the network by various applications. This plugin targets use case where the files can be
downloaded from SFTP server in an uncompressed format and stored on the desired destination such as `HDFS`.


Properties
----------
**host:** Specifies the host name of the SFTP server. (Macro-enabled)

**port:** Specifies the port on which SFTP server is running. Defaults to `22`. (Macro-enabled)

**userName:** Specifies the name of the user to be used while logging to SFTP server.

**password:** Specifies the password to be used while logging to SFTP server.

**srcDirectory:** Specifies the directory on the SFTP server which is to be copied. (Macro-enabled)

**destDirectory:** Specifies the destination directory to which files to be copied. If the directory does not exist,
it will be created. (Macro-enabled)

**extractZipFiles:** Boolean flag to determine whether zip files on the SFTP server need to be extracted on
the destination while copying. Defaults to 'true'.

**sshProperties:** Specifies the properties that are used to configure SSH connection to the FTP server.
For example to enable verbose logging add property 'LogLevel' with value 'VERBOSE'. To enable host key checking set
'StrictHostKeyChecking' to 'yes'. SSH can be configured with the properties described here
'https://linux.die.net/man/5/ssh_config'.
