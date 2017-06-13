# FTP copy action

Copy files from FTP server to the specified destination.

## Usage Notes

Common use case is to have files uploaded on the FTP server in compressed format. These files can then be accessed
over the network using FTP by various applications. This plugin targets use case where files can be downloaded
from FTP server in an uncompressed format and stored on the desired destination such as `HDFS`.

Properties
----------
**host:** Specifies the host name of the FTP server. (Macro-enabled)

**port:** Specifies the port on which FTP server is running. Defaults to `21`. (Macro-enabled)

**userName:** Specifies the name of the user to be used while logging to FTP server. Defaults to `anonymous`.

**password:** Specifies the password to be used while logging to FTP server. Defaults to empty string.

**srcDirectory:** Specifies the directory on the FTP server which is to be copied. (Macro-enabled)

**destDirectory:** Specifies the destination directory to which files to be copied. If the directory does not exist,
it will be created. (Macro-enabled)

**extractZipFiles:** Boolean flag to determine whether zip files on the FTP server need to be extracted on
the destination while copying. Defaults to 'true'.
