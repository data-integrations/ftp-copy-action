# FTP copy action

Copy files from FTP server to the specified destination.

## Usage Notes

Common use case is to have files uploaded on the FTP server in compressed format. These files can then be accessed
over the network using FTP by various applications. This plugin targets use case where files can be downloaded
from FTP server in an uncompressed format and stored on the desired destination such as `HDFS`.

## Plugin Configuration

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Host** | **Y** | N/A | Specifies the host name of the FTP server. (Macro-enabled)
| **Port** | **N** | 21 | Specifies the port on which FTP server is running. (Macro-enabled)
| **User Name** | **N** | anonymous | Specifies the name of the user to be used while logging to FTP server.
| **Source Directory** | **Y** | N/A | Specifies the directory on the FTP server which is to be copied. (Macro-enabled)
| **Destination Directory** | **Y** | N/A | Specifies the destination directory on HDFS to which files to be copied. If the directory does not exist, it will be created. (Macro-enabled)
| **Unzip files** | **N** | true | Boolean flag to determine whether zip files on the FTP server need to be extracted on the destination while copying.
