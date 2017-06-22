# FTP put action

Copy files to FTP server from a specified destination.

## Usage Notes

Common use case is to upload file(s) from local filesystem or HDFS to a FTP server. File Regex filtering can be used
to copy only the file(s) that are of interest.

## Plugin Configuration

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Host** | **Y** | N/A | Specifies the host name of the FTP server. (Macro-enabled)
| **Port** | **N** | 21 | Specifies the port on which FTP server is running. (Macro-enabled)
| **User Name** | **N** | anonymous | Specifies the name of the user to be used while logging to FTP server.
| **Source Path** | **Y** | N/A | Specifies the directory/files on the file system which needs to be copied. (Macro-enabled)
| **Destination Directory** | **Y** | N/A | Specifies the destination directory on FTP server. If the directory does not exist, it will be created. (Macro-enabled)
