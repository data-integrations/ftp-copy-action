# FTP Copy Plugins

[![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)](https://docs.cdap.io/cdap/current/en/integrations/cask-market.html)
![cdap-action](https://cdap-users.herokuapp.com/assets/cdap-action.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

FTP Copy Plugins contain the following two action plugins:
* [FTP Copy](docs/FTPCopy-action.md) - Plugin for copying file(s) from FTP locations.
* [FTP Put](docs/FTPPut-action.md) - Plugin for uploading file(s) to FTP server.

## Build

To build your plugins:

    mvn clean package
    
You can also build without running tests: 

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be uploaded to CDAP to deploy your plugins.

## UI Integration

The CDAP Pipelines UI displays each plugin property as a simple textbox. To customize how the plugin properties
are displayed in the UI, you can place a configuration file in the ``widgets`` directory.
The file must be named following a convention of ``[plugin-name]-[plugin-type].json``.

See [Plugin Widget Configuration](http://docs.cdap.io/cdap/current/en/hydrator-manual/developing-plugins/packaging-plugins.html#plugin-widget-json)
for details on the configuration file.

The UI will also display a reference doc for your plugin if you place a file in the ``docs`` directory
that follows the convention of ``[plugin-name]-[plugin-type].md``.

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json file under the ``target`` directory. This file is deployed along with your .jar file to add your
plugins to CDAP.

## Deployment

You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, if your artifact is named 'example-plugins-1.0.0':

    > load artifact target/example-plugins-1.0.0.jar config-file target/example-plugins-1.0.0.json
    

## Need Help?

### Mailing Lists

CDAP User Group and Development Discussions:

- [cdap-user](https://groups.google.com/d/forum/cdap-user)

The *cdap-user* mailing list is primarily for users using the product to develop
applications. You can expect questions from users, release announcements, and any other
discussions that we think will be helpful to the users.

- [cdap-dev](https://groups.google.com/d/forum/cdap-dev)

The *cdap-dev* mailing list is essentially for developers actively working
on the product, and should be used for all our design, architecture and technical
discussions moving forward. This mailing list will also receive all JIRA and GitHub
notifications.

### IRC Channel

CDAP IRC Channel: #cdap on [irc.freenode.net](http://irc.freenode.net)


## License and Trademarks

Copyright © 2015-2019 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
