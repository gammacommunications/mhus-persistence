/**
 * Copyright 2018 Mike Hummel
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.sql;

import java.util.UUID;

import de.mhus.lib.core.MActivator;
import de.mhus.lib.core.M;
import de.mhus.lib.core.config.NodeConfig;

/**
 * HsqlDbProvider class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class HsqlDbProvider extends JdbcProvider {

    /** Constructor for HsqlDbProvider. */
    public HsqlDbProvider() {
        this(UUID.randomUUID().toString());
    }

    /**
     * Constructor for HsqlDbProvider.
     *
     * @param memoryDbName a {@link java.lang.String} object.
     */
    public HsqlDbProvider(String memoryDbName) {
        config = new NodeConfig();
        config.setProperty("driver", "org.hsqldb.jdbcDriver");
        config.setProperty("url", "jdbc:hsqldb:mem:" + memoryDbName);
        config.setProperty("user", "sa");
        config.setProperty("pass", "");
        config.setProperty("name", memoryDbName);
        activator = M.l(MActivator.class);
    }

    /**
     * Constructor for HsqlDbProvider.
     *
     * @param file a {@link java.lang.String} object.
     * @param user a {@link java.lang.String} object.
     * @param pass a {@link java.lang.String} object.
     */
    public HsqlDbProvider(String file, String user, String pass) {
        config = new NodeConfig();
        config.setProperty("driver", "org.hsqldb.jdbcDriver");
        config.setProperty("url", "file:" + file);
        config.setProperty("user", user);
        config.setProperty("pass", pass);
        config.setProperty("name", file);
        activator = M.l(MActivator.class);
    }
}
