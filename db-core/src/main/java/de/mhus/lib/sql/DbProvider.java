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

import de.mhus.lib.core.MActivator;
import de.mhus.lib.core.M;
import de.mhus.lib.core.config.HashConfig;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.core.lang.MObject;

/**
 * Provide the database connections and other db specific stuff. The default implementation is the
 * JdbcProvider. Implement this to create a completely new kind of database for the framework.
 *
 * @author mikehummel
 */
public abstract class DbProvider extends MObject {

    //	private static Log log = Log.getLog(DbProvider.class);

    protected IConfig config;
    protected MActivator activator;

    /**
     * Returns a new DbConnection for this kind of database.
     *
     * @return x
     * @throws Exception
     */
    public abstract InternalDbConnection createConnection() throws Exception;

    /**
     * Set configuration element and activator.
     *
     * @param config
     * @param activator
     */
    public void doInitialize(IConfig config, MActivator activator) {
        if (config == null) config = new HashConfig();
        if (activator == null) activator = M.l(MActivator.class);
        this.config = config;
        this.activator = activator;
    }

    /**
     * Returns the predefined statement by this name. TODO need to manipulate the set of statements
     * from outside.
     *
     * @param name
     * @return x The query string or null.
     */
    public String[] getQuery(String name) {
        IConfig query = config.getNode("queries");
        if (query == null) return new String[0];

        String queryLanguage = null;
        String queryString = query.getString(name, null);
        String[] out = new String[] {queryLanguage, queryString};

        if (queryString == null) {
            for (IConfig q : query.getNodes("query")) {
                if (q.getString("name", "").equals(name)) {
                    queryLanguage = q.getExtracted("language");
                    queryString = q.getExtracted("string");
                    out = new String[] {queryLanguage, queryString};
                    break;
                }
            }
        }
        return out;
    }

    /**
     * Returns the Dialect object for this database. It contains all deep specific abstraction
     * functions to handle the database.
     *
     * @return x
     */
    public abstract Dialect getDialect();

    /**
     * Nice name of the connection - from configuration to identify it.
     *
     * @return x
     */
    public String getName() {
        if (config == null) return "?";
        return config.getExtracted("name");
    }

    /**
     * Returns the used activator.
     *
     * @return x
     */
    public MActivator getActivator() {
        return activator;
    }
}
