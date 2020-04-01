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
import de.mhus.lib.core.config.IConfig;

public class PseudoDbPool extends DbPool {

    private boolean closed;

    public PseudoDbPool() throws Exception {
        super(null, null);
    }

    /**
     * Create a new pool from a configuration.
     *
     * @param config Config element or null. null will use the central MApi configuration.
     * @param activator Activator or null. null will use the central MApi Activator.
     * @throws Exception
     */
    public PseudoDbPool(IConfig config, MActivator activator) throws Exception {
        super(config, activator);
    }

    /**
     * Create a pool with the DbProvider.
     *
     * @param provider
     */
    public PseudoDbPool(DbProvider provider) {
        super(provider);
    }

    @Override
    public DbConnection getConnection() throws Exception {
        if (closed) throw new Exception("Pool is closed");
        try {
            InternalDbConnection out = getProvider().createConnection();
            out.setPool(this);
            out.setUsed(true);
            return out;
        } catch (Exception e) {
            // special behavior for e.g. mysql, retry to get a connection after gc()
            // Caused by: com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException: Too
            // many connections
            if (e.getMessage().indexOf("Too many connections") > -1) {
                printStackTrace();
            }
            throw e;
        }
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public int getUsedSize() {
        return 0;
    }

    @Override
    public void cleanup(boolean unusedAlso) {}

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public String dumpUsage(boolean used) {
        return "?";
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
