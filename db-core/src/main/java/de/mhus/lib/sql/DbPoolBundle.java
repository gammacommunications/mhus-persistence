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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import de.mhus.lib.core.M;
import de.mhus.lib.core.MActivator;
import de.mhus.lib.core.MApi;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.core.lang.MObject;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.MRuntimeException;

/**
 * The class holds a bundle of different database pools.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DbPoolBundle extends MObject {

    private IConfig config;
    private MActivator activator;
    private Map<String, DbPool> bundle = new HashMap<String, DbPool>();

    /**
     * Create a new bundle from default configuration. Load it from MApi with the key of this class.
     */
    public DbPoolBundle() {
        this(null, null);
    }

    /**
     * Create a new Bundle from configuration.
     *
     * @param config Config element or null. null will use the central MApi configuration.
     * @param activator Activator or null. null will use the central MApi activator.
     */
    public DbPoolBundle(IConfig config, MActivator activator) {

        if (config == null) config = MApi.get().getCfgManager().getCfg(DbPoolBundle.class, null);
        if (activator == null) activator = M.l(MActivator.class);

        this.config = config;
        this.activator = activator;
    }

    /**
     * getPool.
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.sql.DbPool} object.
     * @throws java.lang.Exception if any.
     */
    public DbPool getPool(String name) throws Exception {

        if (bundle == null) throw new MException("Bundle already closed");

        synchronized (bundle) {
            DbPool pool = bundle.get(name);
            if (pool == null) {
                IConfig poolCon = config.getNode(name);
                if (poolCon != null) {
                    pool = new DefaultDbPool(poolCon, activator);
                    bundle.put(name, pool);
                } else {
                    throw new MException("pool config not found", name);
                }
            }
            return pool;
        }
    }

    /**
     * getNames.
     *
     * @return an array of {@link java.lang.String} objects.
     */
    public String[] getNames() {
        LinkedList<String> out = new LinkedList<String>();
        for (IConfig c : config.getNodes()) {
            try {
                out.add(c.getName());
            } catch (MException e) {
                throw new MRuntimeException(e);
            }
        }
        return out.toArray(new String[out.size()]);
    }

    /**
     * Getter for the field <code>config</code>.
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.core.directory.ResourceNode} object.
     */
    public IConfig getConfig(String name) {
        return config.getNode(name);
    }

    /**
     * Getter for the field <code>config</code>.
     *
     * @return a {@link de.mhus.lib.core.directory.ResourceNode} object.
     */
    public IConfig getConfig() {
        return config;
    }

    /** reset. */
    public void reset() {
        bundle = new HashMap<String, DbPool>();
    }

    /** close. */
    public void close() {

        if (bundle == null) return;

        synchronized (bundle) {
            for (DbPool pool : bundle.values()) pool.close();
            bundle = null;
        }
    }
}
