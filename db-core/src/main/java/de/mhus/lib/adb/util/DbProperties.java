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
package de.mhus.lib.adb.util;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.errors.MException;

/**
 * A simple key - value properties implementation.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DbProperties {

    private DbManager manager;
    private String registryName;

    @SuppressWarnings("unused")
    private String dump;

    /**
     * Constructor for DbProperties.
     *
     * @param manager a {@link de.mhus.lib.adb.DbManager} object.
     * @param registryName a {@link java.lang.String} object.
     */
    public DbProperties(DbManager manager, String registryName) {
        this.manager = manager;
        this.registryName = registryName;
    }

    /**
     * get.
     *
     * @param key a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    public String get(String key) throws MException {
        Property prop = (Property) manager.getObject(registryName, key);
        if (prop == null) return null;
        return prop.getValue();
    }

    /**
     * set.
     *
     * @param key a {@link java.lang.String} object.
     * @param value a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    public String set(String key, String value) throws MException {
        Property prop = (Property) manager.getObject(registryName, key);
        if (prop == null) {
            prop = new Property();
            prop.setKey(key);
            prop.setValue(value);
            manager.createObject(registryName, prop);
            return null;
        }
        String oldValue = prop.getValue();
        prop.setValue(value);
        manager.saveObject(registryName, prop);
        dump = null;
        return oldValue;
    }

    /**
     * remove.
     *
     * @param key a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     * @throws de.mhus.lib.errors.MException if any.
     */
    public String remove(String key) throws MException {
        Property prop = (Property) manager.getObject(registryName, key);
        if (prop == null) {
            return null;
        }
        String oldValue = prop.getValue();
        manager.deleteObject(registryName, prop);
        dump = null;
        return oldValue;
    }

    //	public String toString() {
    //		if (dump == null) {
    //			try {
    //				TreeMap<String, String> tree = new TreeMap<String, String>();
    //				DbCollection<Property> res = manager.getByQualification(null, new Property(),
    // registryName, "", null);
    //				for (Property p : res) tree.put(p.getKey(), p.getValue());
    //				res.close();
    //				dump = tree.toString() + "@DbProperties";
    //			} catch (MException e) {
    //				dump = e.toString() + "@DbProperties";
    //			}
    //		}
    //		return dump;
    //	}
}
