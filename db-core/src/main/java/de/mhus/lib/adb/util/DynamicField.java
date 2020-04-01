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

import de.mhus.lib.adb.DbDynamic;
import de.mhus.lib.core.config.HashConfig;
import de.mhus.lib.core.directory.ResourceNode;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.MRuntimeException;

/**
 * DynamicField class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class DynamicField implements DbDynamic.Field {

    /** Constant <code>PRIMARY_KEY="primary_key"</code> */
    public static final String PRIMARY_KEY = "primary_key";
    /** Constant <code>INDEXES="indexes"</code> */
    public static final String INDEXES = "indexes";

    private String name;
    private boolean isPrimaryKey;
    private Class<?> ret;
    private ResourceNode<?> attributes;
    private boolean persistent = true;
    private boolean readOnly;

    /** Constructor for DynamicField. */
    public DynamicField() {}

    /**
     * Constructor for DynamicField.
     *
     * @param name a {@link java.lang.String} object.
     * @param ret a {@link java.lang.Class} object.
     * @param attributes a {@link java.lang.String} object.
     */
    public DynamicField(String name, Class<?> ret, String... attributes) {
        setName(name);
        setRet(ret);
        HashConfig x = new HashConfig();
        for (int i = 0; i < attributes.length; i += 2) {
            try {
                x.setString(attributes[i], attributes[i + 1]);
            } catch (MRuntimeException e) {
            }
        }
        setAttributes(x);
        setPrimaryKey(x.getBoolean(PRIMARY_KEY, false));
    }

    /**
     * Setter for the field <code>name</code>.
     *
     * @param name a {@link java.lang.String} object.
     */
    public void setName(String name) {
        this.name = name;
    }
    /** {@inheritDoc} */
    @Override
    public String getName() {
        return name;
    }
    /**
     * setPrimaryKey.
     *
     * @param isPrimaryKey a boolean.
     */
    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }
    /** {@inheritDoc} */
    @Override
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
    /**
     * Setter for the field <code>ret</code>.
     *
     * @param ret a {@link java.lang.Class} object.
     */
    public void setRet(Class<?> ret) {
        this.ret = ret;
    }
    /** {@inheritDoc} */
    @Override
    public Class<?> getReturnType() {
        return ret;
    }
    /**
     * Setter for the field <code>attributes</code>.
     *
     * @param attributes a {@link de.mhus.lib.core.directory.ResourceNode} object.
     */
    public void setAttributes(ResourceNode<?> attributes) {
        this.attributes = attributes;
    }
    /** {@inheritDoc} */
    @Override
    public ResourceNode<?> getAttributes() {
        return attributes;
    }

    /** {@inheritDoc} */
    @Override
    public String[] getIndexes() throws MException {
        String v = attributes.getString(INDEXES, null);
        return v == null ? null : v.split(",");
    }

    /**
     * Setter for the field <code>persistent</code>.
     *
     * @param in a boolean.
     */
    public void setPersistent(boolean in) {
        persistent = in;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isPersistent() {
        return persistent;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Setter for the field <code>readOnly</code>.
     *
     * @param ro a boolean.
     */
    public void setReadOnly(boolean ro) {
        this.readOnly = ro;
    }
}
