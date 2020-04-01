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
package de.mhus.lib.adb.query;

import java.util.Date;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.model.Field;
import de.mhus.lib.adb.model.Table;
import de.mhus.lib.core.MConstants;
import de.mhus.lib.core.parser.AttributeMap;

/**
 * ADynValue class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class ADynValue extends AAttribute {

    private String name;
    private Object value;
    private Class<?> type;
    private String field;

    /**
     * Constructor for ADynValue.
     *
     * @param value a {@link java.lang.Object} object.
     */
    //	public ADynValue(Object value) {
    //		this(null, value);
    //	}

    /**
     * Constructor for ADynValue.
     *
     * @param type
     * @param field
     * @param name a {@link java.lang.String} object.
     * @param value a {@link java.lang.Object} object.
     */
    public ADynValue(Class<?> type, String field, String name, Object value) {
        this.type = type;
        this.name = name;
        this.field = field;
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override
    public void getAttributes(AQuery<?> query, AttributeMap map) {
        if (name == null) name = "v" + query.nextUnique();
        map.put(name, value);
    }

    /**
     * Getter for the field <code>value</code>.
     *
     * @return a {@link java.lang.Object} object.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Setter for the field <code>value</code>.
     *
     * @param value a {@link java.lang.Object} object.
     */
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "dyn:[" + value + "]";
    }

    public String getName() {
        return name;
    }

    public Class<?> getType() {
        return type;
    }

    public String getField() {
        return field;
    }

    public String getDefinition(DbManager manager) {
        String t = null;
        if (manager != null && type != null && field != null) {
            String regName = manager.getRegistryName(type);
            if (regName != null) {
                Table table = manager.getTable(regName);
                if (table != null) {
                    Field field = table.getField(this.field);
                    if (field != null) {
                        Class<?> fType = field.getType();
                        if (Date.class.isAssignableFrom(fType)) t = MConstants.TYPE_DATE;
                        else if (String.class.isAssignableFrom(fType)) t = MConstants.TYPE_TEXT;
                        else if (int.class == fType
                                || short.class == fType
                                || byte.class == fType
                                || Integer.class.isAssignableFrom(fType)
                                || Short.class.isAssignableFrom(fType)
                                || Byte.class.isAssignableFrom(fType)) t = MConstants.TYPE_INT;
                        else if (long.class == fType
                                || char.class == fType
                                || Long.class.isAssignableFrom(fType)
                                || Character.class.isAssignableFrom(fType))
                            t = MConstants.TYPE_LONG;
                        else if (double.class == fType || Double.class.isAssignableFrom(fType))
                            t = MConstants.TYPE_DOUBLE;
                        else if (float.class == fType || Float.class.isAssignableFrom(fType))
                            t = MConstants.TYPE_FLOAT;
                    }
                }
            }
        }
        if (t != null) {
            return getName() + "," + t;
        }
        return getName();
    }
}
