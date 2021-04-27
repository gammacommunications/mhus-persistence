/**
 * Copyright (C) 2020 Mike Hummel (mh@mhus.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mhus.lib.jpa;

import java.util.Properties;

import de.mhus.lib.core.node.INode;
import de.mhus.lib.core.node.MNode;

public class JpaProperties extends Properties {

    protected JpaSchema schema;
    protected INode config;

    /** */
    private static final long serialVersionUID = 1L;

    public JpaProperties(INode config) {
        super();
        this.config = config;
        // fill from config
        INode cproperties = config.getObjectOrNull("properties");
        if (cproperties != null) {
            for (INode prop : cproperties.getObjectList("property")) {
                setProperty(prop.getExtracted("name"), prop.getExtracted("value"));
            }
        }
    }

    public JpaProperties() {
        super();
        config = new MNode();
    }

    public JpaProperties(Properties arg0) {
        super(arg0);
        config = new MNode();
    }

    public JpaSchema getSchema() {
        return schema;
    }

    public void setSchema(JpaSchema schema) {
        this.schema = schema;
    }

    public void configureTypes() {
        setProperty("openjpa.RuntimeUnenhancedClasses", "supported");

        StringBuilder types = null;
        for (Class<?> type : schema.getObjectTypes()) {
            if (types == null) {
                types = new StringBuilder();
            } else {
                types.append(";");
            }
            types.append(type.getCanonicalName());
        }
        put("openjpa.MetaDataFactory", "jpa(Types=" + types + ")");
        put("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
    }

    public INode getConfig() {
        return config;
    }
}
