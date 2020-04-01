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
package de.mhus.lib.cao.util;

import java.util.Collection;

import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoMetadata;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.CaoUtil;
import de.mhus.lib.cao.CaoWritableElement;
import de.mhus.lib.core.MCollection;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.NotSupportedException;

public abstract class PropertiesNode extends CaoNode {

    private static final long serialVersionUID = 1L;
    protected MProperties properties = new MProperties();
    protected String id;
    protected String name;

    public PropertiesNode(CaoCore core, CaoNode parent) {
        super(core, parent);
    }

    @Override
    public CaoWritableElement getWritableNode() throws MException {
        if (isEditable()) return new WritablePropertiesNode(core, this);
        return null;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Collection<String> getPropertyKeys() {
        return MCollection.toList(properties.keySet());
    }

    @Override
    public Object getProperty(String name) {
        return properties.getProperty(name);
    }

    @Override
    public boolean isProperty(String name) {
        return properties.containsKey(name);
    }

    @Override
    public void removeProperty(String key) {
        throw new NotSupportedException();
    }

    @Override
    public void setProperty(String key, Object value) {
        throw new NotSupportedException();
    }

    protected CaoMetadata createMetadataByProperties() {
        MutableMetadata metadata = new MutableMetadata(core.getDriver());
        for (java.util.Map.Entry<String, Object> entry : properties.entrySet()) {
            ((MutableMetadata) metadata)
                    .addDefinition(entry.getKey(), CaoUtil.objectToMetaType(entry.getValue()), 700);
        }
        return metadata;
    }

    protected abstract void doUpdate(MProperties modified) throws CaoException;
}
