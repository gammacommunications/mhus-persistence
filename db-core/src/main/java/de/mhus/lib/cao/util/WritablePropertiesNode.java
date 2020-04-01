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

import java.io.InputStream;
import java.net.URL;
import java.util.Collection;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoActionStarter;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoList;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.CaoWritableElement;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.MCollection;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.core.strategy.Monitor;
import de.mhus.lib.core.strategy.NotSuccessful;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.core.strategy.Successful;
import de.mhus.lib.core.util.MNls;
import de.mhus.lib.errors.MException;

public class WritablePropertiesNode extends CaoWritableElement {

    private static final long serialVersionUID = 1L;
    protected MProperties properties = new MProperties();

    public WritablePropertiesNode(CaoCore con, PropertiesNode parent) throws CaoException {
        super(con, parent);
        reload();
    }

    @Override
    public CaoNode getNode(String key) {
        return getOriginalElement().getNode(key);
    }

    @Override
    public Collection<CaoNode> getNodes() {
        return getOriginalElement().getNodes();
    }

    @Override
    public Collection<CaoNode> getNodes(String key) {
        return getOriginalElement().getNodes(key);
    }

    @Override
    public Collection<String> getNodeKeys() {
        return getOriginalElement().getNodeKeys();
    }

    @Override
    public String getName() {
        return getOriginalElement().getName();
    }

    @Override
    public InputStream getInputStream(String rendition) {
        return null;
    }

    @Override
    public URL getUrl() {
        return getOriginalElement().getUrl();
    }

    @Override
    public boolean isValid() {
        return getOriginalElement().isValid();
    }

    @Override
    public boolean hasContent() {
        return getOriginalElement().hasContent();
    }

    @Override
    public void removeProperty(String key) {
        properties.remove(key);
    }

    @Override
    public void setProperty(String key, Object value) {
        properties.setProperty(key, value);
    }

    @Override
    public boolean isEditable() {
        return true;
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
    public CaoActionStarter getUpdateAction() throws CaoException {
        try {
            CaoAction action =
                    getOriginalElement().getConnection().getActions().getAction(CaoAction.UPDATE);
            CaoList list = new CaoList(null);
            list.add(this);
            CaoConfiguration config = action.createConfiguration(list, null);
            return new CaoActionStarter(action, config);
        } catch (Throwable t) {
        }
        return new SaveThis();
    }

    private class SaveThis extends CaoActionStarter {

        public SaveThis() {
            super(null, null);
        }

        @Override
        public String getName() {
            return CaoAction.UPDATE;
        }

        @Override
        public boolean canExecute() {
            return true;
        }

        @Override
        public OperationResult doExecute(Monitor monitor) throws CaoException {
            try {
                ((PropertiesNode) getOriginalElement())
                        .doUpdate(WritablePropertiesNode.this.properties);
                return new Successful(CaoAction.UPDATE, "ok", 0);
            } catch (Throwable e) {
                return new NotSuccessful(CaoAction.UPDATE, e.toString(), -1);
            }
        }

        @Override
        public MNls getResourceBundle() {
            return null;
        }
    }

    @Override
    public CaoWritableElement getWritableNode() throws MException {
        return null;
    }

    @Override
    public String getId() {
        return getOriginalElement().getId();
    }

    @Override
    public boolean isNode() {
        return getOriginalElement().isNode();
    }

    @Override
    public void reload() throws CaoException {
        properties.clear();
        for (java.util.Map.Entry<String, Object> entry : getOriginalElement().entrySet())
            properties.put(entry.getKey(), entry.getValue());
    }

    @Override
    public Collection<String> getRenditions() {
        return getOriginalElement().getRenditions();
    }

    @Override
    public void clear() {
        properties.clear();
    }

    @Override
    public String getPath() {
        return getParent().getPath();
    }

    @Override
    public Collection<String> getPaths() {
        return getParent().getPaths();
    }

    @Override
    public IProperties getRenditionProperties(String rendition) {
        return getParent().getRenditionProperties(rendition);
    }
}
