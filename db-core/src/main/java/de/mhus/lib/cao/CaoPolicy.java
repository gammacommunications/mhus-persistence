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
package de.mhus.lib.cao;

import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.mhus.lib.cao.CaoMetaDefinition.TYPE;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.util.EmptyList;
import de.mhus.lib.errors.MException;

// TODO need to be rewritten as aspect

public class CaoPolicy extends CaoNode implements CaoAspect {

    private static final long serialVersionUID = 1L;
    public static final String READ = "read";
    public static final String WRITE = "write";
    public static final String POLICIES = "policies";
    public static final String PRINCIPAL = "principal";

    public static final String CATEGORY_POLICY = "policy";
    public static final String CATEGORY_RIGHT = "right";

    protected boolean writable;
    protected boolean readable;
    protected CaoNode element;
    protected CaoPolicy proxy = null;
    protected CaoMetadata meta;

    public CaoPolicy(CaoCore core, CaoNode element, boolean readable, boolean writable)
            throws MException {
        super(core, element);
        this.element = element;
        this.readable = readable;
        this.writable = writable;
        meta = new CaoMetadata(getConnection().getDriver());
        fillMetaData(meta.definition);
    }

    /**
     * Overwrite this function to append or set your own right definitions.
     *
     * @param definition
     */
    protected void fillMetaData(LinkedList<CaoMetaDefinition> definition) {
        definition.add(new CaoMetaDefinition(meta, READ, TYPE.BOOLEAN, null, 0, CATEGORY_RIGHT));
        definition.add(new CaoMetaDefinition(meta, WRITE, TYPE.BOOLEAN, null, 0, CATEGORY_RIGHT));
        definition.add(new CaoMetaDefinition(meta, POLICIES, TYPE.LIST, null, 0, CATEGORY_POLICY));
        definition.add(new CaoMetaDefinition(meta, PRINCIPAL, TYPE.ELEMENT, null, 0));
    }

    public boolean isWritable() {
        if (proxy != null) return proxy.isWritable();
        return getBoolean(WRITE, writable);
    }

    public boolean isReadable() {
        if (proxy != null) return proxy.isReadable();
        return getBoolean(READ, readable);
    }

    @Override
    public boolean isNode() {
        if (proxy != null) return proxy.isNode();
        return false;
    }

    @Override
    public String getId() {
        if (proxy != null) return proxy.getId();
        return null;
    }

    @Override
    public String getName() {
        if (proxy != null) return proxy.getName();
        return null;
    }

    public CaoMetadata getMetadata() {
        if (proxy != null) return proxy.getMetadata();
        return meta;
    }

    @Override
    public CaoNode getParent() {
        if (proxy != null) return proxy.getParent();
        return null;
    }

    //	@Override
    //	public String getString(String name) throws CaoException {
    //		if (proxy != null) return proxy.getString(name);
    //		if (name.equals(PRINCIPAL)) {
    //			return getConnection().getCurrentUser().getName();
    //		}
    //		return MCast.toString(getBoolean(name, false));
    //	}

    @Override
    public boolean getBoolean(String name, boolean def) {
        if (proxy != null) return proxy.getBoolean(name, def);
        if (READ.equals(name)) return readable;
        if (WRITE.equals(name)) return writable;
        return def;
    }

    //	@Override
    //	public CaoList getList(String name, CaoAccess access, String... attributes)
    //			throws CaoException {
    //		if (proxy != null) return proxy.getList(name, access, attributes);
    //
    //		if (POLICIES.equals(name)) {
    //			return getPoliciesList(attributes);
    //		}
    //
    //		throw new CaoNotFoundException(this,"list",name);
    //	}

    /**
     * Overwrite this function to return another policy informations.
     *
     * @param attributes
     * @return
     * @throws CaoException
     */
    protected CaoList getPoliciesList(String[] attributes) throws MException {
        // Returns a list with the current user
        CaoList list = new CaoList(this);
        list.add(this);
        return list;
    }

    //	@Override
    //	public Object getObject(String name, String... attributes)
    //			throws CaoException {
    //		if (proxy != null) return proxy.getObject(name, attributes);
    //
    //		if (name.equals(PRINCIPAL)) {
    //			return getConnection().getCurrentUser();
    //		}
    //
    //		throw new CaoNotSupportedException();
    //	}

    @Override
    public CaoWritableElement getWritableNode() throws MException {
        if (proxy != null) return proxy.getWritableNode();
        throw new CaoNotSupportedException();
    }

    @Override
    public void reload() throws MException {
        //	proxy = element.getAccessPolicy();
    }

    @Override
    public boolean isValid() {
        return element.isValid();
    }

    @Override
    public Set<String> keys() {
        return element.keys();
    }

    @Override
    public Collection<String> getPropertyKeys() {
        return new EmptyList<>();
    }

    @Override
    public CaoNode getNode(String key) {
        return null;
    }

    @Override
    public List<CaoNode> getNodes() {
        return new LinkedList<>();
    }

    @Override
    public List<CaoNode> getNodes(String key) {
        return new LinkedList<>();
    }

    @Override
    public Collection<String> getNodeKeys() {
        return new EmptyList<>();
    }

    @Override
    public InputStream getInputStream(String key) {
        return null;
    }

    @Override
    public Object getProperty(String name) {
        return null;
    }

    @Override
    public boolean isProperty(String name) {
        return false;
    }

    @Override
    public void removeProperty(String key) {}

    @Override
    public void setProperty(String key, Object value) {}

    @Override
    public URL getUrl() {
        return null;
    }

    @Override
    public boolean hasContent() {
        return false;
    }

    @Override
    public Collection<String> getRenditions() {
        return null;
    }

    @Override
    public void clear() {}

    @Override
    public String getPath() {
        return element.getPath();
    }

    @Override
    public Collection<String> getPaths() {
        return element.getPaths();
    }

    @Override
    public IProperties getRenditionProperties(String rendition) {
        return null;
    }
}
