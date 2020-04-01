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
package de.mhus.lib.cao.auth;

import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import de.mhus.lib.basics.ReadOnly;
import de.mhus.lib.cao.CaoAspect;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.CaoWritableElement;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.logging.Log;
import de.mhus.lib.core.util.MapEntry;
import de.mhus.lib.errors.AccessDeniedException;
import de.mhus.lib.errors.MException;
import de.mhus.lib.errors.NotSupportedException;

public class AuthNode extends CaoNode {

    private static final long serialVersionUID = 1L;
    protected CaoNode instance;
    AuthAccess access;

    public AuthNode(AuthCore connection, AuthNode parent, CaoNode instance) {
        super(connection, parent);
        this.instance = instance;
    }

    public AuthNode(AuthCore connection, CaoNode instance) {
        super(connection, null);
        this.instance = instance;
    }

    @Override
    public Log log() {
        return instance.log();
    }

    @Override
    public Object getProperty(String name) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return null;
        return instance.getProperty(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public String getString(String name, String def) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return def;
        return instance.getString(((AuthCore) core).mapReadName(instance, name), def);
    }

    @Override
    public Collection<String> getPropertyKeys() {
        Collection<String> ret = instance.getPropertyKeys();
        if (ret instanceof ReadOnly) ret = new LinkedList<>(ret);

        for (Iterator<String> iter = ret.iterator(); iter.hasNext(); ) {
            String next = iter.next();
            if (!((AuthCore) core).hasReadAccess(instance, next)) iter.remove();
        }

        return ((AuthCore) core).mapReadNames(this, ret);
    }

    @Override
    public CaoWritableElement getWritableNode() throws MException {
        if (!((AuthCore) core).hasWriteAccess(instance)) return null;
        return new AuthWritableNode((AuthCore) core, this, instance, instance.getWritableNode());
    }

    @Override
    public CaoNode getNode(String key) {
        CaoNode n = instance.getNode(key);
        if (n == null || !((AuthCore) core).hasReadAccess(n)) return null;
        return new AuthNode((AuthCore) core, this, n);
    }

    @Override
    public String getString(String name) throws MException {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return null;
        return instance.getString(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public String getId() {
        return instance.getId();
    }

    @Override
    public Collection<CaoNode> getNodes() {
        Collection<CaoNode> in = instance.getNodes();
        LinkedList<CaoNode> out = new LinkedList<>();
        for (CaoNode n : in) {
            if (((AuthCore) core).hasReadAccess(n)) out.add(new AuthNode((AuthCore) core, this, n));
        }
        return out;
    }

    @Override
    public boolean getBoolean(String name, boolean def) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return def;
        return instance.getBoolean(((AuthCore) core).mapReadName(instance, name), def);
    }

    @Override
    public int hashCode() {
        return instance.hashCode();
    }

    @Override
    public String getName() {
        return instance.getName();
    }

    @Override
    public Collection<CaoNode> getNodes(String key) {
        Collection<CaoNode> in = instance.getNodes(key);
        LinkedList<CaoNode> out = new LinkedList<>();
        for (CaoNode n : in) {
            if (((AuthCore) core).hasReadAccess(n)) out.add(new AuthNode((AuthCore) core, this, n));
        }
        return out;
    }

    @Override
    public boolean getBoolean(String name) throws MException {
        if (!((AuthCore) core).hasReadAccess(instance, name)) throw new AccessDeniedException(name);
        return instance.getBoolean(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public boolean isNode() {
        return instance.isNode();
    }

    @Override
    public Spliterator<java.util.Map.Entry<String, Object>> spliterator() {
        //		return instance.spliterator();
        throw new NotSupportedException();
    }

    @Override
    public Collection<String> getNodeKeys() {
        HashSet<String> out = new HashSet<>();
        for (CaoNode n : getNodes()) out.add(n.getName());
        return out;
    }

    @Override
    public int getInt(String name, int def) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return def;
        return instance.getInt(((AuthCore) core).mapReadName(instance, name), def);
    }

    @Override
    public void reload() throws MException {
        instance.reload();
    }

    @Override
    public long getLong(String name, long def) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return def;
        return instance.getLong(((AuthCore) core).mapReadName(instance, name), def);
    }

    @Override
    public InputStream getInputStream() {
        if (!((AuthCore) core).hasContentAccess(instance, null)) return null;
        return instance.getInputStream();
    }

    @Override
    public float getFloat(String name, float def) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return def;
        return instance.getFloat(((AuthCore) core).mapReadName(instance, name), def);
    }

    @Override
    public String toString() {
        return instance.toString();
    }

    @Override
    public InputStream getInputStream(String rendition) {
        if (!((AuthCore) core).hasContentAccess(instance, rendition)) return null;
        return instance.getInputStream(((AuthCore) core).mapReadRendition(instance, rendition));
    }

    @Override
    public double getDouble(String name, double def) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return def;
        return instance.getDouble(((AuthCore) core).mapReadName(instance, name), def);
    }

    @Override
    public boolean isValid() {
        return instance.isValid();
    }

    @Override
    public String getExtracted(String key) {
        if (!((AuthCore) core).hasReadAccess(instance, key)) throw new AccessDeniedException();
        return instance.getExtracted(((AuthCore) core).mapReadName(instance, key));
    }

    @Override
    public Calendar getCalendar(String name) throws MException {
        if (!((AuthCore) core).hasReadAccess(instance, name)) throw new AccessDeniedException();
        return instance.getCalendar(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public boolean equals(Object other) {
        return instance.equals(other);
    }

    @Override
    public Date getDate(String name) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return null;
        return instance.getDate(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public void setString(String name, String value) {}

    @Override
    public String getExtracted(String key, String def) {
        if (!((AuthCore) core).hasReadAccess(instance, key)) return def;
        return instance.getExtracted(((AuthCore) core).mapReadName(instance, key), def);
    }

    @Override
    public void setInt(String name, int value) {}

    @Override
    public boolean isEditable() {
        if (!((AuthCore) core).hasWriteAccess(instance)) return false;
        return instance.isEditable();
    }

    @Override
    public void setLong(String name, long value) {}

    @Override
    public void setDouble(String name, double value) {}

    @Override
    public Set<String> keys() {
        return instance.keys();
    }

    @Override
    public void setFloat(String name, float value) {}

    @Override
    public void setBoolean(String name, boolean value) {}

    @SuppressWarnings("unchecked")
    @Override
    public <T> T adaptTo(Class<? extends T> ifc) {
        // overlay local aspects - these are allowed
        T ret = super.adaptTo(ifc);
        if (ret != null) return ret;
        // use original if access is granted
        if (!((AuthCore) core).hasAspectAccess(instance, (Class<? extends CaoAspect>) ifc))
            return null;
        return instance.adaptTo(ifc);
    }

    @Override
    public void setCalendar(String name, Calendar value) {}

    @Override
    public void setDate(String name, Date value) {}

    @Override
    public void setNumber(String name, Number value) {}

    @Override
    public Number getNumber(String name, Number def) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return def;
        return instance.getNumber(((AuthCore) core).mapReadName(instance, name), def);
    }

    @Override
    public boolean isProperty(String name) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return false;
        return instance.isProperty(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public void removeProperty(String key) {}

    @Override
    public void setProperty(String key, Object value) {}

    @Override
    public Iterator<java.util.Map.Entry<String, Object>> iterator() {
        return instance.iterator();
    }

    @Override
    public Map<String, Object> toMap() {
        throw new NotSupportedException();
    }

    @Override
    public boolean isEmpty() {
        return instance.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        if (!((AuthCore) core).hasReadAccess(instance, String.valueOf(key))) return false;
        return instance.containsKey(((AuthCore) core).mapReadName(instance, String.valueOf(key)));
    }

    @Override
    public Object get(Object key) {
        if (!((AuthCore) core).hasReadAccess(instance, String.valueOf(key))) return null;
        return instance.get(((AuthCore) core).mapReadName(instance, String.valueOf(key)));
    }

    @Override
    public Object put(String key, Object value) {
        return null;
    }

    @Override
    public Object remove(Object key) {
        return null;
    }

    @Override
    public URL getUrl() {
        return instance.getUrl();
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {}

    @Override
    public boolean hasContent() {
        return instance.hasContent();
    }

    @Override
    public CaoNode getNodeByPath(String path) {
        CaoNode n = instance.getNodeByPath(path);
        if (!((AuthCore) core).hasReadAccess(n)) return null;
        return new AuthNode((AuthCore) core, n);
    }

    @Override
    public void clear() {}

    @Override
    public Set<String> keySet() {
        return instance.keySet();
    }

    @Override
    public String dump() throws MException {
        //		return instance.dump();
        return "";
    }

    @Override
    public int size() {
        return instance.size();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new NotSupportedException();
    }

    @Override
    public Collection<Object> values() {
        throw new NotSupportedException();
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        HashSet<java.util.Map.Entry<String, Object>> ret = new HashSet<>();
        for (String name : getPropertyKeys())
            ret.add(new MapEntry<String, Object>(name, get(name)));
        return ret;
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        Object ret = get(key);
        if (ret == null) return defaultValue;
        return ret;
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super Object> action) {
        for (String name : getPropertyKeys())
            if (((AuthCore) core).hasReadAccess(instance, name)) action.accept(name, get(name));
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ? extends Object> function) {
        throw new NotSupportedException();
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        throw new NotSupportedException();
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new NotSupportedException();
    }

    @Override
    public boolean replace(String key, Object oldValue, Object newValue) {
        throw new NotSupportedException();
    }

    @Override
    public Object replace(String key, Object value) {
        throw new NotSupportedException();
    }

    @Override
    public Object computeIfAbsent(
            String key, Function<? super String, ? extends Object> mappingFunction) {
        throw new NotSupportedException();
    }

    @Override
    public Object computeIfPresent(
            String key,
            BiFunction<? super String, ? super Object, ? extends Object> remappingFunction) {
        throw new NotSupportedException();
    }

    @Override
    public Object compute(
            String key,
            BiFunction<? super String, ? super Object, ? extends Object> remappingFunction) {
        throw new NotSupportedException();
    }

    @Override
    public Object merge(
            String key,
            Object value,
            BiFunction<? super Object, ? super Object, ? extends Object> remappingFunction) {
        throw new NotSupportedException();
    }

    @Override
    public Collection<String> getRenditions() {
        return instance.getRenditions();
    }

    @Override
    public CaoNode getParent() {
        CaoNode p = super.getParent();
        if (p == null) {
            CaoNode ip = instance.getParent();
            if (ip == null) return null;
            parent = new AuthNode((AuthCore) core, ip);
            p = parent;
        }
        return p;
    }

    @Override
    public String getPath() {
        return instance.getPath();
    }

    @Override
    public Collection<String> getPaths() {
        return instance.getPaths();
    }

    @Override
    public IProperties getRenditionProperties(String rendition) {
        if (!((AuthCore) core).hasContentAccess(instance, rendition)) return null;
        return instance.getRenditionProperties(rendition);
    }

    @Override
    public String getParentId() {
        return instance.getParentId();
    }
}
