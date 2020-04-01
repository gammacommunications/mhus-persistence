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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import de.mhus.lib.cao.CaoActionStarter;
import de.mhus.lib.cao.CaoConnection;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.CaoWritableElement;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.IReadProperties;
import de.mhus.lib.core.logging.Log;
import de.mhus.lib.core.util.MNls;
import de.mhus.lib.errors.MException;

public class WritableNode extends CaoWritableElement {

    private static final long serialVersionUID = 1L;
    private CaoNode node;
    private CaoNode current;

    public WritableNode(CaoNode node) {
        super(null, node.getParent());
        this.node = node;
        this.current = node;
    }

    protected synchronized void doWritable() {
        if (isChanged()) return;
        try {
            current = node.getWritableNode();
        } catch (MException e) {
            log().e("Can't edit node", e);
        }
    }

    public boolean isChanged() {
        return current instanceof CaoWritableElement;
    }

    public void save() throws MException {
        if (!isChanged()) return;
        CaoActionStarter action = ((CaoWritableElement) current).getUpdateAction();
        action.doExecute(null);
        current = node;
    }

    @Override
    public Log log() {
        return current.log();
    }

    @Override
    public String nls(String text) {
        return current.nls(text);
    }

    @Override
    public Object getProperty(String name) {
        return current.getProperty(name);
    }

    @Override
    public MNls getNls() {
        return current.getNls();
    }

    @Override
    public CaoConnection getConnection() {
        return current.getConnection();
    }

    @Override
    public CaoNode getParent() {
        return current.getParent();
    }

    @Override
    public String getString(String name, String def) {
        return current.getString(name, def);
    }

    @Override
    public void clear() {
        current.clear();
    }

    @Override
    public void forEach(Consumer<? super java.util.Map.Entry<String, Object>> action) {
        current.forEach(action);
    }

    @Override
    public Collection<String> getPropertyKeys() {
        return current.getPropertyKeys();
    }

    @Override
    public CaoWritableElement getWritableNode() throws MException {
        return current.getWritableNode();
    }

    @Override
    public CaoNode getNode(String key) {
        return current.getNode(key);
    }

    @Override
    public String getId() {
        return current.getId();
    }

    @Override
    public String getString(String name) throws MException {
        return current.getString(name);
    }

    @Override
    public boolean getBoolean(String name, boolean def) {
        return current.getBoolean(name, def);
    }

    @Override
    public Collection<CaoNode> getNodes() {
        return current.getNodes();
    }

    @Override
    public int hashCode() {
        return current.hashCode();
    }

    @Override
    public String getName() {
        return current.getName();
    }

    @Override
    public boolean isNode() {
        return current.isNode();
    }

    @Override
    public boolean getBoolean(String name) throws MException {
        return current.getBoolean(name);
    }

    @Override
    public Collection<CaoNode> getNodes(String key) {
        return current.getNodes(key);
    }

    @Override
    public String getPath() {
        return current.getPath();
    }

    @Override
    public int getInt(String name, int def) {
        return current.getInt(name, def);
    }

    @Override
    public Spliterator<java.util.Map.Entry<String, Object>> spliterator() {
        return current.spliterator();
    }

    @Override
    public Collection<String> getPaths() {
        return current.getPaths();
    }

    @Override
    public Collection<String> getNodeKeys() {
        return current.getNodeKeys();
    }

    @Override
    public void reload() throws MException {
        current.reload();
    }

    @Override
    public long getLong(String name, long def) {
        return current.getLong(name, def);
    }

    @Override
    public float getFloat(String name, float def) {
        return current.getFloat(name, def);
    }

    @Override
    public InputStream getInputStream() {
        return current.getInputStream();
    }

    @Override
    public String toString() {
        return current.toString();
    }

    @Override
    public double getDouble(String name, double def) {
        return current.getDouble(name, def);
    }

    @Override
    public InputStream getInputStream(String rendition) {
        return current.getInputStream(rendition);
    }

    @Override
    public boolean isValid() {
        return current.isValid();
    }

    @Override
    public Calendar getCalendar(String name) throws MException {
        return current.getCalendar(name);
    }

    @Override
    public Collection<String> getRenditions() {
        return current.getRenditions();
    }

    @Override
    public Date getDate(String name) {
        return current.getDate(name);
    }

    @Override
    public boolean equals(Object other) {
        return current.equals(other);
    }

    @Override
    public IProperties getRenditionProperties() {
        return current.getRenditionProperties();
    }

    @Override
    public void setString(String name, String value) {
        doWritable();
        current.setString(name, value);
    }

    @Override
    public IProperties getRenditionProperties(String rendition) {
        return current.getRenditionProperties(rendition);
    }

    @Override
    public void setInt(String name, int value) {
        doWritable();
        current.setInt(name, value);
    }

    @Override
    public boolean isEditable() {
        return current.isEditable();
    }

    @Override
    public void setLong(String name, long value) {
        doWritable();
        current.setLong(name, value);
    }

    @Override
    public String getExtracted(String key) {
        return current.getExtracted(key);
    }

    @Override
    public void setDouble(String name, double value) {
        doWritable();
        current.setDouble(name, value);
    }

    @Override
    public <T> T adaptTo(Class<? extends T> ifc) {
        return current.adaptTo(ifc);
    }

    @Override
    public void setFloat(String name, float value) {
        doWritable();
        current.setFloat(name, value);
    }

    @Override
    public void setBoolean(String name, boolean value) {
        doWritable();
        current.setBoolean(name, value);
    }

    @Override
    public void setCalendar(String name, Calendar value) {
        doWritable();
        current.setCalendar(name, value);
    }

    @Override
    public void setDate(String name, Date value) {
        doWritable();
        current.setDate(name, value);
    }

    @Override
    public String getExtracted(String key, String def) {
        return current.getExtracted(key, def);
    }

    @Override
    public void setNumber(String name, Number value) {
        doWritable();
        current.setNumber(name, value);
    }

    @Override
    public Set<String> keys() {
        return current.keys();
    }

    @Override
    public Number getNumber(String name, Number def) {
        return current.getNumber(name, def);
    }

    @Override
    public boolean isProperty(String name) {
        return current.isProperty(name);
    }

    @Override
    public void removeProperty(String key) {
        doWritable();
        current.removeProperty(key);
    }

    @Override
    public void setProperty(String key, Object value) {
        doWritable();
        current.setProperty(key, value);
    }

    @Override
    public Iterator<java.util.Map.Entry<String, Object>> iterator() {
        return current.iterator();
    }

    @Override
    public Map<String, Object> toMap() {
        return current.toMap();
    }

    @Override
    public boolean isEmpty() {
        return current.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return current.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        return current.get(key);
    }

    @Override
    public Object put(String key, Object value) {
        return current.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return current.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        current.putAll(m);
    }

    @Override
    public void putReadProperties(IReadProperties m) {
        current.putReadProperties(m);
    }

    @Override
    public Set<String> keySet() {
        return current.keySet();
    }

    @Override
    public URL getUrl() {
        return current.getUrl();
    }

    @Override
    public boolean hasContent() {
        return current.hasContent();
    }

    @Override
    public CaoNode getNodeByPath(String path) {
        return current.getNodeByPath(path);
    }

    @Override
    public String dump() throws MException {
        return current.dump();
    }

    @Override
    public int size() {
        return current.size();
    }

    @Override
    public boolean containsValue(Object value) {
        return current.containsValue(value);
    }

    @Override
    public Collection<Object> values() {
        return current.values();
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return current.entrySet();
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        return current.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super Object> action) {
        current.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ? extends Object> function) {
        current.replaceAll(function);
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        return current.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return current.remove(key, value);
    }

    @Override
    public boolean replace(String key, Object oldValue, Object newValue) {
        return current.replace(key, oldValue, newValue);
    }

    @Override
    public Object replace(String key, Object value) {
        return current.replace(key, value);
    }

    @Override
    public Object computeIfAbsent(
            String key, Function<? super String, ? extends Object> mappingFunction) {
        return current.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public Object computeIfPresent(
            String key,
            BiFunction<? super String, ? super Object, ? extends Object> remappingFunction) {
        return current.computeIfPresent(key, remappingFunction);
    }

    @Override
    public Object compute(
            String key,
            BiFunction<? super String, ? super Object, ? extends Object> remappingFunction) {
        return current.compute(key, remappingFunction);
    }

    @Override
    public Object merge(
            String key,
            Object value,
            BiFunction<? super Object, ? super Object, ? extends Object> remappingFunction) {
        return current.merge(key, value, remappingFunction);
    }

    @Override
    public CaoActionStarter getUpdateAction() throws MException {
        if (isChanged()) return ((CaoWritableElement) current).getUpdateAction();
        return null;
    }
}
