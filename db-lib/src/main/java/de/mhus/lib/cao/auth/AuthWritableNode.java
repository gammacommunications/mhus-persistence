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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import de.mhus.lib.basics.ReadOnly;
import de.mhus.lib.cao.CaoActionStarter;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.CaoWritableElement;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.errors.MException;

public class AuthWritableNode extends CaoWritableElement {

    private static final long serialVersionUID = 1L;
    private CaoWritableElement instance;
    private CaoNode readable;

    public AuthWritableNode(
            AuthCore core, AuthNode parent, CaoNode readable, CaoWritableElement writableNode) {
        super(core, parent);
        this.instance = writableNode;
        this.readable = readable;
    }

    @Override
    public CaoActionStarter getUpdateAction() throws MException {
        if (!((AuthCore) core).hasWriteAccess(readable)) return null;
        return new AuthActionStarter(instance.getUpdateAction());
    }

    @Override
    public CaoWritableElement getWritableNode() throws MException {
        return this;
    }

    @Override
    public String getId() {
        return instance.getId();
    }

    @Override
    public String getName() {
        return instance.getName();
    }

    @Override
    public boolean isNode() {
        return instance.isNode();
    }

    @Override
    public void reload() throws MException {
        instance.reload();
    }

    @Override
    public boolean isValid() {
        return instance.isValid();
    }

    @Override
    public Collection<String> getPropertyKeys() {
        Collection<String> ret = instance.getPropertyKeys();
        if (ret instanceof ReadOnly) ret = new LinkedList<>(ret);

        for (Iterator<String> iter = ret.iterator(); iter.hasNext(); ) {
            String next = iter.next();
            if (!((AuthCore) core).hasReadAccess(instance, next)) iter.remove();
        }

        return ((AuthCore) core).mapReadNames(instance, ret);
    }

    @Override
    public CaoNode getNode(String key) {
        CaoNode n = instance.getNode(key);
        if (n == null || !((AuthCore) core).hasReadAccess(n)) return null;
        return new AuthNode((AuthCore) core, (AuthNode) getParent(), n);
    }

    @Override
    public Collection<CaoNode> getNodes() {
        Collection<CaoNode> in = instance.getNodes();
        LinkedList<CaoNode> out = new LinkedList<>();
        for (CaoNode n : in) {
            if (((AuthCore) core).hasReadAccess(n))
                out.add(new AuthNode((AuthCore) core, (AuthNode) getParent(), n));
        }
        return out;
    }

    @Override
    public Collection<CaoNode> getNodes(String key) {
        Collection<CaoNode> in = instance.getNodes(key);
        LinkedList<CaoNode> out = new LinkedList<>();
        for (CaoNode n : in) {
            if (((AuthCore) core).hasReadAccess(n))
                out.add(new AuthNode((AuthCore) core, (AuthNode) getParent(), n));
        }
        return out;
    }

    @Override
    public Collection<String> getNodeKeys() {
        HashSet<String> out = new HashSet<>();
        for (CaoNode n : getNodes()) out.add(n.getName());
        return out;
    }

    @Override
    public InputStream getInputStream(String rendition) {
        if (!((AuthCore) core).hasContentAccess(instance, null)) return null;
        return instance.getInputStream();
    }

    @Override
    public URL getUrl() {
        return instance.getUrl();
    }

    @Override
    public boolean hasContent() {
        return instance.hasContent();
    }

    @Override
    public Object getProperty(String name) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return null;
        return instance.getProperty(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public boolean isProperty(String name) {
        if (!((AuthCore) core).hasReadAccess(instance, name)) return false;
        return instance.isProperty(((AuthCore) core).mapReadName(instance, name));
    }

    @Override
    public void removeProperty(String key) {
        if (!((AuthCore) core).hasWriteAccess(instance, key)) return;
        instance.removeProperty(((AuthCore) core).mapWriteName(instance, key));
    }

    @Override
    public void setProperty(String key, Object value) {
        if (!((AuthCore) core).hasWriteAccess(instance, key)) return;
        instance.setProperty(((AuthCore) core).mapWriteName(instance, key), value);
    }

    @Override
    public Collection<String> getRenditions() {
        return instance.getRenditions();
    }

    @Override
    public void clear() {
        for (String key : keys()) removeProperty(key);
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
        return instance.getRenditionProperties(
                ((AuthCore) core).mapReadRendition(instance, rendition));
    }
}
