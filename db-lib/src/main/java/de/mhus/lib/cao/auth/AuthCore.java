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

import java.util.Collection;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoActionList;
import de.mhus.lib.cao.CaoAspect;
import de.mhus.lib.cao.CaoAspectFactory;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.cao.util.MutableActionList;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.logging.Log;
import de.mhus.lib.errors.MException;

public class AuthCore extends CaoCore {

    protected CaoCore instance;
    private Authorizator auth;
    private AuthNode root;

    public AuthCore(CaoCore instance, Authorizator auth) throws MException {
        this(instance.getName(), instance, auth);
    }

    public AuthCore(String name, CaoCore instance, Authorizator auth) throws MException {
        super(name, instance.getDriver());
        this.con = new AuthConnection(this);
        this.instance = instance;
        this.auth = auth;
        registerAspectFactory(AuthAccess.class, new AuthAccessFactory(this));
    }

    public AuthCore(String name, CaoCore instance) {
        super(name, instance.getDriver());
        this.instance = instance;
    }

    @Override
    public CaoNode getResourceByPath(String path) {
        CaoNode n = instance.getResourceByPath(path);
        if (n == null || !hasReadAccess(n)) return null;
        return new AuthNode(this, n);
    }

    @Override
    public String toString() {
        return instance.toString();
    }

    @Override
    public Log log() {
        return instance.log();
    }

    @Override
    public CaoNode getResourceById(String id) {
        CaoNode n = instance.getResourceById(id);
        if (n == null || !hasReadAccess(n)) return null;
        return new AuthNode(this, n);
    }

    @Override
    public CaoNode getRoot() {
        if (root == null) root = new AuthNode(this, instance.getRoot());
        if (!hasReadAccess(instance.getRoot())) return null;
        return root;
    }

    @Override
    public CaoActionList getActions() {
        MutableActionList out = new MutableActionList();
        for (CaoAction action : instance.getActions())
            if (hasActionAccess(action)) out.add(new AuthAction(this, action));
        return out;
    }

    @Override
    public <T extends CaoAspect> CaoAspectFactory<T> getAspectFactory(Class<T> ifc) {
        CaoAspectFactory<T> mine = super.getAspectFactory(ifc);
        if (mine != null) return mine;
        return instance.getAspectFactory(ifc);
    }

    @Override
    public boolean equals(Object obj) {
        return instance.equals(obj);
    }

    public Authorizator getAuthorizator() {
        return auth;
    }

    public boolean hasReadAccess(CaoNode node) {
        if (auth == null) return true;
        return auth.hasReadAccess(node);
    }

    public boolean hasWriteAccess(CaoNode node) {
        if (auth == null) return true;
        return auth.hasWriteAccess(node);
    }

    public boolean hasStructureAccess(CaoNode node) {
        if (auth == null) return true;
        return auth.hasStructureAccess(node);
    }

    public boolean hasDeleteAccess(CaoNode node) {
        if (auth == null) return true;
        return auth.hasDeleteAccess(node);
    }

    public boolean hasActionAccess(CaoAction action) {
        if (auth == null) return true;
        return auth.hasActionAccess(action);
    }

    public boolean hasReadAccess(CaoNode node, String name) {
        if (auth == null) return true;
        return auth.hasReadAccess(node, name);
    }

    public boolean hasWriteAccess(CaoNode node, String name) {
        if (auth == null) return true;
        return auth.hasWriteAccess(node, name);
    }

    public boolean hasContentAccess(CaoNode node, String rendition) {
        if (auth == null) return true;
        return auth.hasContentAccess(node, rendition);
    }

    public boolean hasContentWriteAccess(CaoNode node, String rendition) {
        if (auth == null) return true;
        return auth.hasContentWriteAccess(node, rendition);
    }

    public boolean hasCreateAccess(CaoNode node, String name, IProperties properties) {
        if (auth == null) return true;
        return auth.hasCreateAccess(node, name, properties);
    }

    public boolean hasAspectAccess(CaoNode node, Class<? extends CaoAspect> ifc) {
        if (auth == null) return true;
        return auth.hasAspectAccess(node, ifc);
    }

    public String mapReadName(CaoNode node, String name) {
        if (auth == null) return name;
        return auth.mapReadName(node, name);
    }

    public String mapReadRendition(CaoNode node, String rendition) {
        if (auth == null) return rendition;
        return auth.mapReadRendition(node, rendition);
    }

    public Collection<String> mapReadNames(CaoNode node, Collection<String> set) {
        if (auth == null) return set;
        return auth.mapReadNames(node, set);
    }

    public String mapWriteName(CaoNode node, String name) {
        if (auth == null) return name;
        return auth.mapWriteName(node, name);
    }

    public boolean hasActionAccess(CaoConfiguration configuration, CaoAction action) {
        if (auth == null) return true;
        return auth.hasActionAccess(configuration, action);
    }

    public CaoNode getInstance(AuthNode node) {
        return node.instance;
    }

    @Override
    protected void closeConnection() throws Exception {}

    @Override
    public void close() {
        instance.close();
    }

    @Override
    public boolean isClosed() {
        return instance.isClosed();
    }

    @Override
    public boolean isShared() {
        return instance.isShared();
    }

    @Override
    public void closeShared() {
        instance.closeShared();
    }
}
