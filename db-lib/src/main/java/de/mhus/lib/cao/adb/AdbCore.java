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
package de.mhus.lib.cao.adb;

import java.io.IOException;
import java.util.UUID;
import java.util.WeakHashMap;

import de.mhus.lib.adb.DbManager;
import de.mhus.lib.adb.DbSchema;
import de.mhus.lib.adb.Persistable;
import de.mhus.lib.adb.query.Db;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoDriver;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.core.MString;
import de.mhus.lib.errors.MException;

public class AdbCore extends CaoCore {

    private DbSchema schema;
    private Class<? extends AdbNodeData> type;
    private WeakHashMap<String, AdbNode> cache = new WeakHashMap<>();
    private String collection;
    private DbManager manager;

    @SuppressWarnings("unchecked")
    public AdbCore(
            String name,
            CaoDriver driver,
            DbManager manager,
            Class<? extends AdbNodeData> type,
            String collection)
            throws IOException {
        super(name, driver);
        con = new AdbConnection(this);
        this.collection = collection;
        this.manager = manager;
        this.schema = manager.getSchema();

        if (type != null) this.type = type;
        else
            for (Class<? extends Persistable> t : schema.getObjectTypes()) {
                if (CaoNode.class.isAssignableFrom(t)) {
                    this.type = (Class<? extends AdbNodeData>) t;
                }
            }
        if (type == null) throw new IOException("Managing type not found");

        actionList.add(new AdbCreate());
        actionList.add(new AdbDelete());
        actionList.add(new AdbMove());
        actionList.add(new AdbCopy());
        actionList.add(new AdbRename());

        actionList.add(new AdbUploadRendition());
        actionList.add(new AdbDeleteRendition());
        doInitializeActions();
    }

    public AdbNodeData[] getChildren(UUID parent) throws MException {
        return manager.getByQualification(
                        Db.query(AdbNodeData.class, type)
                                .eq("collection", collection)
                                .eq("parent", parent))
                .toArrayAndClose(new AdbNodeData[0]);
    }

    public AdbNodeData getChild(UUID parent, String name) throws MException {
        return manager.getObjectByQualification(
                Db.query(AdbNodeData.class, type)
                        .eq("collection", collection)
                        .eq("parent", parent)
                        .eq("name", name));
    }

    public AdbNodeData[] getChildren(UUID parent, String name) throws MException {
        return manager.getByQualification(
                        Db.query(AdbNodeData.class, type)
                                .eq("collection", collection)
                                .eq("parent", parent)
                                .eq("name", name))
                .toArrayAndClose(new AdbNodeData[0]);
    }

    @Override
    public CaoNode getRoot() {
        AdbNode root = null;
        try {
            AdbNodeData[] list = getChildren(null);
            if (list.length == 0) {
                // root not set
            } else if (list.length > 1) {
                // more then one root
                log().w("more the one root node", collection, list[0].getId());
                root = new AdbNode(this, null, list[0]);
            } else {
                root = new AdbNode(this, null, list[0]);
            }
        } catch (MException e) {
            log().d(e);
        }
        return root;
    }

    @Override
    public CaoNode getResourceByPath(String path) {
        synchronized (this) {
            AdbNode node = cache.get(path);
            if (node != null) {
                if (node.isValid()) return node;
                cache.remove(path);
            }

            node = (AdbNode) getRoot();
            for (String part : path.split("/")) {
                if (MString.isSet(part)) {
                    node = (AdbNode) node.getNode(part);
                    if (node == null) return null;
                }
            }

            cache.put(path, node);

            return node;
        }
    }

    @Override
    public CaoNode getResourceById(String id) {
        synchronized (this) {
            AdbNode node = cache.get(id);
            if (node != null) {
                if (node.isValid()) return node;
                cache.remove(id);
            }

            try {
                AdbNodeData data = manager.getObject(type, UUID.fromString(id));
                if (data != null) {
                    node = new AdbNode(this, null, data);
                    cache.put(id, node);
                }
            } catch (MException e) {
                log().d(e);
            }
            return node;
        }
    }

    public Class<? extends AdbNodeData> getType() {
        return type;
    }

    public String getCollection() {
        return collection;
    }

    @Override
    protected void closeConnection() throws Exception {}
}
