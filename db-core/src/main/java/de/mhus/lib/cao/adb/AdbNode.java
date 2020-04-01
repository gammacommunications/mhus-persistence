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

import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoException;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.adb.AdbNodeData.TYPE;
import de.mhus.lib.cao.util.PropertiesNode;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.MCollection;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.core.util.SingleList;
import de.mhus.lib.errors.MException;

public class AdbNode extends PropertiesNode {

    private AdbNodeData data;

    public AdbNode(CaoCore con, CaoNode parent, AdbNodeData data) {
        super(con, parent);
        this.data = data;
        init();
    }

    private void init() {
        this.properties = data.getProperties();
        createMetadataByProperties();
    }

    /** */
    private static final long serialVersionUID = 1L;

    @Override
    public boolean isNode() {
        return data.getType() == TYPE.NODE;
    }

    @Override
    public void reload() throws CaoException {
        try {
            data.reload();
        } catch (MException e) {
            throw new CaoException(e);
        }
    }

    @Override
    public boolean isValid() {
        try {
            return data != null && !data.isAdbChanged();
        } catch (MException e) {
            log().d(this, e);
        }
        return false;
    }

    @Override
    public CaoNode getNode(String key) {
        try {
            AdbNodeData childData = ((AdbCore) core).getChild(data.getId(), key);
            if (childData != null) return new AdbNode(core, this, childData);
        } catch (MException e) {
            log().d(e);
        }
        return null;
    }

    @Override
    public List<CaoNode> getNodes() {
        try {
            AdbNodeData[] childData = ((AdbCore) core).getChildren(data.getId());
            LinkedList<CaoNode> out = new LinkedList<>();
            for (int i = 0; i < childData.length; i++)
                out.add(new AdbNode(core, this, childData[i]));
            return out;
        } catch (MException e) {
            log().d(e);
        }
        return MCollection.getEmptyList();
    }

    @Override
    public List<CaoNode> getNodes(String key) {
        try {
            AdbNodeData[] childData = ((AdbCore) core).getChildren(data.getId(), key);
            LinkedList<CaoNode> out = new LinkedList<>();
            for (int i = 0; i < childData.length; i++)
                out.add(new AdbNode(core, this, childData[i]));
            return out;
        } catch (MException e) {
            log().d(e);
        }
        return MCollection.getEmptyList();
    }

    @Override
    public List<String> getNodeKeys() {
        List<CaoNode> nodes = getNodes();
        LinkedList<String> out = new LinkedList<>();

        for (CaoNode node : nodes) out.add(node.getName());

        return out;
    }

    @Override
    public InputStream getInputStream(String rendition) {
        return null;
    }

    @Override
    public URL getUrl() {
        return null;
    }

    @Override
    public boolean hasContent() {
        return false;
    }

    @Override
    protected void doUpdate(MProperties modified) throws CaoException {
        data.setProperties(modified);
        try {
            data.save();
        } catch (MException e) {
            throw new CaoException(e);
        }
    }

    @Override
    public boolean isEditable() {
        return true;
    }

    @Override
    public CaoNode getParent() {
        CaoNode p = super.getParent();
        if (p == null && data != null && data.getParent() != null) {
            p = ((AdbCore) core).getResourceById(data.getParent().toString());
        }
        return p;
    }

    @Override
    public Collection<String> getRenditions() {
        return null;
    }

    @Override
    public void clear() {}

    @Override
    public String getPath() {
        CaoNode n = this;
        StringBuilder out = new StringBuilder();
        while (true) {
            out.insert(0, n.getName());
            out.insert(0, '/');
            n = n.getParent();
            if (n == null) break;
        }
        return out.toString();
    }

    @Override
    public Collection<String> getPaths() {
        return new SingleList<String>(getPath());
    }

    @Override
    public IProperties getRenditionProperties(String rendition) {
        return null;
    }
}
