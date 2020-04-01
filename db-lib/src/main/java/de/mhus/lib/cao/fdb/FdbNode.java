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
package de.mhus.lib.cao.fdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedList;

import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.aspect.Changes;
import de.mhus.lib.cao.util.PropertiesNode;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.MFile;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.util.EmptyList;
import de.mhus.lib.core.util.SingleList;
import de.mhus.lib.errors.AccessDeniedException;

public class FdbNode extends PropertiesNode {

    private static final long serialVersionUID = 1L;
    private File file;

    public FdbNode(FdbCore connection, File file, FdbNode parent) {
        super(connection, parent);
        this.file = file;
        reload();
        //		this.metadata = ((FdCore)core).getMetadata();
    }

    @Override
    public boolean isNode() {
        return file.isDirectory();
    }

    @Override
    public void reload() {
        ((FdbCore) core).fillProperties(file, properties);
        //		FdNode root = ((FdNode)((FdConnection)getConnection()).getRoot());
        this.id = properties.getString("_id", "");
        this.name = file.getName();
    }

    File getFile() {
        return file;
    }

    @Override
    public boolean isValid() {
        return file.exists();
    }

    @Override
    public CaoNode getNode(String key) {
        File f = new File(file, key);
        if (f.exists()) return new FdbNode((FdbCore) core, f, this);
        return null;
    }

    @Override
    public Collection<CaoNode> getNodes() {
        LinkedList<CaoNode> out = new LinkedList<>();
        for (File f : file.listFiles()) {
            if (f.isHidden() || f.getName().startsWith(".") || f.getName().startsWith("__cao."))
                continue;
            out.add(new FdbNode((FdbCore) core, f, this));
        }
        return out;
    }

    @Override
    public Collection<CaoNode> getNodes(String key) {
        CaoNode out = getNode(key);
        LinkedList<CaoNode> list = new LinkedList<>();
        if (out != null) list.add(out);
        return list;
    }

    @Override
    public Collection<String> getNodeKeys() {
        LinkedList<String> out = new LinkedList<>();
        for (File f : file.listFiles()) {
            if (f.isHidden() || f.getName().startsWith(".")) continue;
            out.add(f.getName());
        }
        return out;
    }

    @Override
    public InputStream getInputStream(String rendition) {

        File contentFile = ((FdbCore) core).getContentFileFor(file, rendition);
        if (contentFile == null || !contentFile.exists()) return null;
        try {
            return new FileInputStream(contentFile);
        } catch (FileNotFoundException e) {
            log().d(contentFile, e);
        }
        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public URL getUrl() {
        File contentFile = ((FdbCore) core).getContentFileFor(file, null);
        if (contentFile == null || !contentFile.exists()) return null;
        try {
            return contentFile.toURL();
        } catch (MalformedURLException e) {
            log().d(file, e);
        }
        return null;
    }

    @Override
    public boolean hasContent() {
        return true;
    }

    @Override
    protected void doUpdate(MProperties modified) {
        if (!isEditable()) throw new AccessDeniedException(file);
        File metaFile = ((FdbCore) core).getMetaFileFor(file);
        modified.remove("id");
        try {
            modified.save(metaFile);
        } catch (IOException e) {
            log().w(metaFile, e);
        }
        reload();

        Changes change = adaptTo(Changes.class);
        if (change != null) change.modified();
    }

    @Override
    public boolean isEditable() {
        return file.canWrite() && file.getParentFile().canWrite();
    }

    @Override
    public Collection<String> getRenditions() {
        if (hasContent()) {
            // TODO if use meta collect list of renditions
            return new EmptyList<>();
        }
        return null;
    }

    @Override
    public void clear() {}

    @Override
    public String getPath() {
        String out =
                MFile.normalizePath(
                        file.getPath()
                                .substring(((FdbCore) core).getFilesDir().getPath().length()));
        if (MSystem.isWindows()) out = out.replace('\\', '/');
        return out;
    }

    @Override
    public Collection<String> getPaths() {
        return new SingleList<String>(getPath());
    }

    @Override
    public IProperties getRenditionProperties(String rendition) {
        File contentFile = ((FdbCore) core).getContentFileFor(file, rendition);
        contentFile = new File(contentFile.getParent(), contentFile.getName() + ".meta");
        if (contentFile == null || !contentFile.exists()) return null;
        return MProperties.load(contentFile);
    }

    public void reloadById() {
        file = ((FdbCore) core).getFileForId(getId());
        reload();
    }

    protected void setParent(CaoNode to) {
        this.parent = to;
    }
}
