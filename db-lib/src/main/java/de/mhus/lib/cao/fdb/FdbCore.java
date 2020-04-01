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
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.TimeoutException;

import de.mhus.lib.cao.CaoConst;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoMetaDefinition.TYPE;
import de.mhus.lib.cao.CaoMetadata;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.CaoPolicy;
import de.mhus.lib.cao.util.MutableMetadata;
import de.mhus.lib.core.MFile;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.core.MString;
import de.mhus.lib.core.MPeriod;
import de.mhus.lib.errors.MException;

public class FdbCore extends CaoCore {

    public static final String CONTROL_FILE_PREFIX = "__cao.";

    private FdbNode root;
    private WeakHashMap<String, FdbNode> cache = new WeakHashMap<>();
    private CaoMetadata metadata;

    @SuppressWarnings("unused")
    private File rootDir;

    private File filesDir;
    private File indexDir;
    private File lockFile;
    private int fileSub;

    private FileLock lock;

    public FdbCore(String name, String root, boolean useCache)
            throws MException, IOException, TimeoutException {
        this(name, new File(root), useCache);
    }

    public FdbCore(String name, File root, boolean useCache)
            throws MException, IOException, TimeoutException {
        this(name, new FdbDriver(), root);
        if (!useCache) cache = null;
        if (this.root != null) this.root.reload();
    }

    public FdbCore(String name, FdbDriver driver, File root)
            throws IOException, TimeoutException, MException {
        super(name, driver);
        this.con = new FdbConnection(this);

        metadata = new MutableMetadata(driver).addDefinition(CaoConst.MODIFIED, TYPE.LONG, 0);

        rootDir = root;
        filesDir = new File(root, "repository/files");
        indexDir = new File(root, "repository/index");
        lockFile = new File(root, "sync.lock");
        filesDir.mkdirs();
        indexDir.mkdirs();
        fileSub = filesDir.getAbsolutePath().length();

        this.root = new FdbNode(this, filesDir, null);

        fullIndex();

        registerAspectFactory(CaoPolicy.class, new FdbPolicyProvider());

        actionList.add(new FdbCreate());
        actionList.add(new FdbDelete());
        actionList.add(new FdbMove());
        actionList.add(new FdbCopy());
        actionList.add(new FdbRename());

        actionList.add(new FdbUploadRendition());
        actionList.add(new FdbDeleteRendition());
        doInitializeActions();
        //		registerAspectFactory(StructureControl.class, new FdbStructureControl());

    }

    private void fullIndex() throws IOException, TimeoutException {

        lock();
        try {
            log().d("Full index", getName());
            indexDir(filesDir);

        } finally {
            release();
        }
    }

    void indexDir(File dir) {
        for (File f : dir.listFiles()) {

            if (f.isFile() && !f.getName().startsWith(".") && !f.getName().startsWith("_")) {
                // auto convert
                log().d(getName(), "Convert file");
                File tmp = new File(dir, "__temp." + f.getName());
                f.renameTo(tmp);
                f.mkdirs();
                tmp.renameTo(new File(f, "__cao.content.content"));
            }

            if (f.isDirectory() && !f.getName().startsWith(".") && !f.isHidden()) {
                try {
                    indexFile(f);
                } catch (Throwable t) {
                    log().d("Index", f, t);
                }
                indexDir(f);
            }
        }
    }

    void indexFile(File f) throws IOException {
        File meta = getMetaFileFor(f);
        MProperties p = MProperties.load(meta);
        if (p == null) p = new MProperties();
        String id = p.getString("_id", null);
        if (id == null) {
            id = UUID.randomUUID().toString();
            p.setString("_id", id);
            p.save(meta);
        }
        rememberIndex(f, id);
    }

    private void rememberIndex(File f, String id) {
        if (id == null) return;
        String c = f.getAbsolutePath().substring(fileSub);
        File iFile = new File(indexDir, idCut(id));
        String cc = iFile.exists() ? MFile.readFile(iFile) : null;
        if (!c.equals(cc)) {
            iFile.getParentFile().mkdirs();
            MFile.writeFile(iFile, c);
        }
    }

    private String idCut(String id) {
        return id.replace('-', '/');
    }

    private String getPathForId(String id) {
        if (id == null) return null;
        File iFile = new File(indexDir, idCut(id));
        String c = MFile.readFile(iFile);
        return c;
    }

    @Override
    public CaoNode getRoot() {
        return root;
    }

    @Override
    public CaoNode getResourceByPath(String path) {
        checkState();
        if (path == null) return null;
        synchronized (this) {
            FdbNode node = cache == null ? null : cache.get(path);
            if (node != null) {
                if (node.isValid()) return node;
                cache.remove(path);
            }

            node = root;
            for (String part : path.split("/")) {
                if (MString.isSet(part)) {
                    node = (FdbNode) node.getNode(part);
                    if (node == null) return null;
                }
            }

            if (cache != null) cache.put(path, node);

            return node;
        }
    }

    public void fillProperties(File file, MProperties p) {
        checkState();

        File metaFile = getMetaFileFor(file);

        if (metaFile.exists() && metaFile.isFile()) {
            MProperties meta = MProperties.load(metaFile);
            p.putAll(meta);
        }

        p.setLong(CaoConst.MODIFIED, metaFile.lastModified());
    }

    public File getMetaFileFor(File file) {
        File metaFile = null;
        if (file.isDirectory()) metaFile = new File(file, "__cao.meta");
        else metaFile = new File(file.getParentFile(), "__cao." + file.getName() + ".meta");
        return metaFile;
    }

    public CaoMetadata getMetadata() {
        return metadata;
    }

    @Override
    public CaoNode getResourceById(String id) {
        checkState();
        String path = getPathForId(id);
        return getResourceByPath(path);
        //		File f = new File(filesDir, path);
    }

    public File getContentFileFor(File file, String rendition) {
        checkState();
        if (file.isFile()) {
            if (rendition != null) return null;
            return file;
        }

        if (rendition == null) rendition = "content";
        rendition = MFile.normalize(rendition);
        return new File(file, "__cao.content." + rendition);
    }

    void deleteIndex(String id) {
        checkState();
        if (id == null) return;
        File iFile = new File(indexDir, idCut(id));
        if (iFile.exists()) iFile.delete();
    }

    File getFilesDir() {
        return filesDir;
    }

    public File getFileForId(String id) {
        checkState();
        String path = getPathForId(id);
        synchronized (this) {
            FdbNode node = cache == null ? null : cache.get(path);
            if (node != null) {
                if (node.isValid()) return node.getFile();
                cache.remove(path);
            }

            File ret = new File(filesDir, path);
            if (ret.exists()) return ret;
            return null;
        }
    }

    public void lock() throws IOException, TimeoutException {
        lock(MPeriod.MINUTE_IN_MILLISECOUNDS);
    }

    public void lock(long timeout) throws IOException, TimeoutException {
        checkState();
        synchronized (this) {
            lock = MFile.aquireLock(lockFile, timeout);
        }
    }

    public void release() {
        synchronized (this) {
            if (lock == null) return;
            MFile.releaseLock(lock);
            lock = null;
        }
    }

    @Override
    protected void closeConnection() throws Exception {
        release();
    }
}
