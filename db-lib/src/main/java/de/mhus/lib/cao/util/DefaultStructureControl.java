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

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoAspectFactory;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.action.CopyConfiguration;
import de.mhus.lib.cao.action.CreateConfiguration;
import de.mhus.lib.cao.action.DeleteConfiguration;
import de.mhus.lib.cao.action.MoveConfiguration;
import de.mhus.lib.cao.action.RenameConfiguration;
import de.mhus.lib.cao.action.UploadRenditionConfiguration;
import de.mhus.lib.cao.aspect.Changes;
import de.mhus.lib.cao.aspect.StructureControl;
import de.mhus.lib.cao.aspect.StructureControl.Behavior;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.MLog;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.strategy.OperationResult;
import de.mhus.lib.errors.MException;

public class DefaultStructureControl extends MLog implements CaoAspectFactory<StructureControl> {

    private Behavior<?> defaultBehavior;

    public DefaultStructureControl(Behavior<?> defaultBehavior) {
        this.defaultBehavior = defaultBehavior;
    }

    public DefaultStructureControl(String attributeName) {
        this.defaultBehavior = new DefaultBehavior(attributeName);
    }

    @Override
    public StructureControl getAspectFor(CaoNode node) {
        return new Aspect(node);
    }

    @Override
    public void doInitialize(CaoCore core, MutableActionList actionList) {}

    private class Aspect implements StructureControl {

        private CaoNode node;

        @SuppressWarnings("unchecked")
        private Behavior<Object> behavior = (Behavior<Object>) defaultBehavior;

        public Aspect(CaoNode node) {
            this.node = node;
        }

        private WritableNode findMyself(List<WritableNode> all) {
            for (WritableNode n : all) if (n.equals(node)) return n;
            return null;
        }

        private void sortAll(List<WritableNode> all) {
            all.sort(behavior);
        }

        private void repairAll(List<WritableNode> all) {
            // repair order of the nodes - do not remove gaps
            HashSet<Object> index = new HashSet<>();
            LinkedList<WritableNode> rest = new LinkedList<>();
            Object max = null;
            for (WritableNode n : all) {
                Object pos = behavior.getPosition(n);
                if (pos == null) {
                    rest.add(n);
                } else {
                    if (index.contains(pos)) {
                        while (index.contains(pos)) pos = behavior.inc(pos);
                        behavior.setPosition(n, pos);
                    }
                    index.add(pos);
                    max = behavior.max(max, pos);
                }
            }
            for (WritableNode n : rest) {
                max = behavior.inc(max);
                behavior.setPosition(n, max);
            }
        }

        private boolean saveAll(List<WritableNode> all) {
            boolean changed = false;
            for (WritableNode n : all)
                if (n.isChanged()) {
                    try {
                        n.save();
                        changed = true;
                    } catch (MException e) {
                    }
                }
            return changed;
        }

        private LinkedList<WritableNode> loadAll() {

            LinkedList<WritableNode> out = behavior.loadAll(node);
            if (out != null) return out;

            out = new LinkedList<WritableNode>();
            for (CaoNode n : node.getParent().getNodes()) out.add(new WritableNode(n));
            return out;
        }

        private void fillGaps(List<WritableNode> all) {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean moveUp() {

            if (node.getParentId() == null) return false;

            // switch place with note before
            List<WritableNode> all = loadAll();
            repairAll(all);
            sortAll(all);
            WritableNode last = null;
            for (WritableNode n : all) {
                if (n.equals(node)) {
                    if (last != null) {
                        // switch pos
                        Object lastPos = behavior.getPosition(last);
                        Object myPos = behavior.getPosition(n);
                        behavior.setPosition(last, myPos);
                        behavior.setPosition(n, lastPos);
                    }
                    break;
                }
                last = n;
            }

            boolean ret = saveAll(all);
            if (ret) {
                Changes change = node.adaptTo(Changes.class);
                if (change != null) change.moved();
            }
            return ret;
        }

        @Override
        public boolean moveDown() {

            if (node.getParentId() == null) return false;

            // switch place with next node
            List<WritableNode> all = loadAll();
            repairAll(all);
            sortAll(all);
            WritableNode last = null;
            for (WritableNode n : all) {
                if (last != null) {
                    // switch pos
                    Object lastPos = behavior.getPosition(last);
                    Object myPos = behavior.getPosition(n);
                    behavior.setPosition(last, myPos);
                    behavior.setPosition(n, lastPos);
                    break;
                }
                if (n.equals(node)) last = n;
            }
            boolean ret = saveAll(all);
            if (ret) {
                Changes change = node.adaptTo(Changes.class);
                if (change != null) change.moved();
            }
            return ret;
        }

        @Override
        public boolean moveToTop() {

            if (node.getParentId() == null) return false;

            // move all one down up to me
            List<WritableNode> all = loadAll();
            repairAll(all);
            sortAll(all);
            Object firstPos = null;
            WritableNode last = null;
            WritableNode me = null;
            Object max = null;
            for (WritableNode n : all) {
                Object pos = behavior.getPosition(n);
                max = pos;
                if (n.equals(node)) {
                    me = n;
                    continue;
                }
                if (firstPos == null) firstPos = pos;
                if (last != null) behavior.setPosition(last, pos);
                last = n;
            }
            max = behavior.inc(max);
            if (last != null) behavior.setPosition(last, max);

            if (me != null && firstPos != null) behavior.setPosition(me, firstPos);

            boolean ret = saveAll(all);
            if (ret) {
                Changes change = node.adaptTo(Changes.class);
                if (change != null) change.moved();
            }
            return ret;
        }

        @Override
        public boolean moveToBottom() {

            if (node.getParentId() == null) return false;

            List<WritableNode> all = loadAll();
            repairAll(all);
            sortAll(all);
            Object max = null;
            for (WritableNode n : all) {
                Object pos = behavior.getPosition(n);
                max = behavior.max(max, pos);
            }
            if (max == null) return false;
            WritableNode me = findMyself(all);
            Object pos = behavior.getPosition(me);
            if (behavior.equals(max, pos)) return false;
            max = behavior.inc(max);
            behavior.setPosition(me, max);
            fillGaps(all);
            boolean ret = saveAll(all);
            if (ret) {
                Changes change = node.adaptTo(Changes.class);
                if (change != null) change.moved();
            }
            return ret;
        }

        @Override
        public boolean moveAfter(CaoNode predecessor) {

            if (node.getParentId() == null) return false;

            if (!node.getParentId().equals(predecessor.getParentId())) {
                if (!moveTo(predecessor.getParent(), false)) return false;
            }

            List<WritableNode> all = loadAll();
            repairAll(all);
            sortAll(all);
            boolean found = false;
            WritableNode me = null;
            Object myPos = null;
            Object lastPos = null;
            Object lastPosAbs = null;
            WritableNode lastNode = null;

            for (WritableNode n : all) {

                if (n.equals(node)) {
                    me = n;
                    continue;
                }

                Object pos = behavior.getPosition(n);
                lastPosAbs = pos;

                if (found) {
                    if (lastNode != null) {
                        behavior.setPosition(lastNode, pos);
                    }
                    lastNode = n;
                    if (lastPos == null) {
                        myPos = pos;
                    }
                    lastPos = pos;
                } else if (n.equals(predecessor)) {
                    found = true;
                }
            }
            if (lastNode != null && lastPos != null) {
                Object nextPos = behavior.inc(lastPos);
                behavior.setPosition(lastNode, nextPos);
            }
            if (me != null && lastPosAbs != null) {
                if (myPos == null) {
                    myPos = behavior.inc(lastPosAbs);
                }
                behavior.setPosition(me, myPos);
            }

            boolean ret = saveAll(all);
            if (ret) {
                Changes change = node.adaptTo(Changes.class);
                if (change != null) change.moved();
            }
            return ret;
        }

        @Override
        public boolean moveBefore(CaoNode successor) {
            if (!moveAfter(successor)) return false;
            return moveUp();
        }

        @Override
        public int getPositionIndex() {
            List<WritableNode> all = loadAll();
            repairAll(all);
            sortAll(all);
            int cnt = 0;
            for (WritableNode n : all) {
                if (n.equals(node)) return cnt;
                cnt++;
            }
            return -1;
        }

        @Override
        public boolean isAtTop() {
            LinkedList<WritableNode> all = loadAll();
            if (all.size() == 0) return false;
            repairAll(all);
            sortAll(all);
            return all.getFirst().equals(node);
        }

        @Override
        public boolean isAtBottom() {
            LinkedList<WritableNode> all = loadAll();
            if (all.size() == 0) return false;
            repairAll(all);
            sortAll(all);
            return all.getLast().equals(node);
        }

        @Override
        public boolean moveTo(CaoNode parent) {
            return moveTo(parent, true);
        }

        public boolean moveTo(CaoNode parent, boolean moveToTop) {
            try {
                CaoAction action = node.getConnection().getAction(CaoAction.MOVE);
                if (action == null) return false;
                MoveConfiguration configuration =
                        (MoveConfiguration) action.createConfiguration(node, null);
                configuration.setNewParent(parent);
                OperationResult ret = action.doExecute(configuration, null);
                log().d("result", ret);
                boolean ret2 = ret != null && ret.isSuccessful();
                if (ret2) {
                    node.reload();
                    if (moveToTop) moveToTop();
                    Changes change = node.adaptTo(Changes.class);
                    if (change != null) change.moved();
                }
                return ret2;
            } catch (Exception e) {
                log().e("moveTo", node, parent, e);
            }
            return false;
        }

        @Override
        public boolean delete(boolean recursive) {
            try {
                CaoAction action = node.getConnection().getAction(CaoAction.DELETE);
                if (action == null) return false;
                DeleteConfiguration configuration =
                        (DeleteConfiguration) action.createConfiguration(node, null);
                configuration.setRecursive(recursive);
                OperationResult ret = action.doExecute(configuration, null);
                return ret != null && ret.isSuccessful();
            } catch (Exception e) {
                log().e("delete", node, recursive, e);
            }
            return false;
        }

        @Override
        public CaoNode createChildNode(String name, IProperties properties) {
            try {
                CaoAction action = node.getConnection().getAction(CaoAction.CREATE);
                if (action == null) return null;
                CreateConfiguration configuration =
                        (CreateConfiguration) action.createConfiguration(node, null);
                configuration.setName(name);
                configuration.getProperties().putAll(properties);
                OperationResult ret = action.doExecute(configuration, null);
                if (ret != null && ret.isSuccessful()) return ret.getResultAs();
            } catch (Exception e) {
                log().e("createChildNode", node, name, e);
            }
            return null;
        }

        @Override
        public boolean uploadRendition(String name, File file) {
            try {
                CaoAction action = node.getConnection().getAction(CaoAction.UPLOAD_RENDITION);
                if (action == null) return false;
                UploadRenditionConfiguration configuration =
                        (UploadRenditionConfiguration) action.createConfiguration(node, null);
                configuration.setRendition(name);
                configuration.setFile(file);
                OperationResult ret = action.doExecute(configuration, null);
                return ret != null && ret.isSuccessful();
            } catch (Exception e) {
                log().e("uploadRendition", node, name, e);
            }
            return false;
        }

        @Override
        public CaoNode copyTo(CaoNode parent, boolean recursive) {
            try {
                CaoAction action = node.getConnection().getAction(CaoAction.COPY);
                if (action == null) return null;
                CopyConfiguration configuration =
                        (CopyConfiguration) action.createConfiguration(node, null);
                configuration.setNewParent(parent);
                configuration.setRecursive(recursive);
                OperationResult ret = action.doExecute(configuration, null);
                if (ret != null && ret.isSuccessful()) return ret.getResultAs();
            } catch (Exception e) {
                log().e("copyTo", node, recursive, e);
            }
            return null;
        }

        @Override
        public boolean rename(String name) {
            try {
                CaoAction action = node.getConnection().getAction(CaoAction.RENAME);
                if (action == null) return false;
                RenameConfiguration configuration =
                        (RenameConfiguration) action.createConfiguration(node, null);
                configuration.setName(name);
                OperationResult ret = action.doExecute(configuration, null);
                return ret != null && ret.isSuccessful();
            } catch (Exception e) {
                log().e("rename", node, name, e);
            }
            return false;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setBehavior(Behavior<?> behavior) {
            this.behavior = (Behavior<Object>) behavior;
        }
    }

    public static class DefaultBehavior implements Behavior<Integer> {

        private String attributeName;

        public DefaultBehavior(String attributeName) {
            this.attributeName = attributeName;
        }

        @Override
        public int compare(CaoNode o1, CaoNode o2) {
            Integer p1 = getPosition(o1);
            Integer p2 = getPosition(o2);

            return comparePositions(p1, p2);
        }

        private int comparePositions(Integer p1, Integer p2) {
            if (p1 == null) return -1;
            return p1.compareTo(p2);
        }

        @Override
        public Integer getPosition(CaoNode node) {
            int ret = node.getInt(attributeName, -1);
            if (ret < 0) return null;
            return ret;
        }

        @Override
        public LinkedList<WritableNode> loadAll(CaoNode node) {
            return null;
        }

        @Override
        public Integer max(Integer a, Integer b) {
            if (a == null && b == null) return 0;
            if (a == null) return b;
            if (b == null) return a;
            return Math.max(a, b);
        }

        @Override
        public Integer inc(Integer pos) {
            if (pos == null) return 0;
            return pos + 1;
        }

        @Override
        public void setPosition(CaoNode node, Integer pos) {
            if (pos == null) node.remove(attributeName);
            else node.setInt(attributeName, pos);
        }

        @Override
        public boolean equals(Integer p1, Integer p2) {
            return MSystem.equals(p1, p2);
        }
    }
}
