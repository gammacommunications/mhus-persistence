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

import java.util.LinkedList;

import de.mhus.lib.cao.CaoAspectFactory;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.aspect.Changes;
import de.mhus.lib.core.MLog;
import de.mhus.lib.core.MSystem;
import de.mhus.lib.core.MPeriod;

public class DefaultChangesQueue extends MLog implements CaoAspectFactory<Changes> {

    public enum EVENT {
        DELETED,
        CREATED,
        MODIFIED,
        MOVED,
        UNLINK,
        LINK,
        BIG_CHANGE,
        RENAMED,
        RENDITION_MODIFIED,
        RENDITION_DELETED
    }

    private LinkedList<Change> queue = new LinkedList<>();

    private String name;

    private int maxQueueSize;

    private long firstBigChange = 0;

    private long maxBigChangeWait = MPeriod.SECOUND_IN_MILLISECOUNDS * 30;

    public DefaultChangesQueue() {
        this(30);
    }

    public DefaultChangesQueue(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public Changes getAspectFor(CaoNode node) {
        return new Aspect(node);
    }

    @Override
    public void doInitialize(CaoCore core, MutableActionList actionList) {
        name = core.getName();
    }

    public Change[] clearEventQueue() {
        // It's an smart queue .... act like it
        synchronized (queue) {

            // special logic for big changes - do not update all every second, wait for the end of
            // the big change
            if (queue.size() == 1 && queue.get(0).event == EVENT.BIG_CHANGE) {
                if (firstBigChange == 0) {
                    firstBigChange = System.currentTimeMillis();
                } else if (MPeriod.isTimeOut(firstBigChange, maxBigChangeWait)) {
                    firstBigChange = 0;
                    Change[] out = queue.toArray(new Change[queue.size()]);
                    queue.clear();
                    return out;
                }

            } else if (firstBigChange != 0) {
                firstBigChange = 0;
            }

            Change[] out = queue.toArray(new Change[queue.size()]);
            queue.clear();
            return out;
        }
    }

    public int getEventQueueSize() {
        return queue.size();
    }

    public class Change {

        private EVENT event;
        private String node;
        private String parent;
        private String info;

        public Change(EVENT event, String node, String parent, String info) {
            this.event = event;
            this.node = node;
            this.parent = parent;
            this.info = info;
        }

        public EVENT getEvent() {
            return event;
        }

        public String getNode() {
            return node;
        }

        public String getParent() {
            return parent;
        }

        public String getInfo() {
            return info;
        }

        @Override
        public String toString() {
            return MSystem.toString(this, event, node, info);
        }
    }

    private class Aspect implements Changes {

        private CaoNode node;

        public Aspect(CaoNode node) {
            this.node = node;
        }

        @Override
        public void deleted() {
            addEvent(EVENT.DELETED, node, node.getParent());
        }

        @Override
        public void modified() {
            addEvent(EVENT.MODIFIED, node, node.getParent());
        }

        @Override
        public void created() {
            addEvent(EVENT.CREATED, node, node.getParent());
        }

        @Override
        public void moved() {
            addEvent(EVENT.MOVED, node, node.getParent());
        }

        @Override
        public void movedFrom(CaoNode oldParent) {
            addEvent(EVENT.UNLINK, node, oldParent);
            addEvent(EVENT.LINK, node, node.getParent());
        }

        @Override
        public void renamed() {
            addEvent(EVENT.RENAMED, node, node.getParent());
        }

        @Override
        public void uploadedRendition(String rendition) {
            addEvent(EVENT.RENDITION_MODIFIED, node, node.getParent(), rendition);
        }

        @Override
        public void deletedRendition(String rendition) {
            addEvent(EVENT.RENDITION_DELETED, node, node.getParent(), rendition);
        }
    }

    public void addEvent(EVENT event, CaoNode node, CaoNode parent) {
        addEvent(event, node, parent, null);
    }

    public void addEvent(EVENT event, CaoNode node, CaoNode parent, String info) {
        // It's an smart queue .... act like it

        if (event == null || node == null) return;
        if (parent == null) parent = node.getParent();

        String nodeId = node.getId();
        String parentId = parent == null ? null : parent.getId();

        if (nodeId == null) return;

        synchronized (queue) {
            // queue is full
            if (queue.size() == 1 && queue.get(0).event == EVENT.BIG_CHANGE) {
                log().t(name, "Queue is already full");
                return;
            }

            // search the same or overwriting event ...
            for (Change item : queue) {
                if (item.getEvent() == event
                        && item.getNode().equals(nodeId)
                        && MSystem.equals(item.info, info)) {
                    item.parent = parentId; // maybe parent changed again
                    return;
                }
            }

            // queue is just full - remove all and set a full refresh event
            if (queue.size() > maxQueueSize) {
                queue.clear();
                queue.add(new Change(EVENT.BIG_CHANGE, null, null, null));
                log().d(name, "Event queue is full");
                return;
            }

            log().t(name, event, node, parent);
            queue.add(new Change(event, nodeId, parentId, info));
        }
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public long getMaxBigChangeWait() {
        return maxBigChangeWait;
    }

    public void setMaxBigChangeWait(long maxBigChangeWait) {
        this.maxBigChangeWait = maxBigChangeWait;
    }

    @Override
    public String toString() {
        return MSystem.toString(this, maxQueueSize, maxBigChangeWait, queue.size());
    }
}
