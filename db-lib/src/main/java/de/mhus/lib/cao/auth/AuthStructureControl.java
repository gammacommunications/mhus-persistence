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

import java.io.File;

import de.mhus.lib.cao.CaoAspectFactory;
import de.mhus.lib.cao.CaoCore;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.aspect.StructureControl;
import de.mhus.lib.cao.util.MutableActionList;
import de.mhus.lib.core.IProperties;

public class AuthStructureControl implements CaoAspectFactory<StructureControl> {

    private AuthCore core;

    @Override
    public StructureControl getAspectFor(CaoNode node) {
        CaoNode orgNode = core.getInstance((AuthNode) node);
        return new Aspect(node, orgNode);
    }

    @Override
    public void doInitialize(CaoCore core, MutableActionList actionList) {
        this.core = (AuthCore) core;
    }

    private class Aspect implements StructureControl {

        @SuppressWarnings("unused")
        private CaoNode node;

        private StructureControl instance;
        private CaoNode orgNode;

        public Aspect(CaoNode node, CaoNode orgNode) {
            this.node = node;
            this.orgNode = orgNode;
            this.instance = orgNode.adaptTo(StructureControl.class);
            ;
        }

        @Override
        public void setBehavior(Behavior<?> behavior) {
            // TODO should use an adapter to hide nodes
            instance.setBehavior(behavior);
        }

        @Override
        public boolean moveUp() {
            if (!core.hasStructureAccess(orgNode)) return false;
            return instance.moveUp();
        }

        @Override
        public boolean moveDown() {
            if (!core.hasStructureAccess(orgNode)) return false;
            return instance.moveDown();
        }

        @Override
        public boolean moveToTop() {
            if (!core.hasStructureAccess(orgNode)) return false;
            return instance.moveToTop();
        }

        @Override
        public boolean moveToBottom() {
            if (!core.hasStructureAccess(orgNode)) return false;
            return instance.moveToBottom();
        }

        @Override
        public boolean moveAfter(CaoNode predecessor) {
            if (!core.hasStructureAccess(orgNode)) return false;
            if (predecessor instanceof AuthNode)
                return instance.moveAfter(((AuthNode) predecessor).instance);
            else return instance.moveAfter(predecessor);
        }

        @Override
        public int getPositionIndex() {
            return instance.getPositionIndex();
        }

        @Override
        public boolean isAtTop() {
            return instance.isAtTop();
        }

        @Override
        public boolean isAtBottom() {
            return instance.isAtBottom();
        }

        @Override
        public boolean moveTo(CaoNode parent) {
            if (!core.hasStructureAccess(orgNode)) return false;
            if (parent instanceof AuthNode) return instance.moveTo(((AuthNode) parent).instance);
            else return instance.moveTo(parent);
        }

        @Override
        public boolean delete(boolean recursive) {
            if (!core.hasDeleteAccess(orgNode)) return false;
            return instance.delete(recursive);
        }

        @Override
        public CaoNode createChildNode(String name, IProperties properties) {
            if (!core.hasCreateAccess(orgNode, name, properties)) return null;
            CaoNode res = instance.createChildNode(name, properties);
            if (res == null) return null;
            return new AuthNode(core, res);
        }

        @Override
        public boolean uploadRendition(String name, File file) {
            if (!core.hasContentWriteAccess(orgNode, name)) return false;
            return instance.uploadRendition(name, file);
        }

        @Override
        public CaoNode copyTo(CaoNode parent, boolean recursive) {
            if (!core.hasWriteAccess(parent)) return null;
            CaoNode res = null;
            if (parent instanceof AuthNode)
                res = instance.copyTo(((AuthNode) parent).instance, recursive);
            else res = instance.copyTo(parent, recursive);
            if (res == null) return null;
            return new AuthNode(core, res);
        }

        @Override
        public boolean rename(String name) {
            if (!core.hasStructureAccess(orgNode)) return false;
            return instance.rename(name);
        }

        @Override
        public boolean moveBefore(CaoNode successor) {
            if (!core.hasStructureAccess(orgNode)) return false;
            if (successor instanceof AuthNode)
                return instance.moveBefore(((AuthNode) successor).instance);
            else return instance.moveBefore(successor);
        }
    }
}
