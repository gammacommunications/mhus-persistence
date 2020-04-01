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
package de.mhus.lib.cao.aspect;

import java.io.File;
import java.util.Comparator;
import java.util.LinkedList;

import de.mhus.lib.cao.CaoAspect;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.util.WritableNode;
import de.mhus.lib.core.IProperties;

public interface StructureControl extends CaoAspect {

    void setBehavior(Behavior<?> behavior);

    boolean moveUp();

    boolean moveDown();

    boolean moveToTop();

    boolean moveToBottom();

    boolean moveAfter(CaoNode predecessor);

    int getPositionIndex();

    boolean isAtTop();

    boolean isAtBottom();

    boolean moveTo(CaoNode parent);

    boolean delete(boolean recursive);

    CaoNode createChildNode(String name, IProperties properties);

    boolean uploadRendition(String name, File file);

    CaoNode copyTo(CaoNode parent, boolean recursive);

    boolean rename(String name);

    public static interface Behavior<S> extends Comparator<CaoNode> {
        S getPosition(CaoNode node);

        LinkedList<WritableNode> loadAll(CaoNode node);

        S max(S max, S pos);

        S inc(S pos);

        void setPosition(CaoNode node, S pos);

        boolean equals(S p1, S p2);
    }

    boolean moveBefore(CaoNode successor);
}
