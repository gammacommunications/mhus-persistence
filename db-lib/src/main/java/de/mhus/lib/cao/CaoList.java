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
package de.mhus.lib.cao;

import java.util.LinkedList;

/**
 * CaoList class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class CaoList extends LinkedList<CaoNode> {

    /** */
    private static final long serialVersionUID = 1L;

    private CaoNode parent;

    public CaoList(CaoNode parent, CaoNode... nodes) {
        this.parent = parent;
        if (nodes != null) for (CaoNode n : nodes) add(n);
    }

    /**
     * Constructor for CaoList.
     *
     * @param parent a {@link de.mhus.lib.cao.CaoNode} object.
     */
    public CaoList(CaoNode parent) {
        this.parent = parent;
    }

    /**
     * Getter for the field <code>parent</code>.
     *
     * @return a {@link de.mhus.lib.cao.CaoNode} object.
     */
    public CaoNode getParent() {
        return parent;
    }

    public CaoList append(CaoNode... nodes) {
        for (CaoNode node : nodes) add(node);
        return this;
    }
}
