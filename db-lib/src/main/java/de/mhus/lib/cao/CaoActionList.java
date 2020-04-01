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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * The class will filter the actions and consider which one is more important. To fill it correctly
 * fill with the more special first and the more generic as last one.
 *
 * <p>Use the method CaoFactory.fillWithActions() to fill it correctly. The method will first ask
 * the drivers of the elements to fill this list. The drivers are asking the CaoApplications and it
 * will ask the CaoConnection by default You can change this behavior by overwriting the method in
 * it's extended classes.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class CaoActionList implements Iterable<CaoAction> {

    protected LinkedList<CaoAction> actions = new LinkedList<CaoAction>();
    protected HashMap<String, CaoAction> index = new HashMap<String, CaoAction>();

    /** {@inheritDoc} */
    @Override
    public Iterator<CaoAction> iterator() {
        return actions.iterator();
    }

    /**
     * getAction.
     *
     * @param actionName a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoAction} object.
     */
    public CaoAction getAction(String actionName) {
        return index.get(actionName);
    }
}
