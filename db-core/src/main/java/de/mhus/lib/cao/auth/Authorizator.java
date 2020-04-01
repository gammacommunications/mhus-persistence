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

import java.util.Collection;

import de.mhus.lib.cao.CaoAction;
import de.mhus.lib.cao.CaoAspect;
import de.mhus.lib.cao.CaoNode;
import de.mhus.lib.cao.action.CaoConfiguration;
import de.mhus.lib.core.IProperties;

public interface Authorizator {

    boolean hasReadAccess(CaoNode node);

    boolean hasWriteAccess(CaoNode node);

    boolean hasActionAccess(CaoAction action);

    boolean hasReadAccess(CaoNode node, String name);

    boolean hasContentAccess(CaoNode node, String rendition);

    boolean hasWriteAccess(CaoNode node, String name);

    boolean hasAspectAccess(CaoNode node, Class<? extends CaoAspect> ifc);

    String mapReadName(CaoNode node, String name);

    String mapReadRendition(CaoNode node, String rendition);

    Collection<String> mapReadNames(CaoNode node, Collection<String> set);

    String mapWriteName(CaoNode node, String name);

    boolean hasActionAccess(CaoConfiguration configuration, CaoAction action);

    boolean hasStructureAccess(CaoNode node);

    boolean hasDeleteAccess(CaoNode node);

    boolean hasCreateAccess(CaoNode node, String name, IProperties properties);

    boolean hasContentWriteAccess(CaoNode node, String rendition);
}
