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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import de.mhus.lib.annotations.activator.DefaultImplementation;
import de.mhus.lib.core.lang.IBase;
import de.mhus.lib.core.lang.MObject;

/**
 * CaoDriverManager class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
@DefaultImplementation(DriverManagerImpl.class)
public class CaoDriverManager extends MObject implements IBase {

    protected HashMap<String, CaoDriver> schemes = new HashMap<String, CaoDriver>();

    /**
     * getScheme.
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoDriver} object.
     */
    public CaoDriver getScheme(String name) {
        return schemes.get(name);
    }

    /**
     * connect.
     *
     * @param uri a {@link java.lang.String} object.
     * @param authentication a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoConnection} object.
     * @throws java.net.URISyntaxException if any.
     */
    public CaoConnection connect(String uri, String authentication) throws URISyntaxException {
        return connect(new URI(uri), authentication);
    }

    /**
     * connect.
     *
     * @param uri a {@link java.net.URI} object.
     * @param authentication a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoConnection} object.
     */
    public CaoConnection connect(URI uri, String authentication) {
        log().t("connect", uri);
        CaoDriver scheme = getScheme(uri.getScheme());
        if (scheme == null) return null;
        return scheme.connect(uri, authentication);
    }
}
