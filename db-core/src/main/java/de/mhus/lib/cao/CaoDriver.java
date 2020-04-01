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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import de.mhus.lib.core.lang.MObject;

/**
 * Abstract CaoDriver class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public abstract class CaoDriver extends MObject {

    protected String scheme;

    /**
     * connect.
     *
     * @param uri a {@link java.lang.String} object.
     * @param authentication a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoConnection} object.
     * @throws java.net.URISyntaxException if any.
     */
    public CaoCore connect(String uri, String authentication) throws URISyntaxException {
        if (uri == null) return connect((URI) null, authentication);
        if (uri.indexOf(':') < 0) return connect(new File(uri).toURI(), authentication);
        return connect(new URI(uri), authentication);
    }

    /**
     * connect.
     *
     * @param uri a {@link java.net.URI} object.
     * @param authentication a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoConnection} object.
     */
    public abstract CaoCore connect(URI uri, String authentication);

    /**
     * Getter for the field <code>scheme</code>.
     *
     * @return a {@link java.lang.String} object.
     */
    public String getScheme() {
        return scheme;
    }

    /**
     * createLoginForm.
     *
     * @param uri a {@link java.net.URI} object.
     * @param authentication a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoLoginForm} object.
     */
    public abstract CaoLoginForm createLoginForm(URI uri, String authentication);
}
