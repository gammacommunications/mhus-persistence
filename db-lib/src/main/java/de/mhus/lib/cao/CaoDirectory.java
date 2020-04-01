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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

import de.mhus.lib.core.lang.IBase;
import de.mhus.lib.core.lang.MObject;

/**
 * CaoDirectory class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class CaoDirectory extends MObject implements IBase {

    protected HashMap<String, CaoConnection> schemes = new HashMap<String, CaoConnection>();

    /**
     * getScheme.
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.cao.CaoConnection} object.
     */
    public CaoConnection getScheme(String name) {
        return schemes.get(name);
    }

    /**
     * getInputStream.
     *
     * @param uri a {@link java.net.URI} object.
     * @return a {@link java.io.InputStream} object.
     * @throws java.io.IOException if any.
     */
    public InputStream getInputStream(URI uri) throws IOException {
        String schemeName = uri.getScheme();
        log().t(schemeName);
        CaoConnection scheme = getScheme(schemeName);
        if (scheme == null) return null;

        return scheme.getResourceByPath(uri.getPath()).getInputStream();
    }

    /**
     * getInputStream.
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link java.io.InputStream} object.
     * @throws java.net.URISyntaxException if any.
     * @throws java.io.IOException if any.
     */
    public InputStream getInputStream(String name) throws URISyntaxException, IOException {
        return getInputStream(new URI(name));
    }

    /**
     * getResource.
     *
     * @param uri a {@link java.net.URI} object.
     * @return a {@link java.net.URL} object.
     */
    public URL getResource(URI uri) {
        String schemeName = uri.getScheme();
        log().t(schemeName);
        CaoConnection scheme = getScheme(schemeName);
        if (scheme == null) return null;
        return scheme.getResourceByPath(uri.getPath()).getUrl();
    }

    /**
     * getResource.
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link java.net.URL} object.
     * @throws java.net.URISyntaxException if any.
     */
    public URL getResource(String name) throws URISyntaxException {
        URI uri = new URI(name);
        return getResource(uri);
    }
}
