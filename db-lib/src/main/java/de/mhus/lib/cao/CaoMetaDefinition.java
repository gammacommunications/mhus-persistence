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

public class CaoMetaDefinition implements CaoAspect {

    public enum TYPE {
        STRING,
        BOOLEAN,
        LONG,
        DOUBLE,
        DATETIME,
        LIST,
        OBJECT,
        TEXT,
        BINARY,
        ELEMENT
    }

    private TYPE type;
    private String name;
    private String nls;
    private long size;
    private String[] categories;
    private CaoDriver driver;

    public CaoMetaDefinition(
            CaoMetadata meta, String name, TYPE type, String nls, long size, String... categories) {
        this.name = name;
        this.type = type;
        this.nls = nls;
        this.size = size;
        this.categories = categories;
        this.driver = meta.getDriver();
    }

    public TYPE getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getNls() {
        return nls;
    }

    public long getSize() {
        return size;
    }

    public String[] getCategories() {
        return categories;
    }

    public final CaoDriver getDriver() {
        return driver;
    }

    public boolean hasCategory(String category) {
        if (category == null || categories == null) return false;
        for (String c : categories) if (c != null && c.equals(category)) return true;
        return false;
    }
}
