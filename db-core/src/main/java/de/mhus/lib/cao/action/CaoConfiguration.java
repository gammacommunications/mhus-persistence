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
package de.mhus.lib.cao.action;

import de.mhus.lib.cao.CaoList;
import de.mhus.lib.core.IProperties;
import de.mhus.lib.core.MProperties;
import de.mhus.lib.core.config.IConfig;
import de.mhus.lib.form.DataSource;
import de.mhus.lib.form.MutableMForm;
import de.mhus.lib.form.UiComponent;

public abstract class CaoConfiguration extends MutableMForm {

    protected MProperties properties = new MProperties();
    protected CaoConfiguration con;
    protected CaoList list;
    protected MyDataSource dataSource;

    /**
     * @param con Connection
     * @param list
     * @param model If null use the default model
     */
    public CaoConfiguration(CaoConfiguration con, CaoList list, IConfig model) {
        super(null, null, model);
        if (this.model == null) this.model = createDefaultModel();
        this.con = con;
        this.list = list;
        dataSource = new MyDataSource();
        setDataSource(dataSource);
    }

    protected abstract IConfig createDefaultModel();

    public IProperties getProperties() {
        return properties;
    }

    public CaoConfiguration getConnection() {
        return con;
    }

    public CaoList getList() {
        return list;
    }

    private class MyDataSource implements DataSource {
        @Override
        public boolean getBoolean(UiComponent component, String name, boolean def) {
            return properties.getBoolean(component.getName() + "." + name, def);
        }

        @Override
        public int getInt(UiComponent component, String name, int def) {
            return properties.getInt(component.getName() + "." + name, def);
        }

        @Override
        public String getString(UiComponent component, String name, String def) {
            return properties.getString(component.getName() + "." + name, def);
        }

        @Override
        public Object getObject(UiComponent component, String name, Object def) {
            Object val = properties.getProperty(component.getName() + "." + name);
            if (val == null) return def;
            return val;
        }

        @Override
        public Object getObject(String name, Object def) {
            Object val = properties.getProperty(name);
            if (val == null) return def;
            return val;
        }

        @Override
        public void setObject(UiComponent component, String name, Object value) {
            properties.put(name, value);
        }

        @Override
        public DataSource getNext() {
            return null;
        }
    }
}
