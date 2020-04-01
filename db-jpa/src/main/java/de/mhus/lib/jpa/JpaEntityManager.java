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
package de.mhus.lib.jpa;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.Metamodel;

import org.apache.openjpa.persistence.OpenJPAEntityManager;

import de.mhus.lib.core.lang.MObject;

/**
 * JpaEntityManager class.
 *
 * @author mikehummel
 * @version $Id: $Id
 */
public class JpaEntityManager extends MObject implements EntityManager {

    private JpaManager manager;
    private EntityManager entityManager;

    /**
     * Constructor for JpaEntityManager.
     *
     * @param manager a {@link de.mhus.lib.jpa.JpaManager} object.
     * @param factory a {@link javax.persistence.EntityManagerFactory} object.
     * @param map a {@link java.util.Map} object.
     */
    @SuppressWarnings("rawtypes")
    public JpaEntityManager(JpaManager manager, EntityManagerFactory factory, Map map) {
        this.manager = manager;
        entityManager = factory.createEntityManager(map);
    }

    /**
     * injectObject.
     *
     * @param object a T object.
     * @param <T> a T object.
     * @return a T object.
     */
    public <T> T injectObject(T object) {
        if (object != null && object instanceof JpaInjection) {
            ((JpaInjection) object).doInjectJpa(this);
        }
        return object;
    }

    /**
     * Getter for the field <code>manager</code>.
     *
     * @return a {@link de.mhus.lib.jpa.JpaManager} object.
     */
    public JpaManager getManager() {
        return manager;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (entityManager == null) return;
        log().t("close");
        entityManager.close();
        entityManager = null;
    }

    /** {@inheritDoc} */
    @Override
    public void persist(Object entity) {
        log().t("persist");
        injectObject(entity);
        entityManager.persist(entity);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T merge(T entity) {
        log().t("merge", entity);
        injectObject(entity);
        return entityManager.merge(entity);
    }

    /** {@inheritDoc} */
    @Override
    public void remove(Object entity) {
        log().t("remove", entity);
        injectObject(entity);
        entityManager.remove(entity);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey) {
        log().t("find", entityClass, primaryKey);
        return injectObject(entityManager.find(entityClass, primaryKey));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
        log().t("find", entityClass, primaryKey, properties);
        return injectObject(entityManager.find(entityClass, primaryKey, properties));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
        log().t("find", entityClass, primaryKey, lockMode);
        return injectObject(entityManager.find(entityClass, primaryKey, lockMode));
    }

    /** {@inheritDoc} */
    @Override
    public <T> T find(
            Class<T> entityClass,
            Object primaryKey,
            LockModeType lockMode,
            Map<String, Object> properties) {
        log().t("find", entityClass, primaryKey, lockMode, properties);
        return entityManager.find(entityClass, primaryKey, lockMode, properties);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T getReference(Class<T> entityClass, Object primaryKey) {
        log().t("getReference", entityClass, primaryKey);
        return injectObject(entityManager.getReference(entityClass, primaryKey));
    }

    /** {@inheritDoc} */
    @Override
    public void flush() {
        log().t("flush");
        entityManager.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void setFlushMode(FlushModeType flushMode) {
        log().t("flush mode", flushMode);
        entityManager.setFlushMode(flushMode);
    }

    /** {@inheritDoc} */
    @Override
    public FlushModeType getFlushMode() {
        return entityManager.getFlushMode();
    }

    /** {@inheritDoc} */
    @Override
    public void lock(Object entity, LockModeType lockMode) {
        log().t("lock", entity, lockMode);
        injectObject(entity);
        entityManager.lock(entity, lockMode);
    }

    /** {@inheritDoc} */
    @Override
    public void lock(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        log().t("lock", entity, lockMode, properties);
        injectObject(entity);
        entityManager.lock(entity, lockMode, properties);
    }

    /** {@inheritDoc} */
    @Override
    public void refresh(Object entity) {
        log().t("refresh", entity);
        injectObject(entity);
        entityManager.refresh(entity);
    }

    /** {@inheritDoc} */
    @Override
    public void refresh(Object entity, Map<String, Object> properties) {
        log().t("refresh", entity, properties);
        injectObject(entity);
        entityManager.refresh(entity, properties);
    }

    /** {@inheritDoc} */
    @Override
    public void refresh(Object entity, LockModeType lockMode) {
        log().t("refresh", entity, lockMode);
        injectObject(entity);
        entityManager.refresh(entity, lockMode);
    }

    /** {@inheritDoc} */
    @Override
    public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        log().t("refresh", entity, lockMode, properties);
        injectObject(entity);
        entityManager.refresh(entity, lockMode, properties);
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        log().t("clear");
        entityManager.clear();
    }

    /** {@inheritDoc} */
    @Override
    public void detach(Object entity) {
        log().t("detach", entity);
        injectObject(entity);
        entityManager.detach(entity);
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(Object entity) {
        log().t("contains", entity);
        injectObject(entity);
        return entityManager.contains(entity);
    }

    /** {@inheritDoc} */
    @Override
    public LockModeType getLockMode(Object entity) {
        injectObject(entity);
        return entityManager.getLockMode(entity);
    }

    /** {@inheritDoc} */
    @Override
    public void setProperty(String propertyName, Object value) {
        log().t("property", propertyName, value);
        entityManager.setProperty(propertyName, value);
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, Object> getProperties() {
        return entityManager.getProperties();
    }

    /** {@inheritDoc} */
    @Override
    public JpaQuery<?> createQuery(String qlString) {
        log().t("create query", qlString);
        return new JpaQuery<Object>(this, entityManager.createQuery(qlString));
    }

    /** {@inheritDoc} */
    @Override
    public <T> JpaQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
        log().t("create query", criteriaQuery);
        return new JpaQuery<T>(this, entityManager.createQuery(criteriaQuery));
    }

    /** {@inheritDoc} */
    @Override
    public <T> JpaQuery<T> createQuery(String qlString, Class<T> resultClass) {
        log().t("create query", qlString, resultClass);
        return new JpaQuery<T>(this, entityManager.createQuery(qlString, resultClass));
    }

    /** {@inheritDoc} */
    @Override
    public Query createNamedQuery(String name) {
        log().t("create named query", name);
        return new JpaQuery<Object>(this, entityManager.createNamedQuery(name));
    }

    /** {@inheritDoc} */
    @Override
    public <T> JpaQuery<T> createNamedQuery(String name, Class<T> resultClass) {
        log().t("create named query", name, resultClass);
        return new JpaQuery<T>(this, entityManager.createNamedQuery(name, resultClass));
    }

    /** {@inheritDoc} */
    @Override
    public Query createNativeQuery(String sqlString) {
        log().t("create native query", sqlString);
        return entityManager.createNativeQuery(sqlString);
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("rawtypes")
    public Query createNativeQuery(String sqlString, Class resultClass) {
        log().t("create native query", sqlString, resultClass);
        return entityManager.createNativeQuery(sqlString, resultClass);
    }

    /** {@inheritDoc} */
    @Override
    public Query createNativeQuery(String sqlString, String resultSetMapping) {
        log().t("create native query", sqlString, resultSetMapping);
        return entityManager.createNativeQuery(sqlString, resultSetMapping);
    }

    /** {@inheritDoc} */
    @Override
    public void joinTransaction() {
        log().t("join transaction");
        entityManager.joinTransaction();
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> cls) {
        log().t("unwrap", cls);
        return injectObject(entityManager.unwrap(cls));
    }

    /** {@inheritDoc} */
    @Override
    public Object getDelegate() {
        return entityManager.getDelegate();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
        return entityManager != null && entityManager.isOpen();
    }

    /** {@inheritDoc} */
    @Override
    public EntityTransaction getTransaction() {
        return entityManager.getTransaction();
    }

    /** {@inheritDoc} */
    @Override
    public CriteriaBuilder getCriteriaBuilder() {
        return entityManager.getCriteriaBuilder();
    }

    /** {@inheritDoc} */
    @Override
    public Metamodel getMetamodel() {
        return entityManager.getMetamodel();
    }

    /** begin. */
    public void begin() {
        log().t("begin");
        getTransaction().begin();
    }

    /** commit. */
    public void commit() {
        log().t("commit");
        getTransaction().commit();
    }

    /** rollback. */
    public void rollback() {
        log().t("rollback");
        getTransaction().rollback();
    }

    /** {@inheritDoc} */
    @Override
    public EntityManagerFactory getEntityManagerFactory() {
        return manager;
    }

    /**
     * copy.
     *
     * @param object a T object.
     * @param <T> a T object.
     * @return a T object.
     */
    public <T> T copy(T object) {
        log().t("copy", object);
        return injectObject(((OpenJPAEntityManager) entityManager).detachCopy(object));
    }
}
