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
package de.mhus.lib.adb.query;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.mhus.lib.basics.consts.Identifier;
import de.mhus.lib.core.parser.AttributeMap;
import de.mhus.lib.core.pojo.MPojo;

/**
 * AQuery class.
 *
 * @author mikehummel
 * @version $Id: $Id
 * @param <T>
 */
public class AQuery<T> extends APrint {

    private LinkedList<AOperation> operations;
    private Class<? extends T> type;
    private ACreateContext context;
    private int unique = 0;
    private AttributeMap map;

    /**
     * Constructor for AQuery.
     *
     * @param type a {@link java.lang.Class} object.
     * @param operations a {@link de.mhus.lib.adb.query.AOperation} object.
     */
    public AQuery(Class<T> type, AOperation... operations) {
        this.type = type;
        this.operations = new LinkedList<>();
        for (AOperation o : operations) this.operations.add(o);
    }

    /**
     * @param mask Masquerading Base Type
     * @param type Real Type querying for
     * @param operations Initial operations
     */
    public AQuery(Class<T> mask, Class<? extends T> type, AOperation... operations) {
        // mask is never used, it's only for the template type definition
        this.type = type;
        this.operations = new LinkedList<>();
        for (AOperation o : operations) this.operations.add(o);
    }

    /**
     * Getter for the field <code>type</code>.
     *
     * @return a {@link java.lang.Class} object.
     */
    public Class<? extends T> getType() {
        return type;
    }

    /**
     * getAttributes.
     *
     * @return a {@link java.util.Map} object.
     */
    public Map<String, Object> getAttributes() {
        if (map == null) {
            map = new AttributeMap();
            getAttributes(this, map);
        }
        return map;
    }

    /** {@inheritDoc} */
    @Override
    public void getAttributes(AQuery<?> query, AttributeMap map) {
        for (AOperation operation : operations) operation.getAttributes(query, map);
    }

    /**
     * eq.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> eq(AAttribute left, AAttribute right) {
        operations.add(Db.eq(left, right));
        return this;
    }

    /**
     * eq.
     *
     * @param attr a {@link java.lang.String} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> eq(String attr, Object value) {
        operations.add(Db.eq(Db.attr(attr), Db.value(type, attr, value)));
        return this;
    }

    /**
     * eq.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> eq(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.eq(Db.attr(name), Db.value(type, name, value)));
        return this;
    }

    /**
     * ne.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> ne(AAttribute left, AAttribute right) {
        operations.add(Db.ne(left, right));
        return this;
    }

    /**
     * ne.
     *
     * @param left a {@link java.lang.String} object.
     * @param right a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> ne(String left, Object right) {
        operations.add(Db.ne(Db.attr(left), Db.value(type, left, right)));
        return this;
    }

    /**
     * ne.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> ne(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.ne(Db.attr(name), Db.value(getter.getClazz(), name, value)));
        return this;
    }

    /**
     * lt.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> lt(AAttribute left, AAttribute right) {
        operations.add(Db.lt(left, right));
        return this;
    }

    /**
     * lt.
     *
     * @param left a {@link java.lang.String} object.
     * @param right a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> lt(String left, Object right) {
        operations.add(Db.lt(Db.attr(left), Db.value(type, left, right)));
        return this;
    }

    /**
     * lt.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> lt(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.lt(Db.attr(name), Db.value(getter.getClazz(), name, value)));
        return this;
    }

    /**
     * le.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> le(AAttribute left, AAttribute right) {
        operations.add(Db.le(left, right));
        return this;
    }

    /**
     * le.
     *
     * @param left a {@link java.lang.String} object.
     * @param right a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> le(String left, Object right) {
        operations.add(Db.le(Db.attr(left), Db.value(type, left, right)));
        return this;
    }

    /**
     * le.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> le(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.le(Db.attr(name), Db.value(getter.getClazz(), name, value)));
        return this;
    }

    /**
     * gt.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> gt(AAttribute left, AAttribute right) {
        operations.add(Db.gt(left, right));
        return this;
    }

    /**
     * gt.
     *
     * @param left a {@link java.lang.String} object.
     * @param right a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> gt(String left, Object right) {
        operations.add(Db.gt(Db.attr(left), Db.value(type, left, right)));
        return this;
    }

    /**
     * gt.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> gt(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.gt(Db.attr(name), Db.value(getter.getClazz(), name, value)));
        return this;
    }

    /**
     * ge.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> ge(AAttribute left, AAttribute right) {
        operations.add(Db.ge(left, right));
        return this;
    }

    /**
     * ge.
     *
     * @param left a {@link java.lang.String} object.
     * @param right a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> ge(String left, Object right) {
        operations.add(Db.ge(Db.attr(left), Db.value(type, left, right)));
        return this;
    }

    /**
     * ge.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> ge(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.ge(Db.attr(name), Db.value(getter.getClazz(), name, value)));
        return this;
    }

    /**
     * el.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> el(AAttribute left, AAttribute right) {
        operations.add(Db.el(left, right));
        return this;
    }

    /**
     * el.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> el(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.el(Db.attr(name), Db.value(type, name, value)));
        return this;
    }

    /**
     * eg.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> eg(AAttribute left, AAttribute right) {
        operations.add(Db.eg(left, right));
        return this;
    }

    /**
     * eg.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> eg(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.eg(Db.attr(name), Db.value(getter.getClazz(), name, value)));
        return this;
    }

    /**
     * like.
     *
     * @param left a {@link java.lang.String} object.
     * @param right a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> like(String left, String right) {
        operations.add(Db.like(Db.attr(left), Db.value(type, left, right)));
        return this;
    }

    /**
     * like.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> like(AAttribute left, AAttribute right) {
        operations.add(Db.like(left, right));
        return this;
    }

    /**
     * like.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @param value a {@link java.lang.Object} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> like(Identifier getter, Object value) {
        String name = MPojo.toAttributeName(getter);
        operations.add(Db.like(Db.attr(name), Db.value(getter.getClazz(), name, value)));
        return this;
    }

    /**
     * and.
     *
     * @param parts a {@link de.mhus.lib.adb.query.APart} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> and(APart... parts) {
        operations.add(Db.and(parts));
        return this;
    }

    /**
     * or.
     *
     * @param parts a {@link de.mhus.lib.adb.query.APart} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> or(APart... parts) {
        operations.add(Db.or(parts));
        return this;
    }

    /**
     * order.
     *
     * @param order a {@link de.mhus.lib.adb.query.AOrder} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> order(AOrder order) {
        operations.add(order);
        return this;
    }

    /**
     * asc.
     *
     * @param attr a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> asc(String attr) {
        operations.add(new AOrder(type, attr, true));
        return this;
    }

    /**
     * asc.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> asc(Identifier getter) {
        return asc(MPojo.toAttributeName(getter));
    }

    /**
     * desc.
     *
     * @param attr a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> desc(String attr) {
        operations.add(new AOrder(type, attr, false));
        return this;
    }

    /**
     * desc.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> desc(Identifier getter) {
        return desc(MPojo.toAttributeName(getter));
    }

    /**
     * literal.
     *
     * @param parts a {@link de.mhus.lib.adb.query.APart} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> literal(APart... parts) {
        operations.add(Db.literal(parts));
        return this;
    }

    /**
     * literal.
     *
     * @param literal a {@link de.mhus.lib.adb.query.ALiteral} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> literal(ALiteral literal) {
        operations.add(Db.literal(literal));
        return this;
    }

    /**
     * not.
     *
     * @param part a {@link de.mhus.lib.adb.query.APart} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> not(APart part) {
        operations.add(Db.not(part));
        return this;
    }

    /**
     * in.
     *
     * @param left
     * @param right
     * @return the query
     */
    public AQuery<T> in(Identifier left, Object... right) {
        operations.add(Db.in(left, right));
        return this;
    }

    /**
     * in.
     *
     * @param left a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> in(AAttribute left, AAttribute... right) {
        operations.add(Db.in(left, new AList(right)));
        return this;
    }

    /**
     * in.
     *
     * @param left a {@link java.util.function.Function} object.
     * @param right a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> in(Identifier left, AAttribute... right) {
        operations.add(Db.in(Db.attr(MPojo.toAttributeName(left)), new AList(right)));
        return this;
    }

    /**
     * Append a sub query compare element.
     *
     * @param left Name of the Attribute in the base query (WHERE [left] IN (...) )
     * @param projection Name of the attribute in the sub query (select [projection] FROM)
     * @param subQuery The subquery itself
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> in(AAttribute left, AAttribute projection, AQuery<?> subQuery) {
        operations.add(Db.in(left, projection, subQuery));
        return this;
    }

    /**
     * in.
     *
     * @param left a {@link java.lang.String} object.
     * @param projection a {@link java.lang.String} object.
     * @param subQuery a {@link de.mhus.lib.adb.query.AQuery} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> in(String left, String projection, AQuery<?> subQuery) {
        operations.add(Db.in(Db.attr(left), Db.attr(projection), subQuery));
        return this;
    }

    /**
     * in.
     *
     * @param left a {@link java.lang.String} object.
     * @param projection a {@link java.lang.String} object.
     * @param subQuery a {@link de.mhus.lib.adb.query.AQuery} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> in(Identifier left, Identifier projection, AQuery<?> subQuery) {
        operations.add(
                Db.in(
                        Db.attr(MPojo.toAttributeName(left)),
                        Db.attr(MPojo.toAttributeName(projection)),
                        subQuery));
        return this;
    }

    /**
     * limit.
     *
     * @param limit a int.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> limit(int limit) {
        operations.add(Db.limit(limit));
        return this;
    }

    /**
     * isNull.
     *
     * @param attr a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> isNull(AAttribute attr) {
        operations.add(Db.isNull(attr));
        return this;
    }

    /**
     * isNull.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> isNull(Identifier getter) {
        operations.add(Db.isNull(Db.attr(MPojo.toAttributeName(getter))));
        return this;
    }

    /**
     * isNotNull.
     *
     * @param attr a {@link de.mhus.lib.adb.query.AAttribute} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> isNotNull(AAttribute attr) {
        operations.add(Db.isNotNull(attr));
        return this;
    }

    /**
     * isNotNull.
     *
     * @param getter a {@link java.util.function.Function} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     * @since 3.3.0
     */
    public AQuery<T> isNotNull(Identifier getter) {
        operations.add(Db.isNotNull(Db.attr(MPojo.toAttributeName(getter))));
        return this;
    }

    /**
     * isNull.
     *
     * @param attr a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> isNull(String attr) {
        operations.add(Db.isNull(Db.attr(attr)));
        return this;
    }

    /**
     * isNotNull.
     *
     * @param attr a {@link java.lang.String} object.
     * @return a {@link de.mhus.lib.adb.query.AQuery} object.
     */
    public AQuery<T> isNotNull(String attr) {
        operations.add(Db.isNotNull(Db.attr(attr)));
        return this;
    }

    public ACreateContext getContext() {
        return context;
    }

    public void setContext(ACreateContext context) {
        this.context = context;
    }

    public List<AOperation> getOperations() {
        return operations;
    }

    public void append(APart part) {
        operations.add(part);
    }

    public synchronized int nextUnique() {
        return ++unique;
    }

    public void doFinal() {
        getAttributes();
    }

    public boolean isFinal() {
        return map != null;
    }

    //	@SuppressWarnings("unchecked")
    //	private synchronized void initRecorder() {
    //		if (recorder == null)
    //			recorder = (Recorder<T>) RecordingObject.create(type);
    //	}

}
