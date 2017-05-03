/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2003 - 2004 Greg Luck.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution, if
 *    any, must include the following acknowlegement:
 *       "This product includes software developed by Greg Luck
 *       (http://sourceforge.net/users/gregluck) and contributors.
 *       See http://sourceforge.net/project/memberlist.php?group_id=93232
 *       for a list of contributors"
 *    Alternately, this acknowledgement may appear in the software itself,
 *    if and wherever such third-party acknowlegements normally appear.
 *
 * 4. The names "EHCache" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For written
 *    permission, please contact Greg Luck (gregluck at users.sourceforge.net).
 *
 * 5. Products derived from this software may not be called "EHCache"
 *    nor may "EHCache" appear in their names without prior written
 *    permission of Greg Luck.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL GREG LUCK OR OTHER
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by contributors
 * individuals on behalf of the EHCache project.  For more
 * information on EHCache, please see <http://ehcache.sourceforge.net/>.
 *
 */


package net.sf.ehcache.store;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.Element;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * An implementation of a MemoryStore.
 * <p/>
 * This uses {@link java.util.LinkedHashMap} as its backing map. It uses the {@link java.util.LinkedHashMap} LRU
 * feature. LRU for this implementation means least recently accessed.
 *
 * @author <a href="mailto:gluck@thoughtworks.com">Greg Luck</a>
 * @version $Id: MemoryStore.java,v 1.1.1.1 2005/01/27 18:15:03 pents90 Exp $
 */
public class MemoryStore implements Store {
    private static final Log LOG = LogFactory.getLog(MemoryStore.class.getName());



    /**
     * Map where items are stored by key
     */
    private Map map;

    /**
     * The cache this store is associated with
     */
    private Cache cache;

    /**
     * The DiskStore associated with this MemoryStore
     */
    private DiskStore diskStore;

    /**
     * status
     */
    private int status;

    /**
     * Required for deserialization
     */
    private MemoryStore() {
        return;
    }

    /**
     * Constructor for the MemoryStore object
     * The backing {@link java.util.LinkedHashMap} is created with LRU by access order.
     */
    public MemoryStore(Cache cache, DiskStore diskStore) {
        status = Store.STATUS_UNINITIALISED;
        this.cache = cache;
        this.diskStore = diskStore;

        try {
            map = loadMapInstance();
        } catch (CacheException e) {
            LOG.error(cache.getName() + "Cache: Cannot start MemoryStore", e);
            return;
        }
        LOG.debug("initialized MemoryStore for " + cache.getName());
        status = Store.STATUS_ALIVE;
    }

    /**
     * Tries to load a {@link java.util.LinkedHashMap} (JDK1.4) and then
     * tries to load an {@link org.apache.commons.collections.LRUMap}.
     * <p/>
     * This way applications running JDK1.4 do not have a dependency
     * on Apache commons-collections.
     *
     * @return a Map, being either {@link java.util.LinkedHashMap} or
     */
    public Map loadMapInstance() throws CacheException {
        //First try to load java.util.LinkedHashMap, which is preferred.
        try {
            Class.forName("java.util.LinkedHashMap");
            Map candidateMap = new SpoolingLinkedHashMap();
            if (LOG.isDebugEnabled()) {
                LOG.debug(cache.getName() + " Cache: Using SpoolingLinkedHashMap implementation");
            }
            return candidateMap;
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(cache.getName() + " Cache: Cannot find java.util.LinkedHashMap");
            }
        }
        //Secondly, try and load org.apache.commons.collections.LRUMap
        try {
            Class.forName("org.apache.commons.collections.LRUMap");
            Map candidateMap = new SpoolingLRUMap();
            if (LOG.isDebugEnabled()) {
                LOG.debug(cache.getName() + " Cache: Using SpoolingLRUMap implementation");
            }
            return candidateMap;
        } catch (Exception e) {
            //Give up
            throw new CacheException(cache.getName()
                    + "Cache: Cannot find org.apache.commons.collections.LRUMap.");
        }
    }

    /**
     * Puts an item in the cache. Note that this automatically results in
     * {@link SpoolingLinkedHashMap#removeEldestEntry} being called.
     *
     * @param element the element to add
     */
    public synchronized void put(Element element) {
        map.put(element.getKey(), element);
    }

    /**
     * Remove all of the elements from the cache.
     */
    public synchronized void removeAll() {
        map.clear();
    }

    /**
     * Gets an item from the cache
     * <p/>
     * The last access time in {@link net.sf.ehcache.Element} is updated.
     *
     * @param key the cache key
     * @return the element, or null if there was no match for the key
     */
    public synchronized Element get(Serializable key) {
        Element cacheElement = (Element) map.get(key);

        if (cacheElement != null) {
            cacheElement.updateAccessStatistics();
            if (LOG.isTraceEnabled()) {
                LOG.trace(cache.getName() + "Cache: MemoryStore hit for " + key);
            }
        } else if (LOG.isTraceEnabled()) {
            LOG.trace(cache.getName() + "Cache: MemoryStore miss for " + key);
        }
        return cacheElement;
    }

    /**
     * Gets an item from the cache, without updating Element statistics
     * <p/>
     * The last access time in {@link net.sf.ehcache.Element} is updated.
     *
     * @param key the cache key
     * @return the element, or null if there was no match for the key
     */
    public synchronized Element getQuiet(Serializable key) {
        Element cacheElement = (Element) map.get(key);

        if (cacheElement != null) {
            //cacheElement.updateAccessStatistics(); Don't update statistics
            if (LOG.isTraceEnabled()) {
                LOG.trace(cache.getName() + "Cache: Quiet MemoryStore hit for " + key);
            }
        } else if (LOG.isTraceEnabled()) {
            LOG.trace(cache.getName() + "Cache: Quiet MemoryStore miss for " + key);
        }
        return cacheElement;
    }

    /**
     * Removes an item from the cache.
     *
     * @param key the key, usually a String
     * @return true if at least one object was removed.
     */
    public synchronized boolean remove(Serializable key) {
        boolean removed = false;

        // remove single item.
        if (map.remove(key) != null) {
            removed = true;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(cache.getName() + "Cache: Cannot remove entry as key " + key + " was not found");
            }
        }
        return removed;
    }

    /**
     * Gets an Array of the keys for all elements in the memory cache
     * <p/>
     * Does not check for expired entries
     *
     * @return An Object[]
     */
    public synchronized Object[] getKeyArray() {
        return map.keySet().toArray();
    }

    /**
     * Returns the current cache size.
     *
     * @return The size value
     */
    public int getSize() {
        return map.size();
    }


    /**
     * Gets the cache that the MemoryStore is used by
     */
    public Cache getCache() {
        return cache;
    }

    /**
     * Gets the status of the MemoryStore.
     * Can be {@link net.sf.ehcache.store.Store#STATUS_ALIVE} or
     * {@link net.sf.ehcache.store.Store#STATUS_ERROR}
     */
    public int getStatus() {
        return status;
    }

    /**
     * Returns the cache type.
     *
     * @return The cacheType value
     */
    public int getCacheType() {
        return Store.CACHE_HUB;
    }

    /**
     * Returns the cache name.
     */
    public String getName() {
        return cache.getName();
    }

    /**
     * Prepares for shutdown.
     */
    public synchronized void dispose() {
        if (status == STATUS_DISPOSED) {
            return;
        }
        status = STATUS_DISPOSED;
        if (cache.isDiskPersistent()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(cache.getName() + " is persistent. Spooling " + map.size() + " elements to the disk store.");
            }
            spoolAllToDisk();
        }
        map.clear();

        //release reference to cache
        cache = null;
    }

    /**
     * Measures the size of the memory store by measuring the serialized
     * size of all elements.
     * <p/>
     * Warning: This method can be very expensive to run. Allow approximately 1 second
     * per 1MB of entries. Running this method could create liveness problems
     * because the object lock is held for a long period
     *
     * @return the size, in bytes
     */
    public synchronized long getSizeInBytes() throws CacheException {
        long sizeInBytes = 0;
        for (Iterator iterator = map.values().iterator(); iterator.hasNext();) {
            Element element = (Element) iterator.next();
            if (element != null) {
                sizeInBytes += element.getSerializedSize();
            }
        }
        return sizeInBytes;
    }

    /**
     * Relies on being called from a synchronized method
     * @param element
     * @return true if the LRU element should be removed
     */
    private boolean removeLeastRecentlyUsedElement(Element element) {
        //check for expiry and remove before going to the trouble of spooling it
        if (cache.isExpired(element)) {
            return true;
        }

        if (map.size() <= cache.getMaxElementsInMemory()) {
            return false;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Memory Store maximum size of " + cache.getMaxElementsInMemory()
                        + " reached. About to spool element with key \"" + element.getKey()
                        + "\" to Disk Store");
            }
            if (cache.isOverflowToDisk()) {
                spoolToDisk(element);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Memory Store size now: " + map.size());
            }
        }
        return true;
    }

    /**
     * Spools all elements to disk, in preparation for shutdown
     * <p/>
     * Relies on being called from a synchronized method
     */
    private void spoolAllToDisk() {
        Collection values = map.values();
        for (Iterator iterator = values.iterator(); iterator.hasNext();) {
            Element element = (Element) iterator.next();
            spoolToDisk(element);
        }
    }


    /**
     * Puts the element in the DiskStore
     * Should only be called if {@link Cache#isOverflowToDisk} is true
     *
     * Relies on being called from a synchronized method
     *
     * @param element The Element
     */
    private void spoolToDisk(Element element) {
        try {
            diskStore.put(element);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException(e.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(cache.getName() + "Cache: spool to disk done for: " + element.getKey());
        }
    }


    /**
     * An LRU Map implementation based on Apache Commons LRUMap.
     * <p/>
     * This is used if {@link java.util.LinkedHashMap} is not found in the classpath.
     * LinkedHashMap is part of JDK
     */
    public class SpoolingLRUMap extends org.apache.commons.collections.LRUMap implements Externalizable {


        /**
         * Constructor.
         * The maximum size is set to {@link Cache#getMaxElementsInMemory}. If the
         * LRUMap gets bigger than this, {@link #processRemovedLRU} is called.
         */
        public SpoolingLRUMap() {
            setMaximumSize(cache.getMaxElementsInMemory());
        }

        /**
         * Called after the element has been removed.
         * <p/>
         * Our choices are to do nothing or spool the element to disk.
         *
         * @param key
         * @param value
         */
        protected void processRemovedLRU(Object key, Object value) {
            Element element = (Element) value;
            removeLeastRecentlyUsedElement(element);
        }
    }

    /**
     * An extension of LinkedHashMap which overrides {@link #removeEldestEntry}
     * to persist cache entries to the auxiliary cache before they are removed.
     * <p/>
     * This implementation also provides LRU by access order.
     */
    public class SpoolingLinkedHashMap extends java.util.LinkedHashMap {
        private static final int INITIAL_CAPACITY = 100;
        private static final float GROWTH_FACTOR = .75F;

        /**
         * Default constructor.
         * Will create an initial capacity of 100, a loading of .75 and
         * LRU by access order.
         */
        public SpoolingLinkedHashMap() {
            super(INITIAL_CAPACITY, GROWTH_FACTOR, true);
        }

        /**
         * Returns <tt>true</tt> if this map should remove its eldest entry.
         * This method is invoked by <tt>put</tt> and <tt>putAll</tt> after
         * inserting a new entry into the map.  It provides the implementer
         * with the opportunity to remove the eldest entry each time a new one
         * is added.  This is useful if the map represents a cache: it allows
         * the map to reduce memory consumption by deleting stale entries.
         * <p/>
         * Will return true if:
         * <ol>
         * <li> the element has expired
         * <li> the cache size is greater than the in-memory actual.
         * In this case we spool to disk before returning.
         * </ol>
         *
         * @param eldest The least recently inserted entry in the map, or if
         *               this is an access-ordered map, the least recently accessed
         *               entry.  This is the entry that will be removed it this
         *               method returns <tt>true</tt>.  If the map was empty prior
         *               to the <tt>put</tt> or <tt>putAll</tt> invocation resulting
         *               in this invocation, this will be the entry that was just
         *               inserted; in other words, if the map contains a single
         *               entry, the eldest entry is also the newest.
         * @return <tt>true</tt> if the eldest entry should be removed
         *         from the map; <tt>false</t> if it should be retained.
         */
        protected boolean removeEldestEntry(Map.Entry eldest) {
            Element element = (Element) eldest.getValue();
            return removeLeastRecentlyUsedElement(element);
        }
    }
}

