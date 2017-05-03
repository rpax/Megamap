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

package net.sf.ehcache;

import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.store.DiskStore;
import net.sf.ehcache.store.MemoryStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Cache is the central class in ehcache. Caches have {@link Element}s and are managed
 * by the {@link CacheManager}. The Cache performs logical actions. It delegates physical
 * implementations to its {@link net.sf.ehcache.store.Store}s.
 * <p/>
 * A reference to a Cache can be obtained through the {@link CacheManager}. A Cache thus obtained
 * is guaranteed to have status {@link #STATUS_ALIVE}. This status is checked for any method which
 * throws {@link IllegalStateException} and the same thrown if it is not alive. This would normally
 * happen if a call is made after {@link CacheManager#shutdown} is invoked.
 * <p/>
 * Cache is threadsafe.
 * <p/>
 * Statistics on cache usage are collected and made available through public methods.
 *
 * @author Greg Luck
 * @version $Id: Cache.java,v 1.1.1.1 2005/01/27 18:15:01 pents90 Exp $
 */
public class Cache implements Cloneable {

    /**
     * A reserved word for cache names. It denotes a default configuration
     * which is applied to caches created without configuration.
     */
    public static final String DEFAULT_CACHE_NAME = "default";

    /**
     * The cache is uninitialised. It cannot be used.
     */
    public static final int STATUS_UNINITIALISED = 1;

    /**
     * The cache is alive. It can be used.
     */
    public static final int STATUS_ALIVE = 2;
    /**
     * The cache is uninitialised. It cannot be used.
     */
    public static final int STATUS_DISPOSED = 3;

    private static final Log LOG = LogFactory.getLog(Cache.class.getName());

    private static final long DEFAULT_EXPIRY_THREAD_INTERVAL_SECONDS = 120;
    private static final int MS_PER_SECOND = 1000;

    private String name;

    private DiskStore diskStore;

    private int status;

    private final int maxElementsInMemory;

    /**
     * Do cache elements in this cache overflowToDisk?
     */
    private final boolean overflowToDisk;

    /**
     * The interval in seconds between runs of the disk expiry thread. 2 minutes is the default.
     * This is not the same thing as time to live or time to idle. When the thread runs it checks
     * these things. So this value is how often we check for expiry.
     */
    private final long diskExpiryThreadIntervalSeconds;

    /**
     * For caches that overflow to disk, does the disk cache persist between CacheManager instances?
     */
    private final boolean diskPersistent;

    /**
     * Can turn off expiration
     */
    private final boolean eternal;

    /**
     * The maximum time between creation time and when an element expires.
     * Is only used if the element is not eternal.
     */
    private final long timeToLiveSeconds;

    /**
     * The maximum amount of time between {@link #get(java.io.Serializable)}s
     * before an element expires
     */
    private final long timeToIdleSeconds;


    // Statistics

    /**
     * Cache hit count
     */
    private int hitCount;

    /**
     * Memory cache hit count
     */
    private int memoryStoreHitCount;

    /**
     * Auxiliary hit counts broken down by auxiliary
     */
    private int diskStoreHitCount;

    /**
     * Count of misses where element was not found
     */
    private int missCountNotFound;

    /**
     * Count of misses where element was expired
     */
    private int missCountExpired;

    /**
     * The {@link MemoryStore} of this {@link Cache}. All caches have a memory store.
     */
    private MemoryStore memoryStore;


    /**
     * Constructor.
     * <p/>
     * The {@link net.sf.ehcache.config.Configurator} and clients can create these.
     * <p/>
     * A client can specify their own settings here and pass the {@link Cache} object
     * into {@link CacheManager#addCache} to specify parameters other than the defaults.
     * <p/>
     * Only the CacheManager can initialise them.
     * <p/>
     * This constructor creates disk stores, if specified, that do not persist between restarts.
     * <p/>
     * The default expiry thread interval of 120 seconds is used. This is the interval between runs
     * of the expiry thread, where it checks the disk store for expired elements. It is not the
     * the timeToLiveSeconds.
     */
    public Cache(String name, int maximumSize, boolean overflowToDisk,
                 boolean eternal, long timeToLiveSeconds, long timeToIdleSeconds) {
        this(name, maximumSize, overflowToDisk, eternal, timeToLiveSeconds, timeToIdleSeconds, false,
                DEFAULT_EXPIRY_THREAD_INTERVAL_SECONDS);
    }

    /**
     * Full Constructor.
     * <p/>
     * The {@link net.sf.ehcache.config.Configurator} and clients can create these.
     * <p/>
     * A client can specify their own settings here and pass the {@link Cache} object
     * into {@link CacheManager#addCache} to specify parameters other than the defaults.
     * <p/>
     * Only the CacheManager can initialise them.
     */
    public Cache(String name,
                 int maximumSize,
                 boolean overflowToDisk,
                 boolean eternal,
                 long timeToLiveSeconds,
                 long timeToIdleSeconds,
                 boolean diskPersistent,
                 long diskExpiryThreadIntervalSeconds) {
        this.name = name;
        this.maxElementsInMemory = maximumSize;
        this.overflowToDisk = overflowToDisk;
        this.eternal = eternal;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.timeToIdleSeconds = timeToIdleSeconds;
        this.diskPersistent = diskPersistent;

        //Set this to a safe value.
        if (diskExpiryThreadIntervalSeconds == 0) {
            this.diskExpiryThreadIntervalSeconds = DEFAULT_EXPIRY_THREAD_INTERVAL_SECONDS;
        } else {
            this.diskExpiryThreadIntervalSeconds = diskExpiryThreadIntervalSeconds;
        }
        status = STATUS_UNINITIALISED;
    }


    /**
     * Newly created caches do not have a {@link net.sf.ehcache.store.MemoryStore} or a {@link net.sf.ehcache.store.DiskStore}.
     * <p/>
     * This method creates those and makes the cache ready to accept elements
     */
    synchronized void initialise(Configuration configuration) {
        if (status != STATUS_UNINITIALISED) {
            throw new IllegalStateException("Cannot initialise the " + name
                    + " cache because its status is not STATUS_UNINITIALISED");
        }
        if (configuration == null) {
            throw new IllegalArgumentException("Cannot intialise caches without a configuration");
        }
        if (maxElementsInMemory == 0) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Cache: " + name + " has a maxElementsInMemory of 0. It is strongly recommended to " +
                        "have a maximumSize of at least 1. Performance is halved by not using a MemoryStore.");
            }
        }
        if (overflowToDisk) {
            diskStore = new DiskStore(this, configuration.getDiskCachePath());
        }
        memoryStore = new MemoryStore(this, diskStore);
        if (diskPersistent) {
            addShutdownHook();
        }
        status = STATUS_ALIVE;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initialised cache: " + name);
        }

    }


    /**
     * Some caches might be persistent, so we want to add a shutdown hook if that is the
     * case, so that the data and index can be written to disk.
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized (this) {
                    if (status == STATUS_ALIVE) {
                        LOG.info("VM shutting down with the disk store for " + name
                                + " still active. The disk store is persistent. Calling dispose...");
                        dispose();
                    }
                }
            }
        });
    }


    /**
     * Put an element in the cache.
     * <p/>
     * Resets the access statistics on the element, which would be the case if it has previously been
     * gotten from a cache, and is now being put back.
     *
     * @param element
     * @throws IllegalStateException    if the cache is not {@link #STATUS_ALIVE}
     * @throws IllegalArgumentException if the element is null
     */
    public synchronized void put(Element element) throws IllegalArgumentException, IllegalStateException {
        checkStatus();
        if (element == null) {
            throw new IllegalArgumentException("Element cannot be null");
        }
        element.resetAccessStatistics();
        memoryStore.put(element);
        return;
    }

    /**
     * Put an element in the cache, without updating statistics. This is meant to be used
     * in conjunction with {@link #getQuiet}
     * <p/>
     * Resets the access statistics on the element, which would be the case if it has previously been
     * gotten from a cache, and is now being put back.
     *
     * @param element
     * @throws IllegalStateException    if the cache is not {@link #STATUS_ALIVE}
     * @throws IllegalArgumentException if the element is null
     */
    public synchronized void putQuiet(Element element) throws IllegalArgumentException, IllegalStateException {
        checkStatus();
        if (element == null) {
            throw new IllegalArgumentException("Element cannot be null");
        }
        memoryStore.put(element);
        return;
    }


    /**
     * Gets an element from the cache. Updates Element Statistics
     * <p/>
     * Note that the Element's lastAccessTime is always the time of this get.
     * Use {@link #getQuiet(java.io.Serializable)} to peak into the Element to see its last access time with get
     *
     * @param key a serializable value
     * @return the element, or null, if it does not exist.
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     * @see #isExpired
     */
    public synchronized Element get(Serializable key) throws IllegalStateException, CacheException {
        checkStatus();
        Element element = null;

        element = searchInMemoryStore(key, true);
        if (element == null && overflowToDisk) {
            try {
                element = searchInDiskStore(key, true);
            } catch (IOException e) {
                throw new CacheException(e.getMessage());
            }
        }

        if (element == null) {
            missCountNotFound++;
            if (LOG.isTraceEnabled()) {
                LOG.trace(name + " cache - Miss");
            }
            return null;
        } else {
            hitCount++;
            return element;
        }
    }

    /**
     * Gets an element from the cache, without updating Element statistics. Cache statistics are
     * still updated.
     * <p/>
     *
     * @param key a serializable value
     * @return the element, or null, if it does not exist.
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     * @see #isExpired
     */
    public synchronized Element getQuiet(Serializable key) throws IllegalStateException, CacheException {
        checkStatus();
        Element element = null;

        element = searchInMemoryStore(key, false);
        if (element == null && overflowToDisk) {
            try {
                element = searchInDiskStore(key, false);
            } catch (IOException e) {
                throw new CacheException(e.getMessage());
            }
        }

        if (element == null) {
            missCountNotFound++;
            if (LOG.isTraceEnabled()) {
                LOG.trace(name + " cache - Miss");
            }
            return null;
        } else {
            hitCount++;
            return element;
        }
    }

    /**
     * Returns a list of all elements in the cache, whether or not they are expired.
     * <p/>
     * The returned keys are unique and can be considered a set.
     * <p/>
     * The List returned is not live. It is a copy.
     * <p/>
     * The time taken is O(n). On a single cpu 1.8Ghz P4, approximately 8ms is required
     * for each 1000 entries.
     *
     * @return a list of {@link Serializable} keys
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized List getKeys() throws IllegalStateException, CacheException {
        checkStatus();
        /* An element with the same key can exist in both the memory store and the
           disk store at the same time. Because the memory store is always searched first
           these duplicates do not cause problems when getting elements/

           This method removes these duplicates before returning the list of keys*/
        List allKeyList = new ArrayList();
        List keyList = Arrays.asList(memoryStore.getKeyArray());
        allKeyList.addAll(keyList);
        if (overflowToDisk) {
            Set allKeys = new HashSet();
            //within the store keys will be unique
            allKeys.addAll(keyList);
            Object[] diskKeys = diskStore.getKeyArray();
            for (int i = 0; i < diskKeys.length; i++) {
                Object diskKey = diskKeys[i];
                if (allKeys.add(diskKey)) {
                    //Unique, so add it to the list
                    allKeyList.add(diskKey);
                }
            }
        }
        return allKeyList;
    }

    /**
     * Returns a list of all elements in the cache. Only keys of non-expired
     * elements are returned.
     * <p/>
     * The returned keys are unique and can be considered a set.
     * <p/>
     * The List returned is not live. It is a copy.
     * <p/>
     * The time taken is O(n), where n is the number of elements in the cache. On
     * a 1.8Ghz P4, the time taken is approximately 200ms per 1000 entries. This method
     * is not syncrhonized, because it relies on a non-live list returned from {@link #getKeys()}
     * , which is synchronised, and which takes 8ms per 1000 entries. This way
     * cache liveness is preserved, even if this method is very slow to return.
     * <p/>
     * Consider whether your usage requires checking for expired keys. Because
     * this method takes so long, depending on cache settings, the list could be
     * quite out of date by the time you get it.
     *
     * @return a list of {@link Serializable} keys
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public List getKeysWithExpiryCheck() throws IllegalStateException, CacheException {
        List allKeyList = getKeys();
        //remove keys of expired elements
        ArrayList nonExpiredKeys = new ArrayList(allKeyList.size());
        int allKeyListSize = allKeyList.size();
        for (int i = 0; i < allKeyListSize; i++) {
            Serializable key = (Serializable) allKeyList.get(i);
            Element element = getQuiet(key);
            if (element != null) {
                nonExpiredKeys.add(key);
            }
        }
        nonExpiredKeys.trimToSize();
        return nonExpiredKeys;
    }


    /**
     * Returns a list of all elements in the cache, whether or not they are expired.
     * <p/>
     * The returned keys are not unique and may contain duplicates. If the cache is only
     * using the memory store, the list will be unique. If the disk store is being used
     * as well, it will likely contain duplicates, because of the internal store design.
     * <p/>
     * The List returned is not live. It is a copy.
     * <p/>
     * The time taken is O(log n). On a single cpu 1.8Ghz P4, approximately 6ms is required
     * for 1000 entries and 36 for 50000.
     * <p/>
     * This is the fastest getKeys method
     *
     * @return a list of {@link Serializable} keys
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized List getKeysNoDuplicateCheck() throws IllegalStateException {
        checkStatus();
        ArrayList allKeys = new ArrayList();
        List memoryKeySet = Arrays.asList(memoryStore.getKeyArray());
        allKeys.addAll(memoryKeySet);
        if (overflowToDisk) {
            List diskKeySet = Arrays.asList(diskStore.getKeyArray());
            allKeys.addAll(diskKeySet);
        }
        return allKeys;
    }

    private Element searchInMemoryStore(Serializable key, boolean updateStatistics) {
        Element element = null;
        if (updateStatistics) {
            element = memoryStore.get(key);
        } else {
            element = memoryStore.getQuiet(key);
        }
        if (element != null) {
            if (isExpired(element)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(name + " Memory cache hit, but element expired");
                }
                missCountExpired++;
                remove(key);
                element = null;
            } else {
                memoryStoreHitCount++;
            }
        }
        return element;
    }

    private Element searchInDiskStore(Serializable key, boolean updateStatistics) throws IOException {
        Element element = null;
        if (updateStatistics) {
            element = diskStore.get(key);
        } else {
            element = diskStore.getQuiet(key);
        }
        if (element != null) {
            if (isExpired(element)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(name + " cache - Disk Store hit, but element expired");
                }
                missCountExpired++;
                remove(key);
                element = null;
            } else {
                diskStoreHitCount++;
                //Put the item back into memory to preserve LRU algorithm across the cache
                memoryStore.put(element);
            }
        }
        return element;
    }


    /**
     * Removes an {@link Element} from the Cache. This also removes it from any
     * stores it may be in.
     *
     * @param key
     * @return true if the element was removed, false if it was not found in the cache
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized boolean remove(Serializable key) throws IllegalStateException {
        checkStatus();
        boolean removedFromMemory = false;
        removedFromMemory = memoryStore.remove(key);
        if (LOG.isDebugEnabled() && removedFromMemory) {
            LOG.debug("Removing " + key + " from memoryStore");
        }

        boolean removedFromDisk = false;
        if (overflowToDisk) {
            try {
                removedFromDisk = diskStore.remove(key);
                if (LOG.isDebugEnabled() && removedFromDisk) {
                    LOG.debug("Removing " + key + " from diskStore");
                }
            } catch (IOException e) {
                LOG.error("Failure removing from Disk Cache", e);
            }
        }
        return (removedFromMemory || removedFromDisk);
    }

    /**
     * Removes all cached items.
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized void removeAll() throws IllegalStateException, IOException {
        checkStatus();
        memoryStore.removeAll();
        if (overflowToDisk) {
            diskStore.removeAll();
        }
    }

    /**
     * Flushes all cache items from memory to auxilliary caches and close the auxilliary caches.
     * <p/>
     * Should be invoked only by CacheManager.
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    synchronized void dispose() throws IllegalStateException {
        checkStatus();
        memoryStore.dispose();
        memoryStore = null;
        if (overflowToDisk) {
            diskStore.dispose();
            diskStore = null;
        }
        status = STATUS_DISPOSED;
    }


    /**
     * Gets the size of the cache. This is a subtle concept. See below.
     * <p/>
     * The size is the number of {@link Element}s in the {@link MemoryStore} plus
     * the number of {@link Element}s in the {@link DiskStore}.
     * <p/>
     * This number is the actual number of elements, including expired elements that have
     * not been removed.
     * <p/>
     * Expired elements are removed from the the memory store when
     * getting an expired element, or when attempting to spool an expired element to
     * disk.
     * <p/>
     * Expired elements are removed from the disk store when getting an expired element,
     * or when the expiry thread runs, which is once every five minutes.
     * <p/>
     * To get an exact size, which would exclude expired elements, use {@link #getKeysWithExpiryCheck()}.size(),
     * although see that method for the approximate time that would take.
     * <p/>
     * To get a very fast result, use {@link #getKeysNoDuplicateCheck()}.size(). If the disk store
     * is being used, there will be some duplicates.
     *
     * @return The size value
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized int getSize() throws IllegalStateException, CacheException {
        checkStatus();
        /* The memory store and the disk store can simultaneously contain elements with the same key
           Cache size is the size of the union of the two key sets.*/
        return getKeys().size();
    }

    /**
     * Gets the size of the memory store for this cache
     * <p/>
     * Warning: This method can be very expensive to run. Allow approximately 1 second
     * per 1MB of entries. Running this method could create liveness problems
     * because the object lock is held for a long period
     * <p/>
     *
     * @return the size of the memory store in bytes
     * @throws IllegalStateException
     */
    public synchronized long calculateInMemorySize() throws IllegalStateException, CacheException {
        checkStatus();
        return memoryStore.getSizeInBytes();
    }


    /**
     * Returns the number of elements in the memory store.
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public long getMemoryStoreSize() throws IllegalStateException {
        checkStatus();
        return memoryStore.getSize();
    }

    /**
     * Returns the number of elements in the disk store.
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public int getDiskStoreSize() throws IllegalStateException {
        checkStatus();
        if (overflowToDisk) {
            return diskStore.getSize();
        } else {
            return 0;
        }
    }

    /**
     * Gets the status attribute of the Store object
     *
     * @return The status value
     */
    public int getStatus() {
        return status;
    }

    private void checkStatus() {
        if (status != STATUS_ALIVE) {
            throw new IllegalStateException("The " + name + " Cache is not alive.");
        }
    }

    /**
     * Number of times a requested item was found in the cache
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public int getHitCount()
            throws IllegalStateException {
        checkStatus();
        return hitCount;
    }

    /**
     * Number of times a requested item was found in the Memory Store
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public int getMemoryStoreHitCount() throws IllegalStateException {
        checkStatus();
        return memoryStoreHitCount;
    }

    /**
     * Number of times a requested item was found in the Disk Store
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public int getDiskStoreHitCount() throws IllegalStateException {
        checkStatus();
        return diskStoreHitCount;
    }

    /**
     * Number of times a requested element was not found in the cache. This
     * may be because it expired, in which case this will also be recorded in {@link #getMissCountExpired},
     * or because it was simply not there.
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public int getMissCountNotFound() throws IllegalStateException {
        checkStatus();
        return missCountNotFound;
    }

    /**
     * Number of times a requested element was found but was expired
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public int getMissCountExpired() throws IllegalStateException {
        checkStatus();
        return missCountExpired;
    }

    /**
     * Gets the cache name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name
     */
    void setName(String name) {
        this.name = name;
    }

    /**
     * Gets timeToIdleSeconds
     */
    public long getTimeToIdleSeconds() {
        return timeToIdleSeconds;
    }

    /**
     * Gets timeToLiveSeconds
     */
    public long getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * Are elements eternal
     */
    public boolean isEternal() {
        return eternal;
    }

    /**
     * Does the overflow go to disk
     */
    public boolean isOverflowToDisk() {
        return overflowToDisk;
    }

    /**
     * Gets the maximum number of elements to hold in memory
     */
    public int getMaxElementsInMemory() {
        return maxElementsInMemory;
    }

    /**
     * Returns a {@link String} representation of {@link Cache}
     */
    public String toString() {
        StringBuffer dump = new StringBuffer();

        dump.append("[ ")
                .append(" name = ").append(name)
                .append(" status = ").append(status)
                .append(" eternal = ").append(eternal)
                .append(" overflowToDisk = ").append(overflowToDisk)
                .append(" maxElementsInMemory = ").append(maxElementsInMemory)
                .append(" timeToLiveSeconds = ").append(timeToLiveSeconds)
                .append(" timeToIdleSeconds = ").append(timeToIdleSeconds)
                .append(" diskPersistent = ").append(diskPersistent)
                .append(" diskExpiryThreadIntervalSeconds = ").append(diskExpiryThreadIntervalSeconds)
                .append(" hitCount = ").append(hitCount)
                .append(" memoryStoreHitCount = ").append(memoryStoreHitCount)
                .append(" diskStoreHitCount = ").append(diskStoreHitCount)
                .append(" missCountNotFound = ").append(missCountNotFound)
                .append(" missCountExpired = ").append(missCountExpired)
                .append(" ]");

        return dump.toString();
    }


    /**
     * Checks whether this cache element has expired.
     * <p/>
     * The element is expired if:
     * <ol>
     * <li> the idle time is non-zero and has elapsed, unless the cache is eternal; or
     * <li> the time to live is non-zero and has elapsed, unless the cache is eternal; or
     * <li> the value of the element is null.
     * </ol>
     *
     * @return true if it has expired
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     * @throws NullPointerException  if the element is null
     */
    public boolean isExpired(Element element) throws IllegalStateException, NullPointerException {
        checkStatus();
        boolean expired;
        synchronized (element) {
            if (element.getValue() == null) {
                expired = true;
            }
            if (!eternal) {
                long now = System.currentTimeMillis();
                long creationTime = element.getCreationTime();
                long ageLived = now - creationTime;
                long ageToLive = timeToLiveSeconds * MS_PER_SECOND;
                long nextToLastAccessTime = element.getNextToLastAccessTime();
                long mostRecentTime = Math.max(creationTime, nextToLastAccessTime);
                long ageIdled = now - mostRecentTime;
                long ageToIdle = timeToIdleSeconds * MS_PER_SECOND;

                if (LOG.isTraceEnabled()) {
                    LOG.trace(element.getKey() + " now: " + now);
                    LOG.trace(element.getKey() + " Creation Time: " + creationTime
                            + " Next To Last Access Time: " + nextToLastAccessTime);
                    LOG.trace(element.getKey() + " mostRecentTime: " + mostRecentTime);
                    LOG.trace(element.getKey() + " Age to Idle: " + ageToIdle + " Age Idled: " + ageIdled);
                }

                if (timeToLiveSeconds != 0 && (ageLived > ageToLive)) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("timeToLiveSeconds exceeded : " + element.getKey());
                    }
                    expired = true;
                } else if (timeToIdleSeconds != 0 && (ageIdled > ageToIdle)) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("timeToIdleSeconds exceeded : " + element.getKey());
                    }
                    expired = true;
                } else {
                    expired = false;
                }
            } else {
                expired = false;
            }
            if (LOG.isDebugEnabled()) {
                Serializable key = null;
                if (element != null) {
                    key = element.getKey();
                }
                LOG.debug(getName() + ": Is element with key " + key + " expired?: " + expired);
            }
            return expired;
        }
    }


    /**
     * Clones a cache. This is only legal if the cache has not been
     * initialized. At that point only primitives have been set and no
     * {@link net.sf.ehcache.store.MemoryStore} or {@link net.sf.ehcache.store.DiskStore} has been created.
     *
     * @return an object of type {@link Cache}
     * @throws CloneNotSupportedException
     */
    public Object clone() throws CloneNotSupportedException {
        if (!(memoryStore == null && diskStore == null)) {
            throw new CloneNotSupportedException("Cannot clone an initialized cache.");
        }
        return (Cache) super.clone();
    }

    /**
     * Gets the internal DiskStore.
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    DiskStore getDiskStore() throws IllegalStateException {
        checkStatus();
        return diskStore;
    }

    /**
     * Gets the internal MemoryStore.
     *
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    MemoryStore getMemoryStore() throws IllegalStateException {
        checkStatus();
        return memoryStore;
    }

    /**
     * @return true if the cache overflows to disk and the disk is persistent between restarts
     */
    public boolean isDiskPersistent() {
        return diskPersistent;
    }

    /**
     * @return the interval between runs
     *         of the expiry thread, where it checks the disk store for expired elements. It is not the
     *         the timeToLiveSeconds.
     */
    public long getDiskExpiryThreadIntervalSeconds() {
        return diskExpiryThreadIntervalSeconds;
    }
}
