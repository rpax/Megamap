/*
 *   Copyright 2005 John Watkinson
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.larvalabs.megamap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.commons.collections.buffer.UnboundedFifoBuffer;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.CacheException;

import java.util.Set;
import java.util.HashSet;
import java.io.Serializable;

/**
 * An efficient, unbounded map (hashtable) that can optionally persist between VM invocations.
 * Keys are stored in memory, but values may be extremely large. They will be persisted to disk as required by the
 * garbage collector of the VM.
 *
 * @author John Watkinson
 */
public class MegaMap implements Runnable {

    static Log log = LogFactory.getLog(MegaMap.class);

    private String storeName;
    private Cache cache;
    private ReferenceMap softMap;
    private Set keySet;
    private UnboundedFifoBuffer cacheQueue;
    private boolean running = false;
    private boolean finishedRunning = false;

    /**
     * Stores a cache instruction for later action.
     */
    private static class CacheAction {

        public static final int ACTION_TYPE_PUT = 1;
        public static final int ACTION_TYPE_REMOVE = 2;

        private int actionType;
        private Serializable key;
        private Serializable value;

        public static CacheAction createPutAction(Serializable key, Serializable value) {
            CacheAction ca = new CacheAction();
            ca.key = key;
            ca.value = value;
            ca.actionType = ACTION_TYPE_PUT;
            return ca;
        }

        public static CacheAction createRemoveAction(Serializable key) {
            CacheAction ca = new CacheAction();
            ca.key = key;
            ca.actionType = ACTION_TYPE_REMOVE;
            return ca;
        }

        public int getActionType() {
            return actionType;
        }

        public Serializable getKey() {
            return key;
        }

        public Serializable getValue() {
            return value;
        }

    }

    /**
     * This constructor cannot be called directly, use {@link MegaMapManager#createMegaMap(java.lang.String, boolean, boolean)}
     * or {@link MegaMapManager#createMegaMap(java.lang.String, java.lang.String, boolean, boolean)}.
     */
    MegaMap(String mapName, CacheManager manager, boolean persistent) throws MegaMapException {
        this.storeName = mapName;
        try {
            init(manager, persistent);
        } catch (CacheException ce) {
            throw new MegaMapException("Error in initialization of MegaMap", ce);
        }
    }

    /**
     * Put a value in the MegaMap. Both the key and value must be Serializable objects.
     * If there is already a value stored for this key, it will be over-written.
     */
    public void put(Serializable key, Serializable value) {
        synchronized (this) {
            softMap.put(key, value);
            keySet.add(key);
        }
        CacheAction action = CacheAction.createPutAction(key, value);
        synchronized (cacheQueue) {
            cacheQueue.add(action);
            cacheQueue.notify();
        }
    }

    /**
     * Gets the value for the given key.
     *
     * @param key
     * @see #hasKey(java.io.Serializable)
     */
    public Serializable get(Serializable key) throws MegaMapException {
        try {
            Serializable value = null;
            synchronized (this) {
                value = (Serializable) softMap.get(key);
            }
            if (value == null) {
                Element element = cache.get(key);
                if (element != null) {
                    log.trace("Found in disk cache.");
                    value = element.getValue();
                }
            } else {
                log.trace("Found in memory cache.");
            }
            return value;
        } catch (CacheException ce) {
            throw new MegaMapException("Exception while getting", ce);
        }
    }

    /**
     * Removes the value for the given key.
     *
     * @param key
     */
    public void remove(Serializable key) {
        synchronized (this) {
            softMap.remove(key);
            keySet.remove(key);
        }
        CacheAction action = CacheAction.createRemoveAction(key);
        synchronized (cacheQueue) {
            cacheQueue.add(action);
            cacheQueue.notify();
        }
    }

    /**
     * Checks if there is a value stored in the MegaMap for the given key.
     * This method should be used to check for the existence of keys instead of {@link #get(java.io.Serializable)} as
     * it could be far more efficient than {@link #get(java.io.Serializable)}.
     * This is because<code>get</code> may have to load the object from disk.
     *
     * @param key the key to search for.
     * @return <code>true</code> if there is a value stored for the given key, <code>false</code> otherwise.
     */
    public boolean hasKey(Serializable key) {
        synchronized (this) {
            return keySet.contains(key);
        }
    }

    private void init(CacheManager manager, boolean persistent) throws CacheException {
        cacheQueue = new UnboundedFifoBuffer();
        softMap = new ReferenceMap();
        keySet = new HashSet();
        cache = new Cache(storeName, 1, true, true, 0L, 0L, persistent, 2147483647L);
        manager.addCache(cache);
        running = true;
        Thread thread = new Thread(this, "MegaMap-" + storeName);
        thread.setDaemon(false);
        thread.start();
    }

    /**
     * Cannot be called directly. Use {@link MegaMapManager#removeMegaMap(java.lang.String)} to shut down a
     * specific MegaMap cleanly or {@link MegaMapManager#shutdown()} to shutdown all MegaMaps cleanly.
     */
    synchronized void shutdown() {
        synchronized (cacheQueue) {
            running = false;
            cacheQueue.notify();
        }
        try {
            while (!finishedRunning) {
                wait();
            }
        } catch (InterruptedException ie) {
            // Ignore
        }
    }


    /**
     * Gets all the keys stored in the MegaMap.
     *
     * @return a set of all Keys. This set is not "live", it may be modified without fear of damaging the MegaMap.
     */
    public Set getKeys() {
        synchronized (this) {
            return new HashSet(keySet);
        }
    }

    /**
     * Called by the thread that managed the MegaMap disk persistence. Do not use directly.
     */
    public void run() {
        log.info("MegaMap-" + storeName + " persistence thread started.");
        try {
            while (true) {
                CacheAction action = null;
                synchronized (cacheQueue) {
                    while (cacheQueue.isEmpty() && running) {
                        cacheQueue.wait();
                    }
                    if (!running && cacheQueue.isEmpty()) {
                        // shut down!
                        break;
                    } else {
                        action = (CacheAction) cacheQueue.remove();
                    }
                }
                // Element will be null if store was shutdown.
                if (action != null) {
                    log.trace("Background thread is running an action...");
                    if (action.getActionType() == CacheAction.ACTION_TYPE_PUT) {
                        Element element = new Element(action.getKey(), action.getValue());
                        cache.put(element);
                        log.trace("Put object in disk cache: '" + action.getKey() + "'.");
                    } else {
                        cache.remove(action.getKey());
                        log.trace("Removed object from disk cache: '" + action.getKey() + "'.");
                    }
                    log.trace("... background thread action complete.");
                }
            }
        } catch (InterruptedException e) {
            log.warn("MegaMap-" + storeName + " persistence thread interrupted.", e);
        }
        log.info("MegaMap-" + storeName + " persistence thread shutting down.");
        synchronized (this) {
            finishedRunning = true;
            notify();
        }
    }

}
