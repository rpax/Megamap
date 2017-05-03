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


package net.sf.ehcache.config;

import net.sf.ehcache.ObjectExistsException;
import net.sf.ehcache.CacheException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The configuration for ehcache. This class is populated through introspection
 * by {@link Configurator}
 * @author Greg Luck
 * @version $Id: Configuration.java,v 1.1.1.1 2005/01/27 18:15:02 pents90 Exp $
 */
public class Configuration {

    private DiskStore diskStore;
    private DefaultCache defaultCache;
    private Map caches = new HashMap();

    /**
     * Allows {@link BeanHandler} to add disk store location to the configuration
     */
    public void addDiskStore(DiskStore diskStore) throws ObjectExistsException {
        if (this.diskStore != null) {
            throw new ObjectExistsException("The Disk Store has already been configured");
        }
        this.diskStore = diskStore;
    }

    /**
     * Allows {@link BeanHandler} to add disk caches to the configuration
     */
    public void addDefaultCache(DefaultCache defaultCache) throws ObjectExistsException {
        if (this.defaultCache != null) {
            throw new ObjectExistsException("The Default Cache has already been configured");
        }
        this.defaultCache = defaultCache;
    }

    /**
     * Allows {@link BeanHandler} to add disk caches to the configuration
     */
    public void addCache(Cache cache) throws ObjectExistsException {
        if (caches.get(cache.name) != null) {
            throw new ObjectExistsException("Cannot create cache: " + cache.name
                    + " with the same name as an existing one.");
        }
        if (cache.name.equalsIgnoreCase(net.sf.ehcache.Cache.DEFAULT_CACHE_NAME)) {
            throw new ObjectExistsException("The Default Cache has already been configured");
        }

        caches.put(cache.name, cache);
    }

    /**
     * Gets the disk cache path
     */
    public String getDiskCachePath() {
        if (diskStore != null) {
            return diskStore.getPath();
        } else {
            return null;
        }
    }

    public void setDiskCachePath(String path) {
        if (diskStore != null) {
            diskStore.setPath(path);
        }
    }

    /**
     * @return the Default Cache
     * @throws CacheException if there is no default cache
     */
    public net.sf.ehcache.Cache getDefaultCache()  throws CacheException {
        if (defaultCache == null) {
            throw new CacheException("Illegal configuration. No default cache is configured.");
        } else {
            return defaultCache.toCache();
        }
    }

    /**
     * Gets a Map of caches
     */
    public Set getCacheKeySet() {
        return caches.keySet();
    }

    /**
     * Gets a cache
     * @param name the name of the cache
     * @return a new net.sf.ehcache.Cache
     */
    public net.sf.ehcache.Cache getCache(String name) {
        Cache cache = (Cache) caches.get(name);
        return cache.toCache();
    }


    /**
     * A class to represent DiskStore configuration
     * e.g. <diskStore path="java.io.tmpdir" />
     */
    public static class DiskStore {
        private static final Log LOG = LogFactory.getLog(DiskStore.class.getName());
        private String path;

        /**
         * Sets the path
         * @param path If the path is a Java System Property it is replaced by
         *             its value in the running VM. The following properties are translated:
         *             <ul>
         *             <li>user.home - User's home directory
         *             <li>user.dir - User's current working directory
         *             <li>java.io.tmpdir - Default temp file path
         *             </ul>
         */
        public void setPath(String path) {
            String translatedPath = null;
            if (path.equals("user.home")) {
                translatedPath = System.getProperty("user.home");
            } else if (path.equals("user.dir")) {
                translatedPath = System.getProperty("user.dir");
            } else if (path.equals("java.io.tmpdir")) {
                translatedPath = System.getProperty("java.io.tmpdir");
            } else {
                translatedPath = path;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Disk Store Path: " + translatedPath);
            }
            this.path = translatedPath;
        }

        /**
         * Gets the path
         */
        private String getPath() {
            return path;
        }
    }


    /**
     * A class to represent Cache configuration
     * e.g.
     * <cache name="testCache1"
     * maxElementsInMemory="10000"
     * eternal="false"
     * timeToIdleSeconds="3600"
     * timeToLiveSeconds="10"
     * overflowToDisk="true"
     * diskPersistent="true"
     * diskExpiryThreadIntervalSeconds="120"
     * />
     */
    public static class Cache {

        /**
         * the name of the cache
         */
        protected String name;

        /**
         * the maximum objects to be held in memory
         */
        protected int maxElementsInMemory;

        /**
         * Sets whether elements are eternal. If eternal,  timeouts are ignored and the element
         * is never expired.
         */
        protected boolean eternal;

        /**
         * the time to idle for an element before it expires. Is only used
         * if the element is not eternal.A value of 0 means do not check for idling.
         */
        protected int timeToIdleSeconds;

        /**
         * Sets the time to idle for an element before it expires. Is only used
         * if the element is not eternal. This attribute is optional in the configuration.
         * A value of 0 means do not check time to live.
         */
        protected int timeToLiveSeconds;

        /**
         * whether elements can overflow to disk when the in-memory cache
         * has reached the set limit.
         */
        protected boolean overflowToDisk;

        /**
         * For caches that overflow to disk, does the disk cache persist between CacheManager instances?
         */
        protected boolean diskPersistent;

        /**
         * The interval in seconds between runs of the disk expiry thread.
         * <p/>
         * 2 minutes is the default.
         * This is not the same thing as time to live or time to idle. When the thread runs it checks
         * these things. So this value is how often we check for expiry.
         */
        protected long diskExpiryThreadIntervalSeconds;


        /**
         * Sets the name of the cache. This must be unique
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Sets the maximum objects to be held in memory
         */
        public void setMaxElementsInMemory(int maxElementsInMemory) {
            this.maxElementsInMemory = maxElementsInMemory;
        }

        /**
         * Sets whether elements are eternal. If eternal,  timeouts are ignored and the element
         * is never expired.
         */
        public void setEternal(boolean eternal) {
            this.eternal = eternal;
        }

        /**
         * Sets the time to idle for an element before it expires. Is only used
         * if the element is not eternal.
         */
        public void setTimeToIdleSeconds(int timeToIdleSeconds) {
            this.timeToIdleSeconds = timeToIdleSeconds;
        }

        /**
         * Sets the time to idle for an element before it expires. Is only used
         * if the element is not eternal.
         */
        public void setTimeToLiveSeconds(int timeToLiveSeconds) {
            this.timeToLiveSeconds = timeToLiveSeconds;
        }

        /**
         * Sets whether elements can overflow to disk when the in-memory cache
         * has reached the set limit.
         */
        public void setOverflowToDisk(boolean overflowToDisk) {
            this.overflowToDisk = overflowToDisk;
        }

        /**
         * Sets whether, for caches that overflow to disk,
         * the disk cache persist between CacheManager instances
         */
        public void setDiskPersistent(boolean diskPersistent) {
            this.diskPersistent = diskPersistent;
        }

        /**
         * Sets the interval in seconds between runs of the disk expiry thread.
         * <p/>
         * 2 minutes is the default.
         * This is not the same thing as time to live or time to idle. When the thread runs it checks
         * these things. So this value is how often we check for expiry.
         */
        public void setDiskExpiryThreadIntervalSeconds(int diskExpiryThreadIntervalSeconds) {
            this.diskExpiryThreadIntervalSeconds = diskExpiryThreadIntervalSeconds;
        }

        /**
         * @return a new Cache with this configuration
         */
        private net.sf.ehcache.Cache toCache() {
            return new net.sf.ehcache.Cache(name,
                    maxElementsInMemory,
                    overflowToDisk,
                    eternal,
                    timeToLiveSeconds,
                    timeToIdleSeconds,
                    diskPersistent,
                    diskExpiryThreadIntervalSeconds);
        }
    }

    /**
     * A class to represent the default cache
     * e.g.
     * <defaultCache
     * maxElementsInMemory="10000"
     * eternal="false"
     * timeToIdleSeconds="3600"
     * timeToLiveSeconds="10"
     * overflowToDisk="true"
     * />
     */
    public static class DefaultCache extends Cache {

        /**
         * @return a new Cache with this configuration
         */
        private net.sf.ehcache.Cache toCache() {
            return new net.sf.ehcache.Cache(net.sf.ehcache.Cache.DEFAULT_CACHE_NAME,
                    maxElementsInMemory,
                    overflowToDisk,
                    eternal,
                    timeToLiveSeconds,
                    timeToIdleSeconds,
                    diskPersistent,
                    diskExpiryThreadIntervalSeconds);
        }
    }
}
