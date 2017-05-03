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
import net.sf.ehcache.config.Configurator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

/**
 * Manages all aspects of EHCache
 *
 * @version $Id: CacheManager.java,v 1.1.1.1 2005/01/27 18:15:01 pents90 Exp $
 * @author Greg Luck
 **/
public final class CacheManager {
    /** Store alive status. */
    public static final int STATUS_UNINITIALISED = 1;

    /** Store alive status. */
    public static final int STATUS_ALIVE = 2;

    /** Store disposed status. */
    public static final int STATUS_SHUTDOWN = 3;

    private static final Log LOG = LogFactory.getLog(CacheManager.class.getName());

    /** The Singleton Instance */
    private static CacheManager instance;
    
    /** The configuration. */
    private Configuration configuration;

    //Configuration file name
    private String configurationFileName;

    //Configuration file URL
    private URL configurationURL;

    //Configuration InputStream
    private InputStream configurationInputStream;

    /** Caches managed by this manager */
    private Hashtable caches = new Hashtable();

    /** Default cache cache */
    private Cache defaultCache;

    /** The path for the directory in which disk caches are created */
    private String diskStorePath;

    private int status;

    private CacheManager(String configurationFileName) throws CacheException {
        status = STATUS_UNINITIALISED;
        this.configurationFileName = configurationFileName;
        configure();
        status = STATUS_ALIVE;
    }

    private CacheManager(URL configurationFileURL) throws CacheException {
        status = STATUS_UNINITIALISED;
        this.configurationURL = configurationFileURL;
        configure();
        status = STATUS_ALIVE;
    }

    private CacheManager(InputStream inputStream) throws CacheException {
        status = STATUS_UNINITIALISED;
        this.configurationInputStream = inputStream;
        configure();
        status = STATUS_ALIVE;
    }

    private CacheManager() throws CacheException {
        //default config will be done
        status = STATUS_UNINITIALISED;
        configure();
        status = STATUS_ALIVE;
    }

    /**
     * Loads configuration.
     * <p>
     * Should only be called once.
     */
    private synchronized void configure() throws CacheException {
        if (defaultCache != null || diskStorePath != null || caches.size() != 0
                || status == STATUS_SHUTDOWN) {
            throw new IllegalStateException("Attempt to reinitialise the Cache Manager");
        }

        configuration = new Configuration();
        try {
            Configurator configurator = new Configurator();
            if (configurationFileName != null) {
                configurator.configure(configuration, new File(configurationFileName));
            } else if (configurationURL != null) {
                configurator.configure(configuration, configurationURL);
            } else if (configurationInputStream != null) {
                configurator.configure(configuration, configurationInputStream);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Configuring ehcache from classpath.");
                }
                configurator.configure(configuration);
            }
            defaultCache = configuration.getDefaultCache();
        } catch (Exception e) {
            throw new CacheException("Cannot configure CacheManager: " + e.getMessage());
        }

        diskStorePath = configuration.getDiskCachePath();
        Set configuredCacheKeys = configuration.getCacheKeySet();
        for (Iterator iterator = configuredCacheKeys.iterator(); iterator.hasNext();) {
            String name = (String) iterator.next();
            addCacheNoCheck(configuration.getCache(name));
        }
    }

    /**
     * A factory method to create a CacheManager with default config.
     * <p>
     * The configuration will be read, {@link Cache}s created and required stores initialized.
     * When the {@link CacheManager} is no longer required, call shutdown to free resources.
     */
    public static CacheManager create() throws CacheException {
        synchronized (CacheManager.class) {
            if (instance == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating new CacheManager with default config");
                }
                instance = new CacheManager();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Attempting to create an existing instance. Existing instance returned.");
                }
            }
            return instance;
        }
    }

    /**
     * A factory method to get an instance ofCacheManager.
     * <p>
     * This has the same effect as {@link CacheManager#create}
     */
    public static CacheManager getInstance() throws CacheException {
        return CacheManager.create();
    }

    /**
     * A factory method to create a CacheManager with a specified configuration.
     * @param configurationFileName an xml file compliant with the ehcache.xsd schema
     * <p>
     * The configuration will be read, {@link Cache}s created and required stores initialized.
     * When the {@link CacheManager} is no longer required, call shutdown to free resources.
     */
    public static CacheManager create(String configurationFileName) throws CacheException {
        synchronized (CacheManager.class) {
            if (instance == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating new CacheManager with config file: " + configurationFileName);
                }
                instance = new CacheManager(configurationFileName);
            }
            return instance;
        }
    }

    /**
     * A factory method to create a CacheManager from an URL.
     * <p>
     * This method makes it possible to specify ehcache.xml, or a differently named
     * config file in the classpath. e.g. this.getClass().getResource(...)
     * <p>
     * @param configurationFileURL an URL to an xml file compliant with the ehcache.xsd schema
     * <p>
     * The configuration will be read, {@link Cache}s created and required stores initialized.
     * When the {@link CacheManager} is no longer required, call shutdown to free resources.
     */
    public static CacheManager create(URL configurationFileURL) throws CacheException {
        synchronized (CacheManager.class) {
            if (instance == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating new CacheManager with config URL: " + configurationFileURL);
                }
                instance = new CacheManager(configurationFileURL);

            }
            return instance;
        }
    }

    /**
     * A factory method to create a CacheManager from a java.io.InputStream.
     * <p>
     * This method makes it possible to use an inputstream for configuration.
     * Note: it is the clients responsibility to close the inputstream.
     * <p>
     * @param inputStream InputStream of xml compliant with the ehcache.xsd schema
     * <p>
     * The configuration will be read, {@link Cache}s created and required stores initialized.
     * When the {@link CacheManager} is no longer required, call shutdown to free resources.
     */
    public static CacheManager create(InputStream inputStream) throws CacheException {
        synchronized (CacheManager.class) {
            if (instance == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating new CacheManager with InputStream");
                }
                instance = new CacheManager(inputStream);
            }
            return instance;
        }
    }

    /**
     * Gets a Cache
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized Cache getCache(String name) throws IllegalStateException {
        checkStatus();
        return (Cache) caches.get(name);
    }

    /**
     * Use this to add a {@link Cache}.
     * Memory and Disk stores will be configured for it and it will be added
     * to the map of caches.
     *
     * It will be created with the defaultCache attributes specified in ehcache.xml
     * @param cacheName the name for the cache
     * @throws ObjectExistsException if the cache already exists
     * @throws CacheException if there was an error creating the cache.
     */
    public synchronized void addCache(String cacheName) throws IllegalStateException,
            ObjectExistsException, CacheException {
        checkStatus();
        if (caches.get(cacheName) != null) {
            throw new ObjectExistsException("Cache " + cacheName + " already exists");
        }
        Cache cache = null;
        try {
            cache = (Cache) defaultCache.clone();
        } catch (CloneNotSupportedException e) {
            LOG.error("Failure adding cache", e);
        }
        cache.setName(cacheName);
        addCache(cache);
    }

    /**
     * Use this to add a {@link Cache}.
     * Memory and Disk stores will be configured for it and it will be added
     * to the map of caches.
     * @param cache
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     * @throws ObjectExistsException if the cache already exists
     * @throws CacheException if there was an error creating the cache.
     */
    public synchronized void addCache(Cache cache) throws IllegalStateException,
            ObjectExistsException, CacheException {
        checkStatus();
        addCacheNoCheck(cache);
    }

    private synchronized void addCacheNoCheck(Cache cache) throws IllegalStateException,
            ObjectExistsException, CacheException {
        if (caches.get(cache.getName()) != null) {
            throw new ObjectExistsException("Cache " + cache.getName() + " already exists");
        }
        cache.initialise(configuration);
        caches.put(cache.getName(), cache);
    }

    /**
     * Checks whether a cache exists.
     * <p/>
     * @param cacheName the cache name to check for
     * @return true if it exists
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized boolean cacheExists(String cacheName) throws IllegalStateException {
        checkStatus();
        return (caches.get(cacheName) != null);
    }

    /**
     * Remove a cache from the CacheManager
     * @param cacheName the cache name
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized void removeCache(String cacheName) throws IllegalStateException {
        checkStatus();
        Cache cache = (Cache) caches.remove(cacheName);
        if (cache != null) {
            cache.dispose();
        }
    }

    /**
     * Each call to {@link #create} must ultimately be matched by a call
     * to this method. A count of instance calls is kept and understood to be the number
     * of clients accessing this {@link CacheManager}.
     * <p>
     * This is the contract with CacheManager clients.
     * <p>
     **/
    public void shutdown() {
        if (status == STATUS_SHUTDOWN) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("CacheManager already shutdown");
            }
            return;
        }
        synchronized (this) {
            Enumeration allCaches = caches.elements();
            while (allCaches.hasMoreElements()) {
                Cache cache = (Cache) allCaches.nextElement();
                if (cache != null) {
                    cache.dispose();
                }
            }
            status = STATUS_SHUTDOWN;
        }
        synchronized (CacheManager.class) {
            instance = null;
        }
    }

    /**
     * Returns a list of the current cache names.
     * @return an array of {@link String}s
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    public synchronized String[] getCacheNames() throws IllegalStateException {
        checkStatus();
        String[] list = new String[caches.size()];
        return (String[]) caches.keySet().toArray(list);
    }

    /**
     * Returns configuration to classes in this package.
     *
     * Used for testing.
     * @throws IllegalStateException if the cache is not {@link #STATUS_ALIVE}
     */
    Configuration getConfiguration() throws IllegalStateException {
        checkStatus();
        return configuration;
    }


    private void checkStatus() {
        if (status != STATUS_ALIVE) {
            throw new IllegalStateException("The CacheManager is not alive.");
        }
    }

    /**
     * Gets the status of the CacheManager.
     * @return one of: {@link CacheManager#STATUS_UNINITIALISED}, {@link CacheManager#STATUS_ALIVE},
     * {@link CacheManager#STATUS_SHUTDOWN}
     */
    public int getStatus() {
        return status;
    }

    public void setDiskStorePath(String path) {
        configuration.setDiskCachePath(path);
    }
    
    public String getDiskStorePath() {
        return configuration.getDiskCachePath();
    }
}

