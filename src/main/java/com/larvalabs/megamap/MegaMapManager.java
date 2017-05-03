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

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.CacheException;

import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;
import java.io.File;

/**
 * This the user's entry point to creating and managing MegaMaps.
 *
 * @see #createMegaMap(String, boolean, boolean)
 * @see #createMegaMap(String, String, boolean, boolean)
 * @see #shutdown()
 * @author John Watkinson
 */
public class MegaMapManager {

    private static final String STORE_SUFFIX = ".data";
    private static final String INDEX_SUFFIX = ".index";

    private static MegaMapManager instance;
    private static final String SYSTEM_TEMP_DIR = "java.io.tmpdir";

    /**
     * Gets the (singleton) instance of the MegaMapManager.
     *
     * @throws MegaMapException if unable to create the instance.
     */
    public static MegaMapManager getMegaMapManager() throws MegaMapException {
        synchronized (MegaMapManager.class) {
            if (instance == null) {
                instance = new MegaMapManager();
            }
            return instance;
        }
    }

    private Map maps;
    private CacheManager manager;

    private MegaMapManager() throws MegaMapException {
        maps = new HashMap();
        try {
            manager = CacheManager.create();
        } catch (CacheException e) {
            throw new MegaMapException("Exception while initializing MegaMapManager", e);
        }
    }

    /**
     * Sets the default path to which to store MegaMap files.
     * If this method is not called, or if it is called with a null parameter then this will default to the
     * default system temp directory (specified by the system property <code>java.io.tmpdir</code>.
     */
    public synchronized void setDiskStorePath(String diskStorePath) {
        manager.setDiskStorePath(diskStorePath);
    }

    /**
     * Throws an exception if the cache name is invalid.
     */
    private String validateCacheName(String name) throws MegaMapException {
        if (name == null) {
            throw new MegaMapException("Cache name cannot be null!");
        } else if (name.length() > 200) {
            throw new MegaMapException("Cache name cannot be longer than 200 characters.");
        } else {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < name.length(); i++) {
                char c = name.charAt(i);
                if ( ((c >= 'A') && (c <='Z')) || ((c >= 'a') && (c <= 'z')) || ((c >= '0') && (c <= '9'))) {
                    sb.append(c);
                } else {
                    sb.append('_');
                }
            }
            return sb.toString();
        }
    }

    /**
     * Creates a new MegaMap (or loads a persisted MegaMap from disk).
     *
     * @param name       the name of the MegaMap. Must consist of letters,
     *                   numbers and the underscore character only (A-Z, a-z, 1-9, _).
     * @param persistent if <code>true</code>, then will persist the cache to disk between VM invocations.
     *                   <b>Note</b>: It is very improtant that the {@link #shutdown} method is called upon shutdown of
     *                   the application if any MegaMaps are created with <code>persistent</code> set to true.
     *                   Otherwise, the integrity of the persisted data cannot be ensured.
     * @return the created MegaMap.
     * @throws MegaMapException if a MegaMap by the name given already exists,
     * if the name given is invalid, or if the MegaMap is unable to write to the disk.
     */
    public synchronized MegaMap createMegaMap(String name, boolean persistent, boolean overwriteOld) throws MegaMapException {
        return createMegaMap(name, null, persistent, overwriteOld);
    }

    /**
     * Creates a new MegaMap (or loads a persisted MegaMap from disk) using the specified path for writing the MegaMap files.
     *
     * @param name       the name of the MegaMap. Must consist of letters,
     *                   numbers and the underscore character only (A-Z, a-z, 1-9, _).
     * @param path       the directory in which to write the MegaMap files.
     * @param persistent if <code>true</code>, then will persist the cache to disk between VM invocations.
     *                   <b>Note</b>: It is very improtant that the {@link #shutdown} method is called upon shutdown of
     *                   the application if any MegaMaps are created with <code>persistent</code> set to true.
     *                   Otherwise, the integrity of the persisted data cannot be ensured.
     * @return the created MegaMap.
     * @throws MegaMapException if a MegaMap by the name given already exists,
     * if the name given is invalid, or if the MegaMap is unable to write to the disk.
     */
    public synchronized MegaMap createMegaMap(String name, String path, boolean persistent, boolean overwriteOld) throws MegaMapException {
        // Validate name
        name = validateCacheName(name);
        // Ensure that it is not already in use
        if (maps.get(name) != null) {
            throw new MegaMapException("MegaMap with name '" + name + "' already exists!");
        }
        // Overwrite old cache if necessary
        if (persistent && overwriteOld) {
            deleteMapFiles(path, name);
        }
        String oldPath = manager.getDiskStorePath();
        if (path != null) {
            manager.setDiskStorePath(path);
        }
        MegaMap megaMap = new MegaMap(name, manager, persistent);
        if (path != null) {
            manager.setDiskStorePath(oldPath);
        }
        maps.put(name, megaMap);
        return megaMap;
    }

    private void deleteMapFiles(String path, String validatedName) throws MegaMapException {
        // Delete the files if they are there
        String filePath;
        if (path == null) {
            filePath = manager.getDiskStorePath();
            if (filePath == null) {
                filePath = System.getProperty(SYSTEM_TEMP_DIR);
            }
        } else {
            filePath = path;
        }
        File storeFile = new File(filePath, validatedName + STORE_SUFFIX);
        storeFile.delete();
        File indexFile = new File(filePath, validatedName + INDEX_SUFFIX);
        indexFile.delete();
    }

    /**
     * Retrieves a previously-created MegaMap.
     * @param name the name of the MegaMap.
     */
    public synchronized MegaMap getMegaMap(String name) {
        try {
            name = validateCacheName(name);
        } catch (MegaMapException e) {
            return null;
        }
        MegaMap megaMap = (MegaMap) maps.get(name);
        return megaMap;
    }

    /**
     * Removes a MegaMap from the manager.
     * This causes the map to be flushed to disk (if persistent) and its supporting thread to be shut down.
     * <b>Note:</b> This does not result in the deletion of a persistent cache. See {@link #deletePersistedMegaMap(String)}
     * and {@link #deletePersistedMegaMap(String, String)} for that functionality.
     * @param name the name of the cache to remove from management.
     */
    public synchronized void removeMegaMap(String name) {
        try {
            name = validateCacheName(name);
        } catch (MegaMapException e) {
            return;
        }
        MegaMap megaMap = (MegaMap) maps.remove(name);
        megaMap.shutdown();
        manager.removeCache(name);
    }

    /**
     * Deletes a previously persisted MegaMap. The MegaMap must not be active or an exception will be thrown.
     * @param name the name of the MegaMap to delete (the current disk store path will be used).
     * @throws MegaMapException if the named MegaMap is currently active.
     * @see #deletePersistedMegaMap(java.lang.String, java.lang.String)
     */
    public synchronized void deletePersistedMegaMap(String name) throws MegaMapException {
        deletePersistedMegaMap(name, null);
    }

    /**
     * Deletes a previously persisted MegaMap. The MegaMap must not be active or an exception will be thrown.
     * @param name the name of the MegaMap to delete.
     * @param path the path to the MegaMap files.
     * @throws MegaMapException if the named MegaMap is currently active.
     * @see #deletePersistedMegaMap(java.lang.String)
     */
    public synchronized void deletePersistedMegaMap(String name, String path) throws MegaMapException {
        name = validateCacheName(name);
        if (maps.get(name) != null) {
            throw new MegaMapException("Could not delete MegaMap '" + name + "' because it is active!");
        } else {
            deleteMapFiles(path, name);
        }
    }

    /**
     * Shuts down the MegaMapManager and all MegaMaps. Persistent MegaMaps will all be saved to disk.
     * There should be no more use of MegaMaps after this method is called.
     * This method should be explicitly called upon shutdown of any application that makes use of persistent
     * MegaMaps. It is not as important for non-persistent MegaMaps, but failing to call this will leave temporary
     * files in the filesystem that otherwise would be deleted. The finalizer for MegaMapManager calls this method,
     * so the call java.lang.System.runFinalizersOnExit(true) will ensure that is called. However, this method is
     * deprecated with no replacement in recent versions of the JDK, so use the system call at your own risk.
     */
    public synchronized void shutdown() {
        Collection allMaps = maps.values();
        for (Iterator iterator = allMaps.iterator(); iterator.hasNext();) {
            MegaMap megaMap = (MegaMap) iterator.next();
            megaMap.shutdown();
        }
        manager.shutdown();
        manager = null;
        instance = null;
    }

    protected void finalize() throws Throwable {
        shutdown();
    }

}
