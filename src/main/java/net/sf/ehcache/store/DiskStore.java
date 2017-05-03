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
import net.sf.ehcache.Element;
import net.sf.ehcache.CacheException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A disk cache implementation.
 *
 * @author Adam Murdoch
 * @author Greg Luck
 * @version $Id: DiskStore.java,v 1.1.1.1 2005/01/27 18:15:03 pents90 Exp $
 */
public class DiskStore implements Store {
    private static final Log LOG = LogFactory.getLog(DiskStore.class.getName());
    private static final int MS_PER_SECOND = 1000;

    private final String name;
    private boolean active;
    private RandomAccessFile randomAccessFile;

    private HashMap diskElements;
    private ArrayList freeSpace;
    private final Map spool;

    private Thread spoolThread;
    private Thread expiryThread;
    private long expiryThreadInterval;

    private final Cache cache;


    /**
     * If persistent, the disk file will be kept
     * and reused on next startup. In addition the
     * memory store will flush all contents to spool,
     * and spool will flush all to disk.
     */
    private final boolean persistent;

    private final String diskPath;

    private File dataFile;

    /**
     * Used to persist elements
     */
    private File indexFile;

    private int status;

    /**
     * The size in bytes of the disk elements
     */
    private long totalSize;

    /**
     * Creates a disk store.
     *
     * @param cache    the {@link Cache} that the store is part of
     * @param diskPath the directory in which to create data and index files
     */
    public DiskStore(Cache cache, String diskPath) {
        status = Store.STATUS_UNINITIALISED;
        this.cache = cache;
        name = cache.getName();
        this.diskPath = diskPath;
        diskElements = new HashMap();
        freeSpace = new ArrayList();
        spool = new HashMap();
        this.expiryThreadInterval = cache.getDiskExpiryThreadIntervalSeconds();
        this.persistent = cache.isDiskPersistent();


        try {
            initialiseFiles();

            active = true;

            // Start up the spool thread
            spoolThread = new SpoolThread();
            spoolThread.start();

            // Start up the expiry thread if not eternal
            if (!cache.isEternal()) {
                expiryThread = new ExpiryThread();
                expiryThread.start();
            }

            status = Store.STATUS_ALIVE;
        } catch (final Exception e) {
            // Cleanup on error
            dispose();
            LOG.error(name + "Cache: Could not create disk store", e);
        }
    }


    private void initialiseFiles() throws Exception {
        // Make sure the cache directory exists
        final File diskDir = new File(diskPath);
        if (diskDir.exists() && !diskDir.isDirectory()) {
            throw new Exception("Store directory \"" + diskDir.getCanonicalPath() + "\" exists and is not a directory.");
        }
        if (!diskDir.exists() && !diskDir.mkdirs()) {
            throw new Exception("Could not create cache directory \"" + diskDir.getCanonicalPath() + "\".");
        }

        dataFile = new File(diskDir, getDataFileName());

        if (persistent) {
            indexFile = new File(diskDir, getIndexFileName());
            readIndex();
            if (diskElements == null) {
                LOG.debug("Index file dirty or empty. Deleting data file " + getDataFileName());
                dataFile.delete();
            }
        } else {
            LOG.debug("Deleting data file " + getDataFileName());
            dataFile.delete();
        }

        // Open the data file as random access. The dataFile is created if necessary.
        randomAccessFile = new RandomAccessFile(dataFile, "rw");
    }

    /**
     * Asserts that the store is active.
     */
    private void checkActive() throws CacheException {
        if (!active) {
            throw new CacheException(name + " Cache: The Disk store is not active.");
        }
    }

    /**
     * Returns the store type.
     */
    public int getCacheType() {
        return Store.DISK_CACHE;
    }

    /**
     * Returns the cache name this disk cache is spooling for
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the cache status.
     */
    public synchronized int getStatus() {
        return Store.STATUS_ALIVE;
    }

    /**
     * Gets an {@link Element} from the Disk Store.
     *
     * @return The element
     */
    public synchronized Element get(final Serializable key) throws IOException {
        try {
            checkActive();

            // Check in the spool.  Remove if present
            Element element = (Element) spool.remove(key);
            if (element != null) {
                element.updateAccessStatistics();
                return element;
            }

            // Check if the element is on disk
            final DiskElement diskElement = (DiskElement) diskElements.get(key);
            if (diskElement == null) {
                // Not on disk
                return null;
            }

            // Load the element
            randomAccessFile.seek(diskElement.position);
            final byte[] buffer = new byte[diskElement.payloadSize];
            randomAccessFile.readFully(buffer);
            final ByteArrayInputStream instr = new ByteArrayInputStream(buffer);
            final ObjectInputStream objstr = new ObjectInputStream(instr);
            element = (Element) objstr.readObject();
            element.updateAccessStatistics();
            return element;
        } catch (Exception e) {
            LOG.error(name + "Cache: Could not read disk store element for key " + key, e);
        }
        return null;
    }

    /**
     * Gets an {@link Element} from the Disk Store, without updating statistics
     *
     * @return The element
     */
    public synchronized Element getQuiet(final Serializable key) throws IOException {
        try {
            checkActive();

            // Check in the spool.  Remove if present
            Element element = (Element) spool.remove(key);
            if (element != null) {
                //element.updateAccessStatistics(); Don't update statistics
                return element;
            }

            // Check if the element is on disk
            final DiskElement diskElement = (DiskElement) diskElements.get(key);
            if (diskElement == null) {
                // Not on disk
                return null;
            }

            // Load the element
            randomAccessFile.seek(diskElement.position);
            final byte[] buffer = new byte[diskElement.payloadSize];
            randomAccessFile.readFully(buffer);
            final ByteArrayInputStream instr = new ByteArrayInputStream(buffer);
            final ObjectInputStream objstr = new ObjectInputStream(instr);
            element = (Element) objstr.readObject();
            //element.updateAccessStatistics(); Don't update statistics
            return element;
        } catch (Exception e) {
            LOG.error(name + "Cache: Could not read disk store element for key " + key, e);
        }
        return null;
    }


    /**
     * Gets an Array of the keys for all elements in the disk store.
     *
     * @return An Object[] of {@link Serializable} keys
     */
    public synchronized Object[] getKeyArray() {
        Set elementKeySet = diskElements.keySet();
        Set spoolKeySet = spool.keySet();
        Set allKeysSet = new HashSet(elementKeySet.size() + spoolKeySet.size());
        allKeysSet.addAll(elementKeySet);
        allKeysSet.addAll(spoolKeySet);
        return allKeysSet.toArray();
    }

    /**
     * Returns the current store size.
     */
    public synchronized int getSize() {
        try {
            checkActive();
            return diskElements.size() + spool.size();
        } catch (Exception e) {
            LOG.error(name + "Cache: Could not determine size of disk store.", e);
            return 0;
        }
    }

    /**
     * Puts an item into the cache.
     */
    public synchronized void put(final Element entry) throws IOException {
        try {
            checkActive();

            // Spool the entry
            spool.put(entry.getKey(), entry);
            notifyAll();
        } catch (Exception e) {
            LOG.error(name + "Cache: Could not write disk store element for " + entry.getKey(), e);
        }
    }

    /**
     * Removes an item from the cache.
     */
    public synchronized boolean remove(final Serializable key) throws IOException {
        try {
            checkActive();

            // Remove the entry from the spool
            final Object spoolValue = spool.remove(key);
            if (spoolValue != null) {
                return true;
            }

            // Remove the entry from the file
            final DiskElement element = (DiskElement) diskElements.remove(key);
            if (element != null) {
                freeBlock(element);
                return true;
            }
        } catch (Exception e) {
            LOG.error(name + "Cache: Could not remove disk store entry for " + key, e);
        }
        return false;
    }

    /**
     * Marks a block as free.
     */
    private void freeBlock(final DiskElement element) {
        totalSize -= element.payloadSize;
        element.payloadSize = 0;
        freeSpace.add(element);
    }

    /**
     * Removes all cached items from the cache.
     * <p/>
     */
    public synchronized void removeAll() throws IOException {
        try {
            checkActive();

            // Ditch all the elements, and truncate the file
            spool.clear();
            diskElements.clear();
            freeSpace.clear();
            totalSize = 0;
            randomAccessFile.setLength(0);
            if (persistent) {
                indexFile.delete();
                indexFile.createNewFile();
            }
        } catch (Exception e) {
            // Clean up
            LOG.error(name + " Cache: Could not rebuild disk store", e);
            dispose();
        }
    }

    /**
     * Shuts down the disk store in preparation for cache shutdown
     * <p/>
     * If a VM crash happens, the shutdown hook will not run. The data file and the index file
     * will be out of synchronisation. At initialisation we always delete the index file
     * after we have read the elements, so that it has a zero length. On a dirty restart, it still will have
     * and the data file will automatically be deleted, thus preserving safety.
     */
    public synchronized void dispose() {

        if (!active) {
            return;
        }

        // Close the cache
        try {
            if (expiryThread != null) {
                expiryThread.interrupt();
            }

            //Flush the spool if persistent, so we don't lose any data.
            if (persistent) {
                flushSpool();
                writeIndex();
            }

            //Clear in-memory data structures
            spool.clear();
            diskElements.clear();
            freeSpace.clear();
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
            if (!persistent) {
                LOG.debug("Deleting file " + dataFile.getName());
                dataFile.delete();
            }
        } catch (Exception e) {
            LOG.error(name + "Cache: Could not shut down disk cache", e);
        } finally {
            active = false;
            randomAccessFile = null;
            notifyAll();
        }
    }

    /**
     * Whether there are any elements waiting to be spooled to disk.
     *
     * @return false if there are elements waiting, otherwise true
     */
    public synchronized boolean isSpoolEmpty() {
        return (!active || spool.size() == 0);
    }

    /**
     * Main method for the spool thread.
     * <p/>
     * Note that the spool thread locks the cache for the entire time it is writing elements to the disk.
     * TODO - Give cache lookups preference to writes
     */
    private synchronized void spoolThreadMain() {
        while (true) {
            // Wait for elements in the spool
            while (active && spool.size() == 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // Bail
                    return;
                }
            }
            if (!active) {
                return;
            }

            // Write elements to disk
            try {
                flushSpool();
            } catch (IOException e) {
                LOG.error(name + "Cache: Could not write elements to disk cache", e);
            }
        }
    }

    /**
     * Flushes all spooled elements to disk.
     * Note that the cache is locked for the entire time that the spool is being flushed.
     */
    private synchronized void flushSpool() throws IOException {
        try {
            // Write elements to the DB
            for (Iterator iterator = spool.values().iterator(); iterator.hasNext();) {
                final Element element = (Element) iterator.next();
                final Serializable key = element.getKey();

                // Remove the old entry, if any
                final DiskElement oldBlock = (DiskElement) diskElements.remove(key);
                if (oldBlock != null) {
                    freeBlock(oldBlock);
                }

                // Serialise the entry
                final ByteArrayOutputStream outstr = new ByteArrayOutputStream();
                final ObjectOutputStream objstr = new ObjectOutputStream(outstr);
                objstr.writeObject(element);
                objstr.close();
                final byte[] buffer = outstr.toByteArray();

                // Check for a free block
                DiskElement diskElement = findFreeBlock(buffer.length);
                if (diskElement == null) {
                    diskElement = new DiskElement();
                    diskElement.position = randomAccessFile.length();
                    diskElement.blockSize = buffer.length;
                }

                // TODO - cleanup block on failure
                // Write the record
                randomAccessFile.seek(diskElement.position);
                //todo the free block algorithm will gradually leak disk space, due to
                //payload size being less than block size
                //this will be a problem for the persistent cache
                randomAccessFile.write(buffer);

                if (cache.isEternal()) {
                    // Never expires
                    diskElement.expiryTime = Long.MAX_VALUE;
                } else {
                    // Calculate expiry time
                    long timeToLive = element.getCreationTime() + cache.getTimeToLiveSeconds() * MS_PER_SECOND;
                    long timeToIdle = element.getLastAccessTime() + cache.getTimeToIdleSeconds() * MS_PER_SECOND;
                    diskElement.expiryTime = Math.max(timeToLive, timeToIdle);
                }

                // Add to index, update stats
                diskElement.payloadSize = buffer.length;
                totalSize += buffer.length;
                diskElements.put(key, diskElement);
            }
        } finally {
            // Clear the spool.  Do this regardless of whether the writes failed - just ditch the elements
            spool.clear();
        }
    }

    /**
     * Writes the Index to disk on shutdown
     * <p/>
     * The index consists of the elements Map and the freeSpace List
     * <p/>
     * Note that the cache is locked for the entire time that the index is being written
     */
    private synchronized void writeIndex() throws IOException {

        ObjectOutputStream objectOutputStream = null;
        try {
            FileOutputStream fout = new FileOutputStream(indexFile);
            objectOutputStream = new ObjectOutputStream(fout);
            objectOutputStream.writeObject(diskElements);
            objectOutputStream.writeObject(freeSpace);
        } finally {
            objectOutputStream.close();
        }
    }

    /**
     * Reads Index to disk on startup.
     * <p/>
     * if the index file does not exist, it creates a new one.
     * <p/>
     * Note that the cache is locked for the entire time that the index is being written
     */
    private synchronized void readIndex() throws IOException {
        ObjectInputStream objectInputStream = null;
        FileInputStream fin = null;
        if (indexFile.exists()) {
            try {
                fin = new FileInputStream(indexFile);
                objectInputStream = new ObjectInputStream(fin);
                diskElements = (HashMap) objectInputStream.readObject();
                freeSpace = (ArrayList) objectInputStream.readObject();
            } catch (StreamCorruptedException e) {
                LOG.error("Corrupt index file. Creating new index.");
            } catch (IOException e) {
                LOG.error("IOException reading index. Creating new index. ");
            } catch (ClassNotFoundException e) {
                LOG.error("Class loading problem reading index. Creating new index. ", e);
            } finally {
                try {
                    if (objectInputStream != null) {
                        objectInputStream.close();
                    } else if (fin != null) {
                        fin.close();
                    }
                } catch (IOException e) {
                    LOG.error("Problem closing the index file.");
                }

                //Always zero out file. That way if there is a dirty shutdown, the file will still be empty
                //the next time we start up and readIndex will automatically fail.
                //If there was a problem reading the index this time we also want to zero it out.
                createNewIndexFile();
            }
        } else {
            createNewIndexFile();
        }
    }

    private void createNewIndexFile() throws IOException {
        if (indexFile.exists()) {
            indexFile.delete();
            LOG.debug("Index file " + indexFile + " deleted.");
        }
        if (indexFile.createNewFile()) {
            LOG.debug("Index file " + indexFile + " created successfully");
        } else {
            throw new IOException("Index file " + indexFile + " could not created.");
        }
    }

    /**
     * The main method for the expiry thread.
     * <p/>
     * Will run while the cache is active. After the cache shuts down
     * it will take the expiryThreadInterval to wake up and complete.
     */
    private void expiryThreadMain() {
        long expiryThreadIntervalMillis = expiryThreadInterval * MS_PER_SECOND;
        try {
            while (active) {
                Thread.sleep(expiryThreadIntervalMillis);

                //Expire the elements
                expireElements();
            }
        } catch (InterruptedException e) {
            // Bail on interruption
            if (LOG.isDebugEnabled()) {
                LOG.debug(name + "Cache: Expiry thread interrupted on Disk Store.");
            }
        }
    }

    /**
     * Removes expired elements.
     * Note that the cache is locked for the entire time that elements are being expired.
     */
    private synchronized void expireElements() {
        final long now = System.currentTimeMillis();

        // Clean up the spool
        for (Iterator iterator = spool.values().iterator(); iterator.hasNext();) {
            final Element element = (Element) iterator.next();
            if (cache.isExpired(element)) {
                // An expired element
                if (LOG.isDebugEnabled()) {
                    LOG.debug(name + "Cache: Removing expired spool element " + element.getKey() + " from Disk Store");
                }
                iterator.remove();
            }
        }

        // Clean up disk elements
        for (Iterator iterator = diskElements.entrySet().iterator(); iterator.hasNext();) {
            final Map.Entry entry = (Map.Entry) iterator.next();
            final DiskElement element = (DiskElement) entry.getValue();
            if (now >= element.expiryTime) {
                // An expired element
                if (LOG.isDebugEnabled()) {
                    LOG.debug(name + "Cache: Removing expired spool element " + entry.getKey() + " from Disk Store");
                }
                iterator.remove();
                freeBlock(element);
            }
        }
    }

    /**
     * Allocates a free block.
     */
    private DiskElement findFreeBlock(final int length) {
        for (int i = 0; i < freeSpace.size(); i++) {
            final DiskElement element = (DiskElement) freeSpace.get(i);
            if (element.blockSize >= length) {
                freeSpace.remove(i);
                return element;
            }
        }
        return null;
    }

    /**
     * Returns a {@link String} representation of the {@link DiskStore}
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[ dataFile = ").append(dataFile.getAbsolutePath())
                .append(", active=").append(active)
                .append(", totalSize=").append(totalSize)
                .append(", status=").append(status)
                .append(", expiryThreadInterval = ").append(expiryThreadInterval)
                .append(" ]");
        return sb.toString();
    }

    /**
     * A reference to an on-disk elements.
     */
    private static class DiskElement implements Serializable {
        /**
         * the file pointer
         */
        private long position;

        /**
         * The size used for data.
         */
        private int payloadSize;

        /**
         * the size of this element.
         */
        private int blockSize;

        /**
         * The expiry time in milliseconds
         */
        private long expiryTime;

    }

    /**
     * A background thread that writes objects to the file.
     */
    private class SpoolThread extends Thread {
        public SpoolThread() {
            super("Store " + name + " Spool Thread");
            setDaemon(true);
        }

        /**
         * Main thread method.
         */
        public void run() {
            spoolThreadMain();
        }
    }

    /**
     * A background thread that removes expired objects.
     */
    private class ExpiryThread extends Thread {
        public ExpiryThread() {
            super("Store " + name + " Expiry Thread");
            setDaemon(true);
        }

        /**
         * Main thread method.
         */
        public void run() {
            expiryThreadMain();
        }
    }

    /**
     * @return the total size of the data file and the index file, in bytes.
     */
    public long getTotalFileSize() {
        return getDataFileSize() + getIndexFileSize();
    }

    /**
     * @return the size of the data file in bytes.
     */
    public long getDataFileSize() {
        return dataFile.length();
    }

    /**
     * The design of the layout on the data file means that there will be small gaps created when DiskElements
     * are reused.
     *
     * @return the sparseness, measured as the percentage of space in the Data File not used for holding data
     */
    public float calculateDataFileSparseness() {
        return 1 - ((float) getUsedDataSize() / (float) getDataFileSize());
    }

    /**
     * When elements are deleted, spaces are left in the file. These spaces are tracked and are reused
     * when new elements need to be written.
     * <p/>
     * This method indicates the actual size used for data, excluding holes. It can be compared with
     * {@link #getDataFileSize()} as a measure of fragmentation.
     */
    public long getUsedDataSize() {
        return totalSize;
    }

    /**
     * @return the size of the index file, in bytes.
     */
    public long getIndexFileSize() {
        if (indexFile == null) {
            return 0;
        } else {
            return indexFile.length();
        }
    }

    /**
     * @return the file name of the data file where the disk store stores data, without any path information.
     */
    public String getDataFileName() {
        return name + ".data";
    }

    /**
     * @return the disk path, which will be dependent on the operating system
     */
    public String getDataFilePath() {
        return diskPath;
    }

    /**
     * @return the file name of the index file, which maintains a record of elements and their addresses
     *         on the data file, without any path information.
     */
    public String getIndexFileName() {
        return name + ".index";
    }


    /**
     * The expiry thread is started provided the cache is not eternal
     * <p/>
     * If started it will continue to run until the {@link #dispose()} method is called,
     * at which time it should be interrupted and then die.
     *
     * @return true if an expiryThread was created and is still alive.
     */
    public boolean isExpiryThreadAlive() {
        if (expiryThread == null) {
            return false;
        } else {
            return expiryThread.isAlive();
        }
    }


}
