/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;

/**
 * This class has been taken from DefaultContainerExecutor class in Hadoop 2.2.0 source code with minor modifications.
 */

public class ResourceLocalizer {
    private static final Logger LOGGER = Logger.getLogger(ResourceLocalizer.class.getName());

    /**
     * Permissions for user dir.
     * $local.dir/usercache/$user
     */
    static final short USER_PERM = (short) 0750;
    /**
     * Permissions for user appcache dir.
     * $local.dir/usercache/$user/appcache
     */
    static final short APPCACHE_PERM = (short) 0710;
    /**
     * Permissions for user filecache dir.
     * $local.dir/usercache/$user/filecache
     */
    static final short FILECACHE_PERM = (short) 0710;
    /**
     * Permissions for user app dir.
     * $local.dir/usercache/$user/appcache/$appId
     */
    static final short APPDIR_PERM = (short) 0710;
    /**
     * Permissions for user log dir.
     * $logdir/$user/$appId
     */
    static final short LOGDIR_PERM = (short) 0710;

    private static final FsPermission PUBLIC_CACHE_OBJECT_PERM = FsPermission.createImmutable((short) 0755);

    private static final Random random = new Random();

    private Configuration config;
    private FileContext lfs;
    private String user;
    private List<String> localDirs;

    private LinkedHashMap<String, CacheStatus> cachedArchives = new LinkedHashMap<String, CacheStatus>();
    private final List<CacheFile> cacheFiles = new ArrayList<CacheFile>();
    private LocalDirAllocator lDirAllocator;
//    protected BaseDirManager baseDirManager = new BaseDirManager();
    private final LocalFileSystem localFs;

    private static class CacheDir {
        long size;
        long subdirs;
    }

//    protected class BaseDirManager {
//        private TreeMap<Path, CacheDir> properties = new TreeMap<Path, CacheDir>();
//
//        void checkAndCleanup() throws IOException {
//            Collection<CacheStatus> toBeDeletedCache = new LinkedList<CacheStatus>();
//            HashMap<Path, CacheDir> toBeCleanedBaseDir = new HashMap<Path, CacheDir>();
//            synchronized (properties) {
//                for (Map.Entry<Path, CacheDir> baseDir : properties.entrySet()) {
//                    CacheDir baseDirCounts = baseDir.getValue();
//                    /*              
//                                  LOG.debug(baseDir.getKey() + ": allowedCacheSize=" + allowedCacheSize +
//                                      ",baseDirCounts.size=" + baseDirCounts.size +
//                                      ",allowedCacheSubdirs=" + allowedCacheSubdirs + 
//                                      ",baseDirCounts.subdirs=" + baseDirCounts.subdirs);
//                                  if (allowedCacheSize < baseDirCounts.size ||
//                                      allowedCacheSubdirs < baseDirCounts.subdirs) {
//                                    CacheDir tcc = new CacheDir();
//                                    tcc.size = baseDirCounts.size - allowedCacheSizeCleanupGoal;
//                                    tcc.subdirs = baseDirCounts.subdirs - allowedCacheSubdirsCleanupGoal;
//                                    toBeCleanedBaseDir.put(baseDir.getKey(), tcc);
//                                  }
//                    */
//                }
//            }
//            // try deleting cache Status with refcount of zero
//            synchronized (cachedArchives) {
//                for (Iterator<Map.Entry<String, CacheStatus>> it = cachedArchives.entrySet().iterator(); it.hasNext();) {
//                    Map.Entry<String, CacheStatus> entry = it.next();
//                    String cacheId = entry.getKey();
//                    CacheStatus cacheStatus = cachedArchives.get(cacheId);
//                    CacheDir leftToClean = toBeCleanedBaseDir.get(cacheStatus.getBaseDir());
//
//                    if (leftToClean != null && (leftToClean.size > 0 || leftToClean.subdirs > 0)) {
//                        boolean gotLock = cacheStatus.lock.tryLock();
//                        if (gotLock) {
//                            try {
//                                // if reference count is zero mark the cache for deletion
//                                boolean isUsed = cacheStatus.isUsed();
//                                long cacheSize = cacheStatus.size;
//                                if (!isUsed) {
//                                    leftToClean.size -= cacheSize;
//                                    leftToClean.subdirs--;
//                                    // delete this cache entry from the global list 
//                                    // and mark the localized file for deletion
//                                    toBeDeletedCache.add(cacheStatus);
//                                    it.remove();
//                                }
//                            } finally {
//                                cacheStatus.lock.unlock();
//                            }
//                        }
//                    }
//                }
//            }
//
//            // do the deletion, after releasing the global lock
//            for (CacheStatus cacheStatus : toBeDeletedCache) {
//                cacheStatus.lock.lock();
//                try {
//                    Path localizedDir = cacheStatus.getLocalizedUniqueDir();
//                    if (cacheStatus.user == null) {
//                        System.out.println("Deleted path " + localizedDir);
//                        try {
//                            localFs.delete(localizedDir, true);
//                        } catch (IOException e) {
//                            System.out.println(String.format("Could not delete distributed cache empty directory "
//                                    + localizedDir, e));
//                        }
//                    } else {
//                        System.out.println("Deleted path " + localizedDir + " as " + cacheStatus.user);
//                        String base = cacheStatus.getBaseDir().toString();
//                        //                String userDir = TaskTracker.getUserDir(cacheStatus.user); 
//                        String userDir = "taskTracker" + Path.SEPARATOR + cacheStatus.user;
//                        int skip = base.length() + 1 + userDir.length() + 1;
//                        String relative = localizedDir.toString().substring(skip);
//                        //                taskController.deleteAsUser(cacheStatus.user, relative);
//                    }
//                    deleteCacheInfoUpdate(cacheStatus);
//                } finally {
//                    cacheStatus.lock.unlock();
//                }
//            }
//        }
//
//        /**
//         * Decrement the size and sub directory count of the cache from baseDirSize
//         * and baseDirNumberSubDir. Have to lock lcacheStatus before calling this.
//         * 
//         * @param cacheStatus
//         *            cache status of the cache is deleted
//         */
//        public void deleteCacheInfoUpdate(CacheStatus cacheStatus) {
//            if (!cacheStatus.inited) {
//                // if it is not created yet, do nothing.
//                return;
//            }
//            // decrement the size of the cache from baseDirSize
//            synchronized (baseDirManager.properties) {
//                CacheDir cacheDir = properties.get(cacheStatus.getBaseDir());
//                if (cacheDir != null) {
//                    cacheDir.size -= cacheStatus.size;
//                    cacheDir.subdirs--;
//                }
//            }
//        }
//
//        /**
//         * Update the maps baseDirSize and baseDirNumberSubDir when adding cache.
//         * Increase the size and sub directory count of the cache from baseDirSize
//         * and baseDirNumberSubDir. Have to lock lcacheStatus before calling this.
//         * 
//         * @param cacheStatus
//         *            cache status of the cache is added
//         */
//        public void addCacheInfoUpdate(CacheStatus cacheStatus) {
//            long cacheSize = cacheStatus.size;
//            // decrement the size of the cache from baseDirSize
//            synchronized (baseDirManager.properties) {
//                CacheDir cacheDir = properties.get(cacheStatus.getBaseDir());
//                if (cacheDir != null) {
//                    cacheDir.size += cacheSize;
//                    cacheDir.subdirs++;
//                } else {
//                    cacheDir = new CacheDir();
//                    cacheDir.size = cacheSize;
//                    cacheDir.subdirs = 1;
//                    properties.put(cacheStatus.getBaseDir(), cacheDir);
//                }
//            }
//        }
//    }

    static class CacheFile {
        /** URI as in the configuration */
        final URI uri;

        enum FileType {
            REGULAR,
            ARCHIVE
        }

        boolean isPublic = true;
        /** Whether to decompress */
        final FileType type;
        final long timestamp;
        /** Whether this is to be added to the classpath */
        final boolean shouldBeAddedToClassPath;
        boolean localized = false;
        /** The owner of the localized file. Relevant only on the tasktrackers */
        final String owner;
        private CacheStatus status;

        CacheFile(URI uri, FileType type, boolean isPublic, long timestamp, boolean classPath) throws IOException {
            this.uri = uri;
            this.type = type;
            this.isPublic = isPublic;
            this.timestamp = timestamp;
            this.shouldBeAddedToClassPath = classPath;
            this.owner = getLocalizedCacheOwner(isPublic);
        }

        static String getLocalizedCacheOwner(boolean isPublic) throws IOException {
            String user;
            if (isPublic) {
                user = UserGroupInformation.getLoginUser().getShortUserName();
            } else {
                user = UserGroupInformation.getCurrentUser().getShortUserName();
            }
            return user;
        }

        /**
         * Set the status for this cache file.
         * 
         * @param status
         */
        public void setStatus(CacheStatus status) {
            this.status = status;
        }

        /**
         * Get the status for this cache file.
         * 
         * @return the status object
         */
        public CacheStatus getStatus() {
            return status;
        }

        /**
         * Converts the scheme used by DistributedCache to serialize what files to
         * cache in the configuration into CacheFile objects that represent those
         * files.
         */
        private static List<CacheFile> makeCacheFiles(URI[] uris, long[] timestamps, boolean cacheVisibilities[],
                Path[] paths, FileType type) throws IOException {
            List<CacheFile> ret = new ArrayList<CacheFile>();
            if (uris != null) {
                if (uris.length != timestamps.length) {
                    throw new IllegalArgumentException("Mismatched uris and timestamps.");
                }
                Map<String, Path> classPaths = new HashMap<String, Path>();
                if (paths != null) {
                    for (Path p : paths) {
                        classPaths.put(p.toUri().getPath().toString(), p);
                    }
                }
                for (int i = 0; i < uris.length; ++i) {
                    URI u = uris[i];
                    boolean isClassPath = (null != classPaths.get(u.getPath()));
                    ret.add(new CacheFile(u, type, cacheVisibilities[i], timestamps[i], isClassPath));
                }
            }
            return ret;
        }

        boolean getLocalized() {
            return localized;
        }

        void setLocalized(boolean val) {
            localized = val;
        }
    }

    class CacheStatus {
        //
        // This field should be accessed under global cachedArchives lock.
        //
        private AtomicInteger refcount; // number of instances using this cache

        //
        // The following two fields should be accessed under
        // individual cacheStatus lock.
        //
        long size; //the size of this cache.
        boolean inited = false; // is it initialized ?
        private final ReentrantLock lock = new ReentrantLock();

        //
        // The following five fields are Immutable.
        //

        // The sub directory (tasktracker/archive or tasktracker/user/archive),
        // under which the file will be localized
        Path subDir;
        // unique string used in the construction of local load path
        String uniqueString;
        // the local load path of this cache
        Path localizedLoadPath;
        //the base dir where the cache lies
        Path localizedBaseDir;
        // The user that owns the cache entry or null if it is public
        final String user;
        //The key of this in the cachedArchives.
        private final String key;

        public CacheStatus(Path baseDir, Path localLoadPath, Path subDir, String uniqueString, String user, String key) {
            super();
            this.localizedLoadPath = localLoadPath;
            this.refcount = new AtomicInteger();
            this.localizedBaseDir = baseDir;
            this.size = 0;
            this.subDir = subDir;
            this.uniqueString = uniqueString;
            this.user = user;
            this.key = key;
        }

        public void incRefCount() {
            lock.lock();
            try {
                refcount.incrementAndGet();
            } finally {
                lock.unlock();
            }
        }

        public void decRefCount() {
            synchronized (cachedArchives) {
                lock.lock();
                try {
                    refcount.decrementAndGet();
                    if (refcount.get() <= 0) {
                        String key = this.key;
                        cachedArchives.remove(key);
                        cachedArchives.put(key, this);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        public int getRefCount() {
            return refcount.get();
        }

        public boolean isUsed() {
            lock.lock();
            try {
                return refcount.get() > 0;
            } finally {
                lock.unlock();
            }
        }

        Path getBaseDir() {
            return this.localizedBaseDir;
        }

        // mark it as initialized
        void initComplete() {
            inited = true;
        }

        // is it initialized?
        boolean isInited() {
            return inited;
        }

        Path getLocalizedUniqueDir() {
            return new Path(localizedBaseDir, new Path(subDir, uniqueString));
        }
    }

    public ResourceLocalizer(Configuration config) throws IOException {
        this.config = config;
        this.lDirAllocator = new LocalDirAllocator("mapred.local.dir"); // mapred.local.dir is deprecated. Instead, use mapreduce.cluster.local.dir
        this.localFs = FileSystem.getLocal(config);

        try {
            this.lfs = FileContext.getLocalFSFileContext(config);

            this.user = UserGroupInformation.getCurrentUser().getShortUserName();
            this.localDirs = Arrays.asList(new JobConf(config).getLocalDirs()); // old style

            determineTimestampsAndCacheVisibilities(config);

            this.cacheFiles.addAll(CacheFile.makeCacheFiles(DistributedCache.getCacheFiles(config),
                    DistributedCache.getFileTimestamps(config), getFileVisibilities(config),
                    DistributedCache.getFileClassPaths(config), CacheFile.FileType.REGULAR));
            this.cacheFiles.addAll(CacheFile.makeCacheFiles(DistributedCache.getCacheArchives(config),
                    DistributedCache.getArchiveTimestamps(config), getArchiveVisibilities(config),
                    DistributedCache.getArchiveClassPaths(config), CacheFile.FileType.ARCHIVE));
            
            if (this.cacheFiles != null){
                System.out.println("[ResourceLocalizer][Constructor] cacheFiles: " + cacheFiles.size());
                int i = 1;
                for (CacheFile cf: cacheFiles){
                    System.out.println("[" + i++ + "] cacheFile: " + cf.uri + ", type: " + cf.type);
                }
            }
        } catch (UnsupportedFileSystemException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void determineTimestampsAndCacheVisibilities(Configuration job) throws IOException {
        Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
        determineTimestamps(job, statCache);
        determineCacheVisibilities(job, statCache);
    }

    static void determineTimestamps(Configuration job, Map<URI, FileStatus> statCache) throws IOException {
        URI[] tarchives = DistributedCache.getCacheArchives(job);
        if (tarchives != null) {
            FileStatus status = getFileStatus(job, tarchives[0], statCache);
            StringBuffer archiveFileSizes = new StringBuffer(String.valueOf(status.getLen()));
            StringBuffer archiveTimestamps = new StringBuffer(String.valueOf(status.getModificationTime()));
            for (int i = 1; i < tarchives.length; i++) {
                status = getFileStatus(job, tarchives[i], statCache);
                archiveFileSizes.append(",");
                archiveFileSizes.append(String.valueOf(status.getLen()));
                archiveTimestamps.append(",");
                archiveTimestamps.append(String.valueOf(status.getModificationTime()));
            }
            job.set(DistributedCache.CACHE_ARCHIVES_SIZES, archiveFileSizes.toString());
            DistributedCache.setArchiveTimestamps(job, archiveTimestamps.toString());
        }

        URI[] tfiles = DistributedCache.getCacheFiles(job);
        if (tfiles != null) {
            FileStatus status = getFileStatus(job, tfiles[0], statCache);
            StringBuffer fileSizes = new StringBuffer(String.valueOf(status.getLen()));
            StringBuffer fileTimestamps = new StringBuffer(String.valueOf(status.getModificationTime()));
            for (int i = 1; i < tfiles.length; i++) {
                status = DistributedCache.getFileStatus(job, tfiles[i]);
                fileSizes.append(",");
                fileSizes.append(String.valueOf(status.getLen()));
                fileTimestamps.append(",");
                fileTimestamps.append(String.valueOf(status.getModificationTime()));
            }
            job.set(DistributedCache.CACHE_FILES_SIZES, fileSizes.toString());
            DistributedCache.setFileTimestamps(job, fileTimestamps.toString());
        }
    }

    static void determineCacheVisibilities(Configuration job, Map<URI, FileStatus> statCache) throws IOException {
        URI[] tarchives = DistributedCache.getCacheArchives(job);
        if (tarchives != null) {
            StringBuffer archiveVisibilities = new StringBuffer(String.valueOf(isPublic(job, tarchives[0], statCache)));
            for (int i = 1; i < tarchives.length; i++) {
                archiveVisibilities.append(",");
                archiveVisibilities.append(String.valueOf(isPublic(job, tarchives[i], statCache)));
            }
            setArchiveVisibilities(job, archiveVisibilities.toString());
        }
        URI[] tfiles = DistributedCache.getCacheFiles(job);
        if (tfiles != null) {
            StringBuffer fileVisibilities = new StringBuffer(String.valueOf(isPublic(job, tfiles[0], statCache)));
            for (int i = 1; i < tfiles.length; i++) {
                fileVisibilities.append(",");
                fileVisibilities.append(String.valueOf(isPublic(job, tfiles[i], statCache)));
            }
            setFileVisibilities(job, fileVisibilities.toString());
        }
    }

    public static boolean[] getFileVisibilities(Configuration conf) {
        return parseBooleans(conf.getStrings(JobContext.CACHE_FILE_VISIBILITIES));
    }

    public static boolean[] getArchiveVisibilities(Configuration conf) {
        return parseBooleans(conf.getStrings(JobContext.CACHE_ARCHIVES_VISIBILITIES));
    }

    private static boolean[] parseBooleans(String[] strs) {
        if (null == strs) {
            return null;
        }
        boolean[] result = new boolean[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            result[i] = Boolean.parseBoolean(strs[i]);
        }
        return result;
    }

    static void setArchiveVisibilities(Configuration conf, String booleans) {
        conf.set(JobContext.CACHE_ARCHIVES_VISIBILITIES, booleans);
    }

    static void setFileVisibilities(Configuration conf, String booleans) {
        conf.set(JobContext.CACHE_FILE_VISIBILITIES, booleans);
    }

    static boolean isPublic(Configuration conf, URI uri, Map<URI, FileStatus> statCache) throws IOException {
        FileSystem fs = FileSystem.get(uri, conf);
        Path current = new Path(uri.getPath());
        //the leaf level file should be readable by others
        if (!checkPermissionOfOther(fs, current, FsAction.READ, statCache)) {
            return false;
        }
        return ancestorsHaveExecutePermissions(fs, current.getParent(), statCache);
    }

    static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path, Map<URI, FileStatus> statCache)
            throws IOException {
        Path current = path;
        while (current != null) {
            //the subdirs in the path should have execute permissions for others
            if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE, statCache)) {
                return false;
            }
            current = current.getParent();
        }
        return true;
    }

    private static boolean checkPermissionOfOther(FileSystem fs, Path path, FsAction action,
            Map<URI, FileStatus> statCache) throws IOException {
        FileStatus status = getFileStatus(fs, path, statCache);
        FsPermission perms = status.getPermission();
        FsAction otherAction = perms.getOtherAction();
        if (otherAction.implies(action)) {
            return true;
        }
        return false;
    }

    private static FileStatus getFileStatus(Configuration job, URI uri, Map<URI, FileStatus> statCache)
            throws IOException {
        FileStatus stat = statCache.get(uri);
        if (stat == null) {
            stat = DistributedCache.getFileStatus(job, uri);
            statCache.put(uri, stat);
        }
        return stat;
    }

    private static FileStatus getFileStatus(FileSystem fs, Path path, Map<URI, FileStatus> statCache)
            throws IOException {
        URI uri = path.toUri();
        FileStatus stat = statCache.get(uri);
        if (stat == null) {
            stat = fs.getFileStatus(path);
            statCache.put(uri, stat);
        }
        return stat;
    }

    public void start() {
        String appId = "hyracksApp";

        try {
            createUserLocalDirs(localDirs, user); // create $local.dir/usercache/$user and its immediate parent  -----/tmp/hadoop-jimahnok/mapred/local/usercache/jimahnok
            createUserCacheDirs(localDirs, user); // create $local.dir/usercache/$user/appcache      ------ make appcache, filecache folders under jimahnok above.
            createAppDirs(localDirs, user, appId);// create $local.dir/usercache/$user/appcache/$appId ----  /tmp/hadoop-jimahnok/mapred/local/usercache/jimahnok/appcache/hyracksApp

            // TODO: Why pick first app dir. The same in LCE why not random?
            Path appStorageDir = getFirstApplicationDir(localDirs, user, appId); // /tmp/hadoop-jimahnok/mapred/local/usercache/jimahnok/appcache/hyracksApp
            lfs.setWorkingDirectory(appStorageDir);
            FileSystem.getLocal(config).setWorkingDirectory(new Path(appStorageDir.toString()));
System.out.println("[ResourceLocalizer][start] NOW workingDir: " + appStorageDir.toUri());

//            setupCache(this.config, "taskTracker" + Path.SEPARATOR + "distcache", "jimahnok" + Path.SEPARATOR + "distcache");
            setupCache(this.config, "taskTracker" + Path.SEPARATOR + "distcache", "usercache" + Path.SEPARATOR + user + Path.SEPARATOR + "filecache");

            if (DistributedCache.getSymlink(config)) {
                URI[] archives = DistributedCache.getCacheArchives(config);
                URI[] files = DistributedCache.getCacheFiles(config);
                Path[] localArchives = DistributedCache.getLocalCacheArchives(config);
                Path[] localFiles = DistributedCache.getLocalCacheFiles(config);
                if(archives != null)
                    System.out.println("archives: " + archives.length);
                if(files != null)
                    System.out.println("files: " + files.length);
                if(localArchives != null)
                    System.out.println("localArchives: " + localArchives.length);
                if(localFiles != null)
                    System.out.println("localFiles: " + localFiles.length);
                
                if (archives != null) {
                    for (int i = 0; i < archives.length; i++) {
                        String link = archives[i].getFragment();
                        String target = localArchives[i].toString();
                        System.out.println("[ResourceLocalizer][start][ARCHIVES] link: " + link + ", target: " + target);
                        symlink(appStorageDir, target, link);
                    }
                }
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        String link = files[i].getFragment();
                        String target = localFiles[i].toString();
                        System.out.println("[ResourceLocalizer][start][FILES] link: " + link + ", target: " + target);
                        symlink(appStorageDir, target, link);
                    }
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // code from TaskDistributedCacheManager class in Hadoop 1.2.1
    public void setupCache(Configuration taskConf, String publicCacheSubdir, String privateCacheSubdir)
            throws IOException {
        ArrayList<Path> localArchives = new ArrayList<Path>();
        ArrayList<Path> localFiles = new ArrayList<Path>();

        for (CacheFile cacheFile : cacheFiles) {
            URI uri = cacheFile.uri;
            FileSystem fileSystem = FileSystem.get(uri, taskConf);
            FileStatus fileStatus = fileSystem.getFileStatus(new Path(uri.getPath()));
            Path p;
            if (cacheFile.isPublic) {
                p = getLocalCache(uri, taskConf, publicCacheSubdir, fileStatus,
                        cacheFile.type == CacheFile.FileType.ARCHIVE, cacheFile.timestamp, cacheFile.isPublic,
                        cacheFile);
                System.out.println("[ResouceLocalizer][setupCache][PUBLIC] p: " + p + ", exist: " + new File(p.toString()).exists());
            } else {
                p = getLocalCache(uri, taskConf, privateCacheSubdir, fileStatus,
                        cacheFile.type == CacheFile.FileType.ARCHIVE, cacheFile.timestamp, cacheFile.isPublic,
                        cacheFile);
                System.out.println("[ResouceLocalizer][setupCache][PRIVATE] p: " + p + ", exist: " + new File(p.toString()).exists());
            }
            cacheFile.setLocalized(true);

            if (cacheFile.type == CacheFile.FileType.ARCHIVE) {
                localArchives.add(p);
            } else {
                localFiles.add(p);
            }
            //            if (cacheFile.shouldBeAddedToClassPath) {
            //              classPaths.add(p.toString());
            //            }
        }

        // Update the configuration object with localized data.
        if (!localArchives.isEmpty()) {
            DistributedCache.addLocalArchives(taskConf, stringifyPathList(localArchives));
        }
        if (!localFiles.isEmpty()) {
            DistributedCache.addLocalFiles(taskConf, stringifyPathList(localFiles));
        }

    }

    private static String stringifyPathList(List<Path> p) {
        if (p == null || p.isEmpty()) {
            return null;
        }
        StringBuilder str = new StringBuilder(p.get(0).toString());
        for (int i = 1; i < p.size(); i++) {
            str.append(",");
            str.append(p.get(i).toString());
        }
        return str.toString();
    }

    Path getLocalCache(URI cache, Configuration conf, String subDir, FileStatus fileStatus, boolean isArchive,
            long confFileStamp, boolean isPublic, CacheFile file) throws IOException {
        String key;
        String user = CacheFile.getLocalizedCacheOwner(isPublic);
        key = getKey(cache, conf, confFileStamp, user, isArchive);
        CacheStatus lcacheStatus;
        Path localizedPath = null;
        Path localPath = null;
        synchronized (cachedArchives) {
            lcacheStatus = cachedArchives.get(key);
            if (lcacheStatus == null) {
System.out.println("11111111111111111111111");                
                // was never localized
                String uniqueString = (String.valueOf(random.nextLong()) + "_" + cache.hashCode() + "_" + (confFileStamp % Integer.MAX_VALUE));
                String cachePath = new Path(subDir, new Path(uniqueString, makeRelative(cache, conf))).toString();
                localPath = lDirAllocator.getLocalPathForWrite(cachePath, fileStatus.getLen(), this.config, isPublic);
                lcacheStatus = new CacheStatus(new Path(localPath.toString().replace(cachePath, "")), localPath,
                        new Path(subDir), uniqueString, isPublic ? null : user, key);
                cachedArchives.put(key, lcacheStatus);
            }

            //mark the cache for use.
            file.setStatus(lcacheStatus);
            lcacheStatus.incRefCount();
        }

        try {
            // do the localization, after releasing the global lock
            synchronized (lcacheStatus) {
                if (!lcacheStatus.isInited()) {
                    System.out.println("22222222222222222222222");                    
                    if (isPublic) {
                        System.out.println("3333333333333333");
                        localizedPath = localizePublicCacheObject(conf, cache, confFileStamp, lcacheStatus, fileStatus,
                                isArchive);
                    } else {
                        System.out.println("44444444444444444");
                        localizedPath = localPath;
                        if (!isArchive) {
                            System.out.println("555555555555555555");
                            //for private archives, the lengths come over RPC from the 
                            //JobLocalizer since the JobLocalizer is the one who expands
                            //archives and gets the total length
                            lcacheStatus.size = fileStatus.getLen();

                            // Increase the size and sub directory count of the cache
                            // from baseDirSize and baseDirNumberSubDir.
//                            baseDirManager.addCacheInfoUpdate(lcacheStatus);
                        }
                        localizedPath = localizePublicCacheObject(conf, cache, confFileStamp, lcacheStatus, fileStatus,
                                isArchive); /////////////////////////////////////////////////////////////////////////////////////////////////////
                    }
                    lcacheStatus.initComplete();
                } else {
                    System.out.println("6666666666666666666");
                    localizedPath = checkCacheStatusValidity(conf, cache, confFileStamp, lcacheStatus, fileStatus,
                            isArchive);
                }
            }
        } catch (IOException ie) {
            lcacheStatus.decRefCount();
            throw ie;
        }
        return localizedPath;
    }

    Path localizePublicCacheObject(Configuration conf, URI cache, long confFileStamp, CacheStatus cacheStatus,
            FileStatus fileStatus, boolean isArchive) throws IOException {
        long size = downloadCacheObject(conf, cache, cacheStatus.localizedLoadPath, confFileStamp, isArchive,
                PUBLIC_CACHE_OBJECT_PERM);
        cacheStatus.size = size;

        // Increase the size and sub directory count of the cache
        // from baseDirSize and baseDirNumberSubDir.
//        baseDirManager.addCacheInfoUpdate(cacheStatus);
        return cacheStatus.localizedLoadPath;
    }

    public static long downloadCacheObject(Configuration conf, URI source, Path destination, long desiredTimestamp,
            boolean isArchive, FsPermission permission) throws IOException {
        FileSystem sourceFs = FileSystem.get(source, conf);
        FileSystem localFs = FileSystem.getLocal(conf);

        Path sourcePath = new Path(source.getPath());
        long modifiedTime = sourceFs.getFileStatus(sourcePath).getModificationTime();
        if (modifiedTime != desiredTimestamp) {
            DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
            throw new IOException("The distributed cache object " + source + " changed during the job from "
                    + df.format(new Date(desiredTimestamp)) + " to " + df.format(new Date(modifiedTime)));
        }

        Path parchive = null;
        if (isArchive) {
            parchive = new Path(destination, destination.getName());
        } else {
            parchive = destination;
        }
        // if the file already exists, we are done
        if (localFs.exists(parchive)) {
            return 0;
        }
        // the final directory for the object
        Path finalDir = parchive.getParent();
        // the work directory for the object
        Path workDir = createRandomPath(finalDir);
        if (!localFs.mkdirs(workDir, permission)) {
            throw new IOException("Mkdirs failed to create directory " + workDir);
        }
        Path workFile = new Path(workDir, parchive.getName());
        sourceFs.copyToLocalFile(sourcePath, workFile);
        localFs.setPermission(workFile, permission);
        if (isArchive) {
            String tmpArchive = workFile.getName().toLowerCase();
            File srcFile = new File(workFile.toString());
            File destDir = new File(workDir.toString());
            if (tmpArchive.endsWith(".jar")) {
                RunJar.unJar(srcFile, destDir);
            } else if (tmpArchive.endsWith(".zip")) {
                FileUtil.unZip(srcFile, destDir);
            } else if (isTarFile(tmpArchive)) {
                FileUtil.unTar(srcFile, destDir);
            } else {
                // else will not do anyhting
                // and copy the file into the dir as it is
            }
            FileUtil.chmod(destDir.toString(), "ugo+rx", true);
        }
        // promote the output to the final location
        if (!localFs.rename(workDir, finalDir)) {
            localFs.delete(workDir, true);
            if (!localFs.exists(finalDir)) {
                throw new IOException("Failed to promote distributed cache object " + workDir + " to " + finalDir);
            }
            // someone else promoted first
            return 0;
        }

        long cacheSize = FileUtil.getDU(new File(parchive.getParent().toString()));
        return cacheSize;
    }

    private static boolean isTarFile(String filename) {
        return (filename.endsWith(".tgz") || filename.endsWith(".tar.gz") || filename.endsWith(".tar"));
    }

    private static Path createRandomPath(Path base) throws IOException {
        return new Path(base.toString() + "-work-" + random.nextLong());
    }

    private Path checkCacheStatusValidity(Configuration conf, URI cache, long confFileStamp, CacheStatus cacheStatus,
            FileStatus fileStatus, boolean isArchive) throws IOException {
        FileSystem fs = FileSystem.get(cache, conf);
        // Has to be
        if (!ifExistsAndFresh(conf, fs, cache, confFileStamp, cacheStatus, fileStatus)) {
            throw new IOException("Stale cache file: " + cacheStatus.localizedLoadPath + " for cache-file: " + cache);
        }
        return cacheStatus.localizedLoadPath;
    }

    private boolean ifExistsAndFresh(Configuration conf, FileSystem fs, URI cache, long confFileStamp,
            CacheStatus lcacheStatus, FileStatus fileStatus) throws IOException {
        long dfsFileStamp;
        if (fileStatus != null) {
            dfsFileStamp = fileStatus.getModificationTime();
        } else {
            dfsFileStamp = DistributedCache.getTimestamp(conf, cache);
        }

        return true;
    }

    String getKey(URI cache, Configuration conf, long timeStamp, String user, boolean isArchive) throws IOException {
        return (isArchive ? "a" : "f") + "^" + makeRelative(cache, conf) + String.valueOf(timeStamp) + user;
    }

    String makeRelative(URI cache, Configuration conf) throws IOException {
        String host = cache.getHost();
        if (host == null) {
            host = cache.getScheme();
        }
        if (host == null) {
            URI defaultUri = FileSystem.get(conf).getUri();
            host = defaultUri.getHost();
            if (host == null) {
                host = defaultUri.getScheme();
            }
        }
        String path = host + cache.getPath();
        path = path.replace(":/", "/"); // remove windows device colon
        return path;
    }

    private static void symlink(Path workDir, String target, String link) throws IOException {
        if (link != null) {
            link = workDir.toString() + Path.SEPARATOR + link;
            File flink = new File(link);
            if (!flink.exists()) {
                System.out.println(String.format("Creating symlink: %s <- %s", target, link));
                if (0 != FileUtil.symLink(target, link)) {
                    System.out.println(String.format("Failed to create symlink: %s <- %s", target, link));
                }
            }
        }
    }

    void createUserLocalDirs(List<String> localDirs, String user) throws IOException {
        boolean userDirStatus = false;
        FsPermission userperms = new FsPermission(USER_PERM);
        for (String localDir : localDirs) {
            // create $local.dir/usercache/$user and its immediate parent
            try {
                createDir(getUserCacheDir(new Path(localDir), user), userperms, true);
            } catch (IOException e) {
                continue;
            }
            userDirStatus = true;
        }
        if (!userDirStatus) {
            throw new IOException("Not able to initialize user directories "
                    + "in any of the configured local directories for user " + user);
        }
    }

    void createUserCacheDirs(List<String> localDirs, String user) throws IOException {
        boolean appcacheDirStatus = false;
        boolean distributedCacheDirStatus = false;
        FsPermission appCachePerms = new FsPermission(APPCACHE_PERM);
        FsPermission fileperms = new FsPermission(FILECACHE_PERM);

        for (String localDir : localDirs) {
            // create $local.dir/usercache/$user/appcache
            Path localDirPath = new Path(localDir);
            final Path appDir = getAppcacheDir(localDirPath, user);
            try {
                createDir(appDir, appCachePerms, true);
                appcacheDirStatus = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
            // create $local.dir/usercache/$user/filecache
            final Path distDir = getFileCacheDir(localDirPath, user);
            try {
                createDir(distDir, fileperms, true);
                distributedCacheDirStatus = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!appcacheDirStatus) {
            throw new IOException("Not able to initialize app-cache directories "
                    + "in any of the configured local directories for user " + user);
        }
        if (!distributedCacheDirStatus) {
            throw new IOException("Not able to initialize distributed-cache directories "
                    + "in any of the configured local directories for user " + user);
        }
    }

    void createAppDirs(List<String> localDirs, String user, String appId) throws IOException {
        boolean initAppDirStatus = false;
        FsPermission appperms = new FsPermission(APPDIR_PERM);
        for (String localDir : localDirs) {
            Path fullAppDir = getApplicationDir(new Path(localDir), user, appId);
            // create $local.dir/usercache/$user/appcache/$appId
            try {
                createDir(fullAppDir, appperms, true);
                initAppDirStatus = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!initAppDirStatus) {
            throw new IOException("Not able to initialize app directories "
                    + "in any of the configured local directories for app " + appId.toString());
        }
    }

    private Path getFileCacheDir(Path base, String user) {
        return new Path(getUserCacheDir(base, user), ContainerLocalizer.FILECACHE);
    }

    private Path getAppcacheDir(Path base, String user) {
        return new Path(getUserCacheDir(base, user), ContainerLocalizer.APPCACHE);
    }

    private Path getUserCacheDir(Path base, String user) {
        return new Path(new Path(base, ContainerLocalizer.USERCACHE), user);
    }

    private void createDir(Path dirPath, FsPermission perms, boolean createParent) throws IOException {
        lfs.mkdir(dirPath, perms, createParent);
        if (!perms.equals(perms.applyUMask(lfs.getUMask()))) {
            lfs.setPermission(dirPath, perms);
        }
    }

    private Path getFirstApplicationDir(List<String> localDirs, String user, String appId) {
        return getApplicationDir(new Path(localDirs.get(0)), user, appId);
    }

    private Path getApplicationDir(Path base, String user, String appId) {
        return new Path(getAppcacheDir(base, user), appId);
    }

    private static List<Path> getPaths(List<String> dirs) {
        List<Path> paths = new ArrayList<Path>(dirs.size());
        for (int i = 0; i < dirs.size(); i++) {
            paths.add(new Path(dirs.get(i)));
        }
        return paths;
    }
}
