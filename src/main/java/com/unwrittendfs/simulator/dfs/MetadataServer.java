package com.unwrittendfs.simulator.dfs;

import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.file.FileAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class MetadataServer {

    private Map<String, Integer> mFileDescriptorMapping; // File name to FD mapping
    private Map<Integer, List<Integer>> mFdToChunkMapping; // Map FD to list of chunk IDs
    private Map<Integer, List<DataLocation>> mChunkIdToDataServerMapping; // Map Chunk ID to list of dataservers
    private Map<Integer, FileAttribute> mFileAttributeMapping; // File attributes corresponding to each FD
    private Map<Integer, Map<Integer, Long>> mClientFilePointerMapping; // Clients position corresponding to each open FD
    private static Logger sLog; // Instance of logger

    // TODO: Handle recycling of FDs
    private static int sFdCount = 0; // Used to assign new FDs.
    private static int sChunkCount = 0; // Used to assign chunk IDs.

    public MetadataServer() {
        // Initialize datastructures
        mFileDescriptorMapping = new HashMap<String, Integer>();
        mFdToChunkMapping = new HashMap<Integer, List<Integer>>();
        mChunkIdToDataServerMapping = new HashMap<Integer, List<DataLocation>>();
        mFileAttributeMapping = new HashMap<Integer, FileAttribute>();
        mClientFilePointerMapping = new HashMap<Integer, Map<Integer, Long>>();
        sLog = Logger.getLogger(MetadataServer.class.getSimpleName());
        sLog.setLevel(Simulation.getLogLevel());
    }

    public int createNewFile(String filename, int client_id) {
        // First check if file already exists
        if (mFileDescriptorMapping.get(filename) != null) {
            // File already exists
            sLog.warning("Filename: " + filename + " already exists");
            return -1;
        }
        // It is actually a new file
        mFileDescriptorMapping.put(filename, sFdCount); // insert into FD mapping
        // create file attribute for this FD
        mFileAttributeMapping.put(sFdCount, new FileAttribute(Simulation.getSimulatorTime()));
        // Create mapping from client to this FD
        Map<Integer, Long> clientMap = new HashMap<>();
        clientMap.put(client_id, 0L); // initial offset for client in file is 0
        mClientFilePointerMapping.put(sFdCount, clientMap);
        mFdToChunkMapping.put(sFdCount, new ArrayList<Integer>());
        sFdCount++;
        return sFdCount - 1;
    }

    public int openFile(String filename, int client_id) {
        // First let's get the FD for the filename
        Integer fd = mFileDescriptorMapping.get(filename);
        if (fd == null) {
            // File does not exist
            sLog.warning("Filename: " + filename + " does not exist");
            return -1;
        }
        Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
        if (clientMap.get(client_id) != null) {
            // Clients already has file open. Possibly didn't call a close?
            sLog.info("OpenFile call for already opened file : " + filename + "  by clientId " + client_id);
            return fd;
        }
        clientMap = new HashMap<>();
        clientMap.put(client_id, 0L); // initial offset of 0 for client on an open()
        mClientFilePointerMapping.put(fd, clientMap);
        return fd;
    }

    public boolean closeFile(int fd, int client_id) {
        // Get list of client offsets for fd
        Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
        if (clientMap == null) {
            // File does not exist

            sLog.warning("closeFile call for non-existent fd : " + fd + "  by clientId " + client_id);

            return false;
        }
        Long offset = clientMap.remove(client_id);
        if (offset == null) {
            // Clients calling close on file which it has not opened

            sLog.warning("FD: " + Integer.toString(fd) + " Client " + Integer.toString(client_id)
                    + " calling close on file it has not opened");
            return false;
        }
        mClientFilePointerMapping.put(fd, clientMap); // Update client map
        return true;
    }

    public long seekFile(int fd, long offset, int client_id) {
        // Get list of client offsets for fd
        Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
        if (clientMap == null) {

            // File does not exist
            sLog.warning("FD: " + Integer.toString(fd) + " does not exist");

            return -1;
        }
        Long oldOffset = clientMap.get(client_id);
        if (oldOffset == null) {
            // Clients has not opened the file before

            sLog.warning("FD: " + Integer.toString(fd) + " Client " + Integer.toString(client_id)
                    + " calling close on file it has not opened");
            return -1;
        }
        clientMap.put(client_id, offset); // Update offset
        mClientFilePointerMapping.put(fd, clientMap); // Update client map for FD
        return offset;
    }

    public Map<Integer, List<Integer>> deleteFile(int fd) {
        sLog.info("Delete File FD : " + fd);
        String filename = null;
        // Find filename
        for (String key : mFileDescriptorMapping.keySet()) {
            if (mFileDescriptorMapping.get(key).equals(fd)) {
                filename = key;
                break;
            }
        }
        if (filename == null) {
            // FD does not exist
            sLog.warning("FD: " + Integer.toString(fd) + " does not exist");
            return null;
        }
        mFileDescriptorMapping.remove(filename);
        mClientFilePointerMapping.remove(fd);
        mFileAttributeMapping.remove(fd);
        Map<Integer, List<Integer>> deletedChunks = new HashMap<Integer, List<Integer>>();
        List<Integer> chunks = mFdToChunkMapping.remove(fd);
        for (Integer i : chunks) {
            List<DataLocation> locations = mChunkIdToDataServerMapping.remove(i);
            for (DataLocation location : locations) {
                int dataserver = location.getDataServer();
                // First time encountering this DS
                deletedChunks.computeIfAbsent(dataserver, k -> new ArrayList<Integer>());
                // Add chunk to list of deleted chunks for the DS
                deletedChunks.get(dataserver).add(i);
            }
        }
        return deletedChunks;
    }

    public List<Integer> getChunksForFile(int fd) {
        return mFdToChunkMapping.get(fd);
    }

    public long getOffsetForClient(int fd, int client_id) {
        Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
        if (clientMap == null) {
            sLog.info("Client Map is null while getting offset of FD  : " + fd);
            return -1;
        }
        Long offset = clientMap.get(client_id);
        if (offset == null) {
            sLog.info("Client Map is null while getting offset of FD  : " + fd);
            return -1;
        }
        return offset;
    }

    public List<DataLocation> getDataLocations(int chunk_id) {
        return mChunkIdToDataServerMapping.get(chunk_id);
    }

    public FileAttribute getFileAttributes(int fd) {
        return mFileAttributeMapping.get(fd);
    }

    public int addChunkToFile(int fd, int client_id, List<DataLocation> dataServers) {
        // Get list of client offsets for fd
        Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
        if (clientMap == null) {
            // File does not exist
            sLog.warning("FD: " + Integer.toString(fd) + " does not exist during add chunk");
            return -1;
        }
        Long oldOffset = clientMap.get(client_id);
        if (oldOffset == null) {
            // Clients has not opened the file before
            sLog.warning("FD: " + Integer.toString(fd) + " Client " + Integer.toString(client_id)
                    + " has not opened file during add chunk");
            return -1;
        }
        sLog.info("Adding chunk to file with fd: " + Integer.toString(fd) + " chunkID: " + Integer.toString(sChunkCount));
        List<Integer> chunksForFile = mFdToChunkMapping.get(fd);
        if (chunksForFile == null) {
            // File is getting data for first time
            chunksForFile = new ArrayList<Integer>();
        }
        chunksForFile.add(sChunkCount); // Allocate new chunk
        mFdToChunkMapping.put(fd, chunksForFile);
        // Add dataservers corresponding to chunk
        mChunkIdToDataServerMapping.put(sChunkCount, dataServers);
        sChunkCount++;
        return sChunkCount - 1;
    }

    public boolean removeChunkFromFile(int fd, int client_id, int chunk_id) {
        // Get list of client offsets for fd
        Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
        if (clientMap == null) {
            // File does not exist

            sLog.warning("FD: " + Integer.toString(fd) + " does not exist during remove chunk");

            return false;
        }
        Long oldOffset = clientMap.get(client_id);
        if (oldOffset == null) {
            // Clients has not opened the file before
            sLog.warning("FD: " + Integer.toString(fd) + " Client " + Integer.toString(client_id)
                    + " has not opened file during remove chunk");
            return false;
        }
        List<Integer> chunksForFile = mFdToChunkMapping.get(fd);
        if (chunksForFile == null) {
            return false;
        }
        List<Integer> chunks = mFdToChunkMapping.get(fd);
        chunks.remove(new Integer(chunk_id));
        mFdToChunkMapping.put(fd, chunks);
        mChunkIdToDataServerMapping.remove(chunk_id);
        return true;
    }

    public boolean updateFileSize(int fd, long size, boolean increase) {
        FileAttribute fileattr = mFileAttributeMapping.get(fd);
        if (increase) {
            return fileattr.increaseFileSize(Simulation.getSimulatorTime(), size);
        } else {
            return fileattr.decreaseFileSize(Simulation.getSimulatorTime(), size);
        }
    }
}