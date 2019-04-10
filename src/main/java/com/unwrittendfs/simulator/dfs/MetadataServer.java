package com.unwrittendfs.simulator.dfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.file.FileAttribute;

public class MetadataServer {

	private Map<String, Integer> mFileDescriptorMapping; // File name to FD mapping
	private Map<Integer, List<Integer>> mFdToChunkMapping; // Map FD to list of chunk IDs
	private Map<Integer, List<DataLocation>> mChunkIdToDataServerMapping; // Map Chunk ID to list of dataservers
	private Map<Integer, FileAttribute> mFileAttributeMapping; // File attributes corresponding to each FD
	private Map<Integer, Map<Integer, Long>> mClientFilePointerMapping; // Client position corresponding to each open FD

	// TODO: Handle recycling of FDs
	private static int sFdCount = 0; // Used to assign new FDs.

	public MetadataServer() {
		// Initialize datastructures
		mFileDescriptorMapping = new HashMap<String, Integer>();
		mFdToChunkMapping = new HashMap<Integer, List<Integer>>();
		mChunkIdToDataServerMapping = new HashMap<Integer, List<DataLocation>>();
		mFileAttributeMapping = new HashMap<Integer, FileAttribute>();
		mClientFilePointerMapping = new HashMap<Integer, Map<Integer, Long>>();
	}

	public int createNewFile(String filename, int client_id) {
		// First check if file already exists
		if (mFileDescriptorMapping.get(filename) != null) {
			// File already exists
			return -1;
		}
		// It is actually a new file
		mFileDescriptorMapping.put(filename, sFdCount); // insert into FD mapping
		// create file attribute for this FD
		mFileAttributeMapping.put(sFdCount, new FileAttribute(Simulation.getSimulatorTime()));
		// Create mapping from client to this FD
		Map<Integer, Long> clientMap = new HashMap<>();
		clientMap.put(client_id, (long) 0); // initial offset for client in file is 0
		mClientFilePointerMapping.put(sFdCount, clientMap);
		sFdCount++;
		return sFdCount - 1;
	}

	public int openFile(String filename, int client_id) {
		// First let's get the FD for the filename
		Integer fd = mFileDescriptorMapping.get(filename);
		if (fd == null) {
			// File does not exist
			return -1;
		}
		Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
		if (clientMap.get(client_id) != null) {
			// Client already has file open. Possibly didn't call a close?
			return fd;
		}
		clientMap = new HashMap<>();
		clientMap.put(client_id, (long) 0); // initial offset of 0 for client on an open()
		mClientFilePointerMapping.put(fd, clientMap);
		return fd;
	}

	public boolean closeFile(int fd, int client_id) {
		// Get list of client offsets for fd
		Map<Integer, Long> clientMap = mClientFilePointerMapping.get(fd);
		if (clientMap == null) {
			// File does not exist
			return false;
		}
		Long offset = clientMap.remove(client_id);
		if (offset == null) {
			// Client calling close on file which it has not opened
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
			return -1;
		}
		Long oldOffset = clientMap.get(client_id);
		if(oldOffset == null) {
			// Client has not opened the file before
			return -1;
		}
		clientMap.put(client_id, offset); // Update offset
		mClientFilePointerMapping.put(fd, clientMap); // Update client map for FD
		return offset;
	}
	
	public Map<Integer, List<Integer>> deleteFile(int fd) {
		String filename = null;
		// Find filename
		for(String key : mFileDescriptorMapping.keySet()) {
			if(mFileDescriptorMapping.get(key).equals(fd)) {
				filename = key;
				break;
			}
		}
		if(filename == null) {
			// FD does not exist
			return null;
		}
		mFileDescriptorMapping.remove(filename);
		mClientFilePointerMapping.remove(fd);
		mFileAttributeMapping.remove(fd);
		Map<Integer, List<Integer>> deletedChunks = new HashMap<Integer, List<Integer>>();
		List<Integer> chunks = mFdToChunkMapping.remove(fd);
		for(Integer i : chunks) {
			List<DataLocation> locations = mChunkIdToDataServerMapping.remove(i);
			for(DataLocation location : locations) {
				int dataserver = location.getDataServer();
				if(deletedChunks.get(dataserver) == null) { // First time encountering this DS
					deletedChunks.put(dataserver, new ArrayList<Integer>());
				}
				// Add chunk to list of deleted chunks for the DS
				deletedChunks.get(dataserver).add(i);
			}
		}
		return deletedChunks;
	}
	
	public FileAttribute getFileAttributes(int fd) {
		return mFileAttributeMapping.get(fd);
	}
}