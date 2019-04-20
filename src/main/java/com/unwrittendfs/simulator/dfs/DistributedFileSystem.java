package com.unwrittendfs.simulator.dfs;

import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dataserver.DataLocation;
import com.unwrittendfs.simulator.dataserver.DataServer;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.exceptions.GenericException;
import com.unwrittendfs.simulator.exceptions.PageCorruptedException;
import com.unwrittendfs.simulator.file.FileAttribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedFileSystem {

	private static DistributedFileSystem sInstance = null;
	private MetadataServer mMetadataServer; // Holds instance of Metadata server
	protected ClusterConfiguration mClusterConfiguration; // Holds instance of cluster configuration
	protected Map<Integer, DataServer> mDataServerMap; // List of available data-servers
	private static Logger sLog; // Instance of logger

	protected DistributedFileSystem(ClusterConfiguration config, List<DataserverConfiguration> dataserverConfigurations) {
		mClusterConfiguration = config;
		mMetadataServer = new MetadataServer();
		mDataServerMap = new HashMap<Integer, DataServer>();
		for(DataserverConfiguration server : dataserverConfigurations){
			mDataServerMap.put(server.getDataServerId(), new DataServer(server));
		}
		sInstance = this;
		sLog = Logger.getLogger(DistributedFileSystem.class.getSimpleName());
		sLog.setLevel(Simulation.getLogLevel());
	}

	public static DistributedFileSystem getInstance() {
		return sInstance;
	}

	public int create(String filename, int client_id) {
		return mMetadataServer.createNewFile(filename, client_id);
	}

	public int open(String filename, int client_id) {
		return mMetadataServer.openFile(filename, client_id);
	}

	public boolean close(int fd, int client_id) {
		return mMetadataServer.closeFile(fd, client_id);
	}

	public long read(int fd, String buffer, long count, int client_id) throws PageCorruptedException, GenericException {
		// Get the list of chunks from MDS
		List<Integer> chunks = mMetadataServer.getChunksForFile(fd);
		if (chunks == null) {
			sLog.warning("FD:" + Integer.toString(fd) + " File does not exist");
			// No contents in file
			return -1;
		}
		// Get the current offset for this client for this FD
		long offset = mMetadataServer.getOffsetForClient(fd, client_id);
		if (offset == -1) {
			// Clients has not yet opened file
			sLog.warning("FD:" + Integer.toString(fd) + " Client has not opened file");
			return -1;
		}
		sLog.info("FD:" + Integer.toString(fd) + " chunks:" + chunks.toString());
		// Identify which chunks to read.
		int start_chunk = (int) (offset / mClusterConfiguration.getChunkSize());
		int end_chunk = (int) ((offset + count) / mClusterConfiguration.getChunkSize());
		// Need to read from [start_chunk, end_chunk)
		int total_chunks_for_file = chunks.size();
		long bytesRead = 0;
		for (int i = start_chunk; i < end_chunk; i++) {
			if (i < total_chunks_for_file) {
				// Get data server to read this chunk from
				List<DataLocation> locations = mMetadataServer.getDataLocations(chunks.get(i));
				if (locations == null) {
					sLog.warning("FD:" + Integer.toString(fd) + "Can't find chunk location");
					break;
				}
				// Order the data locations in some priority order for issuing reads.
				List<DataServer> orderedServers = orderServersToRead(locations);
				long bytes = 0;
				for (DataServer server : orderedServers) {
					sLog.log(Level.INFO, "FD:" + Integer.toString(fd) + " Reading chunk " + Integer.toString(chunks.get(i))
						+ " Reading from server " + Integer.toString(server.getConfig().getDataServerId()));
					bytes = server.read(chunks.get(i));
					if (bytes == mClusterConfiguration.getChunkSize()) {
						// Was able to read data in it's entirety
						break;
					}
				}
				if (bytes < mClusterConfiguration.getChunkSize()) {
					// Not able to read the chunk successfully
					break;
				}
				bytesRead += bytes;
			} else {
				break;
			}
		}
		// Update client offset after reading
		seek(fd, offset + bytesRead, client_id);
		return bytesRead;
	}

	protected List<DataServer> orderServersToRead(List<DataLocation> locations) {
		// Implement this API in each Distributed File System Implementation
		return null;
	}

	public long write(int fd, String buffer, long count, int client_id) throws GenericException {
		// Get the list of chunks from MDS
		List<Integer> chunks = mMetadataServer.getChunksForFile(fd);
		// Get the current offset for this client for this FD
		long offset = mMetadataServer.getOffsetForClient(fd, client_id);
		if (offset == -1) {
			// Clients has not yet opened file
			sLog.warning("FD:" + Integer.toString(fd) + " Client has not yet opened file");
			return -1;
		}
		// Identify which chunks to overwrite and how many new chunks need to be created
		sLog.info("FD:" + Integer.toString(fd) + " count: " + Long.toString(count));
		int start_chunk = (int) (offset / mClusterConfiguration.getChunkSize());
		int end_chunk = (int) ((offset + count) / mClusterConfiguration.getChunkSize());
		int total_chunks_for_file = chunks.size();
		List<Integer> chunksToOverWrite = new ArrayList<Integer>();
		int number_new_chunks = 0;
		for(int i=start_chunk;i<end_chunk;i++) {
			if(i >= total_chunks_for_file) {
				number_new_chunks = end_chunk - i;
				break;
			}
			chunksToOverWrite.add(chunks.get(i));
		}
		// Get list of servers for each chunk to be overwritten and overwrite
		// For new chunks, create new chunks with MDS and write to them
		long bytesWritten = 0;
		bytesWritten += overWriteChunks(chunksToOverWrite);
		for(int i=0;i<number_new_chunks;i++) {
			// First find locations where we can create new chunk
			List<DataLocation> potentialLocations = getLocationsForNewChunk();
			int new_chunk_id = mMetadataServer.addChunkToFile(fd, client_id, potentialLocations);
			long bytes_written = 0;
			for(DataLocation location : potentialLocations) {
				long written = mDataServerMap.get(location.getDataServer()).write(new_chunk_id, mClusterConfiguration.getChunkSize());
				bytes_written += written;
			}
			// If all replicas could not be written successfully
			if(bytes_written < mClusterConfiguration.getChunkSize()*mClusterConfiguration.getNumberReplicas()) {
				// All replicas not created fully. Remove new chunk ID from cluster and delete data
				sLog.warning("FD :" + Integer.toString(fd) + " Chunk ID: " + Integer.toString(new_chunk_id)
						+ " Could not write to chunks");
				mMetadataServer.removeChunkFromFile(fd, client_id, new_chunk_id);
				for(DataLocation location : potentialLocations) {
					mDataServerMap.get(location.getDataServer()).deleteChunks(new ArrayList<>(new_chunk_id));
				}
				break;
			}
			// If all replicas were correctly written to, move on with life ..
			bytesWritten += bytes_written/mClusterConfiguration.getNumberReplicas();
		}
		// New file size is number of bytes written to new chunks
		mMetadataServer.updateFileSize(fd, bytesWritten - 
				chunksToOverWrite.size() * mClusterConfiguration.getChunkSize(), true);
		// Update client's offset
		seek(fd, offset + bytesWritten, client_id);
		return bytesWritten;
	}

	protected long overWriteChunks(List<Integer> chunkstoOverwrite) throws GenericException {
		// Implementation can be specific to Distributed File System
		// Default implementation is to overwrite chunks in the same SSDs
		long bytesWritten = 0;
		for(Integer chunk: chunkstoOverwrite) {
			List<DataLocation> locations = mMetadataServer.getDataLocations(chunk);
			for(DataLocation location : locations) {
				// Always assume that an overwrite will succeed
				sLog.info("Overwriting chunk " + Integer.toString(chunk) + " at DS " 
						+ Integer.toString(location.getDataServer()));
				bytesWritten += mDataServerMap.get(location.getDataServer()).write(chunk, mClusterConfiguration.getChunkSize());
			}
		}
		return bytesWritten/mClusterConfiguration.getmNumReplicas();
	}

	protected List<DataLocation> getLocationsForNewChunk() throws GenericException {
		// Implementation can be specific to Distributed File System
		return null;
	}

	public long seek(int fd, long offset, int client_id) {
		return mMetadataServer.seekFile(fd, offset, client_id);
	}

	public boolean delete(int fd) {
		// First delete metadata from MDS and get list of data chunks to delete for each
		// DS
		Map<Integer, List<Integer>> chunksToDelete = mMetadataServer.deleteFile(fd);
		if (chunksToDelete == null) {
			// Possibly file does not exist
			return false;
		}
		for (Integer ds : chunksToDelete.keySet()) {
			mDataServerMap.get(ds).deleteChunks(chunksToDelete.get(ds));
		}
		return true;
	}

	public FileAttribute stat(int fd) {
		return mMetadataServer.getFileAttributes(fd);
	}
	
	public void printStats() {
		for(Integer dataserver : mDataServerMap.keySet()) {
			mDataServerMap.get(dataserver).printStats();
		}
	}
}
