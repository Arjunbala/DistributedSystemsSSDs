package com.unwrittendfs.simulator.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unwrittendfs.simulator.client.workload.DownpourSGD;
import com.unwrittendfs.simulator.client.workload.HotNCold;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.dfs.ClusterConfiguration;
import com.unwrittendfs.simulator.exceptions.GenericException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ConfigUtils {
	private static ObjectMapper objectMapper = new ObjectMapper();

	public static ClusterConfiguration getClusterConfig(File jsonString) throws IOException {
		ClusterConfiguration config = new ClusterConfiguration();
		try {
			config = objectMapper.readValue(jsonString, ClusterConfiguration.class);
			return config;
		} catch (IOException e) {
			System.err.println("Failed to initialize the ClusterConfiguration");
			e.printStackTrace();
			throw e;
		}

	}

	public static List<DataserverConfiguration> getDataServers(File jsonString) throws IOException {
		try {
			List<DataserverConfiguration> dataserverConfigurations = new ArrayList<>();
			dataserverConfigurations =  objectMapper.readValue(jsonString, new TypeReference<List<DataserverConfiguration>>(){});
			for(DataserverConfiguration dataServer : dataserverConfigurations){
				dataServer.setmRand(new Random(dataServer.getmRandomSeed()));
			}
			return dataserverConfigurations;
		} catch (IOException e) {
			System.err.println("Failed to initialize the DataServerConfiguration");
			e.printStackTrace();
			throw e;
		}
	}

	public static DownpourSGD getSGDWorkloadConfig() throws GenericException {
		try {
			File file = new File("resources/SGDWorkload.json");
			return objectMapper.readValue(file, DownpourSGD.class);
		} catch (IOException ex){
			throw new GenericException("Failed to read SGDWorkload properties", ex);
		}
	}

	public static HotNCold getHotNColdWorkloadConfig() throws GenericException {
		try {
			File file = new File("resources/HotNCold.json");
			return objectMapper.readValue(file, HotNCold.class);
		} catch (IOException ex) {
			throw new GenericException("Failed to read HotNCold Workload properties", ex);
		}
	}
}