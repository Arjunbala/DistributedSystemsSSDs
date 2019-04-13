package com.unwrittendfs.simulator.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unwrittendfs.simulator.client.ClientWorkload;
import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;
import com.unwrittendfs.simulator.dfs.ClusterConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ConfigUtils {
	private static ObjectMapper objectMapper = new ObjectMapper();

	public static ClusterConfiguration getClusterConfig(String jsonString) throws IOException {
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

	public static List<DataserverConfiguration> getDataServers(String jsonString) throws IOException {
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

	public static List<ClientWorkload> getClientWorkload(String jsonContent) throws IOException {
		try{
			List<ClientWorkload> clientWorkloads = new ArrayList<>();
			clientWorkloads = objectMapper.readValue(jsonContent, new TypeReference<List<DataserverConfiguration>>(){});
			return clientWorkloads;
		} catch (IOException e){
			System.err.println("Failed to initialize the ClientWorkload");
			e.printStackTrace();
			throw e;
		}
	}
}