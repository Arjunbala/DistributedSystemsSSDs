package com.unwrittendfs.simulator;

import com.unwrittendfs.simulator.dfs.ClusterConfiguration;
import com.unwrittendfs.simulator.utils.ConfigUtils;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class Simulation {
	
	private static int sSimulatorTime = 0;
	private static JSONParser jsonParser = new JSONParser();
	
	public static void main(String args[]) throws IOException {

		Simulation simulation = new Simulation();
		File file = simulation.getFileFromResources("ClusterConfig.json");
		ClusterConfiguration clusterConfiguration = ConfigUtils.getClusterConfig(file);
		return;
	}

	private File getFileFromResources(String fileName) {

		ClassLoader classLoader = getClass().getClassLoader();
		URL resource = classLoader.getResource(fileName);
		if (resource == null) {
			throw new IllegalArgumentException("file is not found!");
		} else {
			return new File(resource.getFile());
		}

	}
	
	public static int getSimulatorTime() {
		return sSimulatorTime;
	}
}