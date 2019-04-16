package com.unwrittendfs.simulator.client.workload;

import com.unwrittendfs.simulator.Simulation;
import com.unwrittendfs.simulator.dfs.DistributedFileSystem;

public class TestWorkload implements IClientWorkload {
	
	private DistributedFileSystem mDfs;
	
	public TestWorkload(DistributedFileSystem dfs) {
		mDfs = dfs;
	}

	@Override
	public void execute() {
		int fd = mDfs.create("abc", 1);
		System.out.println(fd);
		String buffer = null;
		long written = mDfs.write(fd, buffer, 2048*1024, 1);
		Simulation.incrementSimulatorTime();
		System.out.println(written);
		long offset = mDfs.seek(fd, 0, 1);
		Simulation.incrementSimulatorTime();
		mDfs.write(fd, buffer, 2048*1024, 1);
		Simulation.incrementSimulatorTime();
		long read = mDfs.read(fd, buffer, 1024, 1);
		Simulation.incrementSimulatorTime();
		System.out.println(read);
		read = mDfs.read(fd, buffer, 1024, 1);
		Simulation.incrementSimulatorTime();
		System.out.println(read);
		mDfs.delete(fd);
		Simulation.incrementSimulatorTime();
		read = mDfs.read(fd, buffer, 1024, 1);
		Simulation.incrementSimulatorTime();
		System.out.println(read);
	}
}