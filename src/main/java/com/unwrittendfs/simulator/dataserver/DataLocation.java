package com.unwrittendfs.simulator.dataserver;

public class DataLocation {
	public enum DataRole {
		PRIMARY_REPLICA,
		SECONDARY_REPLICA
	};
	private int mDataServerId;
	private DataRole mRole;
	
	public DataLocation(int id, DataRole role) {
		mDataServerId = id;
		mRole = role;
	}
	
	public int getDataServer() {
		return mDataServerId;
	}
	
	public DataRole getRole() {
		return mRole;
	}
	
	public void updateRole(DataRole newRole) {
		mRole = newRole;
	}

	@Override
	public String toString() {
		return "DataLocation{" +
				"mDataServerId=" + mDataServerId +
				", mRole=" + mRole +
				'}';
	}
}