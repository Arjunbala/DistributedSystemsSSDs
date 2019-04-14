package com.unwrittendfs.simulator.dfs;

import com.unwrittendfs.simulator.dataserver.DataserverConfiguration;

import java.util.List;

public class CephFileSystem  extends DistributedFileSystem  {

    protected CephFileSystem(ClusterConfiguration config, List<DataserverConfiguration> dataserverConfigurations) {
        super(config, dataserverConfigurations);
    }
}
