# Example command to run
mvn exec:java -Dexec.mainClass="com.unwrittendfs.simulator.Simulation" -Dexec.args="SGD ClusterConfig.json DataServerConfiguration.json SGDWorkload.json

# Arguments
- First argument is type of workload to run - Can be SGD, HOTNCOLD, MAPREDUCE

- Second argument is configuration of distributed cluster to which the run the workload. Configurations include the type of distributed file system (GFS/Ceph) and the number of replicas.

- Third argument is a configuration file that can be used to specify configurations of each SSDs in the cluster. Features supported include garbage collection, wear-leveling, and data scrubbing. There are configurations available to tune these aspects.

- Fourth argument specifies the workload configuration file.
