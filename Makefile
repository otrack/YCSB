default: 
	mvn -P source-run -pl site.ycsb:cassandra-binding -am clean install
	mvn -DskipTests -P source-run -pl site.ycsb:swiftpaxos-binding -am install
	mvn -pl core dependency:copy-dependencies


