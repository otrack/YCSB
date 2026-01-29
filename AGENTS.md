# YCSB Project Quick Reference

- Language: Java
- Build tool: Maven (multi-module; root pom.xml)

Quick local test using the in-memory database (BasicDB):
- Build: `mvn -q -DskipTests package`
- Load: `bin/ycsb.sh load basic -P workloads/workloada`
- Run:  `bin/ycsb.sh run basic  -P workloads/workloada`

Notes:
- The default in-memory database is BasicDB (aka `site.ycsb.BasicDB.java`) in the core module.
- BasicDB is suitable for functional testing (e.g., trying a new workload) without an external database.
