# ptee
Hadoop Performance Testing & Measurement Suite - Business Level Workload Emulation, while remaining Close to the Metal - Scala Based

Enables the generation of Actionable (Measurement) Information supporting Informed Capacity Planning as well as Predictive Models for Performance Scaling and Cluster Sizing  

Get additional, Data Science Focused, performance testing workloads from https://github.com/evoisec/ptp 

Among the Performance Testing Workloads, this repo also contains innovative algorithm / method for RAPID Finding and Deleting of individual records or small groups of records from large Parquet (Impala) Tables/Files - those who know Parquet, Impala and Spark, know that such operation is NOT supported out of the box by any of these Hadoop Engines and Hadoop file format. This algo/job, besides for performance testing, can also be REUSED in ETL jobs implementing a broad range of well known Bsuiness Scenarios in a need for such functionality.  
this is the algo in question:
https://github.com/evoisec/ptee/blob/master/src/main/scala/PII.scala

Apache JMETER is used for Test Scenario Workflow Automation - this is a JMETER template which can orchestrate Spark Jobs, Map  Reduce Jobs, Impala and Hive JDBC Queries (the JDBC drivers and docs are in the jdbc-drivers dir of this repo)
https://github.com/evoisec/ptee/blob/master/JMETER-DSP-Perf-Testing-Workflow-Automation-Template.jmx

There is a lack of modern, business level tools for workload performance testing on Hadoop. Carried on by inertia, the majority 
of the IT industry is still using archaic, outadted "benchmarks" the majority of which are writtent in Map Reduce 
(not used almost at all in real projects) and implementing "toy" algorithms / data processing like e.g. "sorting" (as in "TerraSort") 

The purpose of this suite is to fill the gap by augmenting (ie provides some innovations) and complementing more modern performance measurement suites (however not maintained on a regular basis / not up to date) such as:

https://github.com/Intel-bigdata/HiBench

https://github.com/palantir/spark-tpcds-benchmark 
