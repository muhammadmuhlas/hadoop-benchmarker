# hadoop-benchmarker
`hadoop-benchmarker` application helps to run a test suites using hadoop standard tools. Following tools are supported at this time.

- Hbase Performance Evaluation
- YCSB

`hadoop-benchmarker` reads test file, constructs and executes a command line for each tests and output the test results in json format.  

## Software Requirements
`hadoop-benchmarker` is a python application, hence it requires python to be installed on machine. Although it has been tested with python 2.7 but it should work with other version of python too.
As `hadoop-benchmarker` is a wrapper around standard tools. Please make sure standard tool can be run independently successfully.

## Installation
Download `hadoop-benchmarker` to a directory from where you want to execute your tests. e.g. `/opt`

## Test Definition
Tests are configured in INI formatted file [https://en.wikipedia.org/wiki/INI_file]. Each test file can contain test(s) which gets executed. Please see tool specific section to write test definitions.

## Test Executions
Once test configuration file is created, the tests can be invoked by executing `bin/hadoop-benchmarker.sh -c <path to test configuration file>`

## Test Output
`hadoop-benchmarker` produced both `logs` and `results` for each execution run. By default, both `logs` and `results` are created in `/tmp` location. In `/tmp/results` directory, a new folder is created for each run which contains `result.json` file and hadoop configurations.  

## Output Schema
`hadoop-benchmarker` application parses the console output of standards tools, hence the json structure is different for different type of tests. There are some common sections e.g. `params` section contains the parameter supplied in test configuration file for each test.

## Kerberos cluster
In Kerberos clusters, please create kerberos ticket using `kinit` before running `hadoop-benchmarker`


## Hbase Performance Evaluation Tool
Hbase provides several tools to admin, analyze or monitor your cluster. `hbase pe` is part of builtin tool which helps to measure the performance of the cluster. Please run `hbase pe` in your cluster or visit http://hbase.apache.org/book.html#__code_hbase_pe_code to find more about hbase performance evaluation test.

### PE Test Configuration
To define PE test, all `hbase pe` option can be used. Please make sure, you define attribute as it is required by `hbase pe`. For example if `hbase pe` requires attributes should be prefixed by `--`, please prefix attribute with `--` in configuration file. See some of the configuration file in conf directory as a reference.

In addition to `hbase pe` parameters, following attributes must be defined in test file.
- `command` for PE tests, the value of this attribute will be `hbase`
- `tool` the value of this attribute will be `pe`
- `--nomapred` is required as map reduce execution is not supported yet
- `TestType` This attribute configures the test which `hbase pe` will execute. To find supported values, run `hbase pe` without any parameter. Some of the examples are `randomWrite`, `randomRead` etc.
- `nClients` This attributes show number of threads participating in `hbase pe` execution.

## YCSB
YCSB [https://github.com/brianfrankcooper/YCSB] is widely used database benchmarking tool and it support Hbase as well.

### Installation
To setup YCSB on your system, please follow instructions at https://github.com/brianfrankcooper/YCSB/wiki/Getting-Started

### YCSB Test Configuration
To define `YCSB` tests, all YCSB Properties [https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties] can be used.
In addition to YCSB properties, following attributes are required for ycsb tests.

- `ycsb_home` path where ycsb is installed.
- `command` the value must be `ycsb`
- `tool`, the value must be `load` or `run`
- `dbType` for hbase test, `hbase10` should be used.
- `table` , the table which will be used for the test. Please note, ycsb doesn't create table and this table should be created prior to running the test.

## TestDFSIO
The TestDFSIO is widely used to benchmark HDFS component of Hadoop. It helps to find bottlenecks in network, hardware, OS and even in hadoop configurations. TestDFSIO starts a map reduce jobs to write and read data in hdfs. `write` test needs to be performed before `read` test can be executed.

### Installation
`TestDFSIO` is part of hadoop distribution. e.g. in Hortonworks distribution the jar file containing this test is located at `/us/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient-tests.jar`. No extra steps are required to use this tool.

### TestDFSIO Test Configuration
When configuring TestDFSIO tests, following attributes are supported.

- `command` - the value is `hadoop`
- `tool` - the value is `jar`
- `jarFilePath` - the location of jar file containing `TestDFSIO` tool.
- `jarClass` - the value is `TestDFSIO`
- `numberFiles` - number of files, `TestDFSIO` will write or read.
- `fileSize` - the size of each file written/read by `TestDFSIO`
- `testProperties` - comma separated properties which are supplied to `TestDFSIO` program as jvm params. e.g. hadoop wide configuration can be defined to override the default values in hadoop configuration files.
- `testType` - test which will be executed. e.g. `write` or `read`

### Sample Configuration
A sample configuration file `sample_testdfsio.cfg` is provided in `conf` directory of the project.

## NNBench
`NNBench` is useful in benchmarking namenode. It makes lot of requests to namenode with a very small payload. It can issue write, read, delete, rename operations.

### Installation
`nnbench` is part of hadoop distribution. e.g. in Hortonworks distribution the jar file containing this test is located at `/us/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-client-jobclient-tests.jar`. No extra steps are required to use this tool.

### nnbench Test Configuration

When configuring `nnbench` tests, following attributes are supported.

- `command` - the value is `hadoop`
- `tool` - the value is `jar`
- `jarFilePath` - the location of jar file containing `nnbench` tool.
- `jarClass` - the value is `nnbench`
- `operation` - the operation `nnbench` will perform. `create_write`, `open_read`, `delete`, `rename` are valid values.
- `maps` - number of maps
- `reduces` - number of reducers
- `startTime` - time to start, given in seconds from the epoch.
- `blockSize` - Block size in bytes. 
- `bytesToWrite` - Bytes to write.
- `bytesPerChecksum` - Bytes per checksum for the files.
- `numberOfFiles` - number of files to create.
- `replicationFactorPerFile` - Replication factor for the files.
- `baseDir` - base DFS path. default is /becnhmarks/NNBench.
- `readFileAfterOpen` - true or false. if true, it reads the file and reports the average time to read. This is valid with the open_read operation.

### Sample Configurations
A sample configuration `sample_nnbench.cfg` is provided in `conf` directory of this project.


    
