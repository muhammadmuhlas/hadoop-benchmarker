# hadoop-benchmarker
`hadoop-benchmarker` application helps to run a test suites using hadoop standard tools. Following tools are supported at this time.

- Hbase Performance Evaluation
- YCSB

`hadoop-benchmarker` reads test file, constructs and executes a command line for each tests and output the test results in json format.  

## Software Requirements
`hadoop-benchmarker` is a python application, hence it requires python to be installed on machine. Although it has been tested with python 2.7 but it should work with other version of python too.
As `hadoop-benchmarker` is a wrapper around standard tools. Please make sure standard tool can be run independently successfully.

## Test Definition
Tests are configured in INI formatted file [https://en.wikipedia.org/wiki/INI_file]. Each test file can contain test(s) which gets executed. Please see tool specific section to write test definitions.

## Test Executions
Once test configuration file is created, the tests can be invoked by executing `bin/hadoop-benchmarker.sh -c <path to test configuration file>`

## Test Output
`hadoop-benchmarker` produced both `logs` and `results` for each execution run. By default, both `logs` and `results` are created in `/tmp` location. In `/tmp/results` directory, a new folder is created for each run which contains `result.json` file and hadoop configurations.  

## Output Schema
`hadoop-benchmarker` application parses the console output of standards tools, hence the json structure is different for different type of tests. There are some common sections e.g. `params` section contains the parameter supplied in test configuration file for each test.

## Hbase Performance Evaluation Tool
Hbase provides several tools to admin, analyze or monitor your cluster. `hbase pe` is part of builtin tool which helps to measure the performance of the cluster. Please run `hbase pe` in your cluster or visit http://hbase.apache.org/book.html#__code_hbase_pe_code to find more about hbase performance evaluation test.

### PE Test Configuration
To define PE test, all `hbase pe` option can be used. Please make sure, you define attribute as it is required by `hbase pe`. For example if `hbase pe` requires attributes should be prefixed by `--`, please prefix attribute with `--` in configuration file. See some of the configuration file in conf directory as a reference.

In addition to `hbase pe` parameters, following attributes must be defined in test file.
- `command` for PE tests, the value of this attribute will be `hbase`
- `tool` the value of this attribute will be `pe`
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
