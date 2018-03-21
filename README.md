# hadoop-benchmarker
`hadoop-benchmarker` helps to run standard benchmarking tool i.e. Performance Evaluation (pe).

## Steps To Add Performance Evaluation (pe) Tests
- create a configuration file in conf directory. The file should follow INI format [https://en.wikipedia.org/wiki/INI_file]
- Each section in configuration file represents one test.
- In each test, the first entry must be `tool: pe`
- The last two options must be type of test and number of clients respectively. i.e. `randomWrite`, `randomRead` etc.

## YCSB tests
YCSB tests are supported. See the `ycsb_sample.cfg` file to create other test runs.
