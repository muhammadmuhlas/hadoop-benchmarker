import argparse
import ConfigParser
import subprocess
import time
import os
import traceback
import json
import shutil
import csv

# constants
FILE_FLAG_CREATE_IF_NOT_EXISTS = "a+"
LOG_DIR = "/tmp/logs"
RESULT_DIR = "/tmp/results"

EXECUTION_TIME = 'ExecutionTime'
PARAMS = 'params'
COMMAND = 'command'
YCSB = 'ycsb'
HBASE = 'hbase'
NAME = 'Name'
SUMMARY = 'summary'
OPERATIONS = 'operations'
UPDATE = 'update'
INSERT = 'insert'
READ = 'read'
TYPE = 'TestType'
RECORDS = 'Records'





#create a parser which evaluate command line arguments.
def createArgumentParser():
    """ A utility method to make sure correct argument are passed to hadoop benchmarker programe.
    Args:
        None

    Returns:
       parser :ArgumentParser

    """
    usage = './bin/hadoop-benchmarker.sh --config-file <configFileName>'
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('--config-file','-c' , dest='configFile', required=True)
    return parser

# configuration for tests are loaded. The configuration files are expected to be
# in conf directory
def loadConfiguration(configFile):
    """ This function reads given config file and constructs a ConfigParser obj.
    Args:
       configFile : config file path
    Returns:
        config: ConfigParser obj
    """
    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.optionxform = str
    config.readfp(open(configFile))
    return config

def executeCommand(process):
    """This function runs the command. The command details is given as process.

    Args:
        process list : a list of options to run the command.

    Returns:
            result tuple: a tuple which contains the console output. All the output is redirected to stdout for simplicity
    """
    p = subprocess.Popen(process, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = p.communicate()
    return result

def getTestRunParams(config, testName):
    """ This function scans configuration file for a test and creates a dict from
        its parameters.
    Args:
         config : a ConfigParser object
         testName : a string which represents a section in config file.
    Returns:
         params: a dictionary which contains all the parameters for a given test run.
    """
    params = {}
    options = config.options(testName)
    for option in options:
        value = config.get(testName, option)
        params[option] = value

    return params
#This function extracts the argument of the command.
def getPeArguments(config, testName):
    """reads configuration for PE test and construct an array of all parameters
    Args:
       config: ConfigParser
       testName: string
    Returns:
         args: dict
    """
    args = []
    commandArgs =''
    peTestType = ''
    options = config.options(testName)
    for option in options:
        if (option != 'tool') & (option != 'command'):
            if option == 'nClients':
                commandArgs = config.get(testName, option)
                continue
            if option == 'TestType':
                peTestType = config.get(testName, option)
                continue
            value = config.get(testName, option)
            if not value:
                args.append(option)
                continue
            args.append(option+"="+value)

    if peTestType:
        args.append(peTestType)

    if commandArgs:
        args.append(commandArgs)
    return args

# extract YCSB arguments
def getYCSBArguments(config, testName):
    """This reads `config` for given `testName` and construct a ycsb command to
    execute.
    Args:
         config obj: obj contains the configuration for this run.
         testName (str): The section name in config file.
    Returns:
            a list consists of all the parts of `ycsb` command
    """

    YCSBARGS = {
    'YCSB_HOME' : 'ycsb_home',
    'OPERATION' : 'tool',
    'DBTYPE' : 'dbType',
    'WORKLOAD' : 'workload',
    'TABLE' : 'table',
    'COLUMNFAMILY' : 'columnfamily',
    'PRINCIPAL' : 'principal',
    'KEYTAB' : 'keytab',
    'THREADS' : 'threads',
    'TARGET' : 'target',
    'JVM-ARGS' : 'jvm-args'
    }

    ycsb_home = ''
    operation = ''
    dbType = ''
    workload = ''
    table = ''
    columnfamily = ''
    principal = ''
    keytab = ''
    thread = ''
    target = ''
    jvm_args = ''
    other_args = ''

    options = config.options(testName)
    for option in options:
        if option == YCSBARGS['YCSB_HOME']:
            ycsb_home = config.get(testName, option)
        elif option == YCSBARGS['OPERATION']:
            operation = config.get(testName, option)
        elif option == YCSBARGS['DBTYPE']:
            dbType = config.get(testName, option)
        elif option == YCSBARGS['WORKLOAD']:
            workload = config.get(testName, option)
        elif option == YCSBARGS['TABLE']:
            table = config.get(testName, option)
        elif option == YCSBARGS['COLUMNFAMILY']:
            columnfamily = config.get(testName, option)
        elif option == YCSBARGS['PRINCIPAL']:
            principal = config.get(testName, option)
        elif option == YCSBARGS['KEYTAB']:
            keytab = config.get(testName, option)
        elif option == YCSBARGS['THREADS']:
            thread = config.get(testName, option)
        elif option == YCSBARGS['TARGET']:
            target = config.get(testName, option)
        elif option == YCSBARGS['JVM-ARGS']:
            jvm_args = config.get(testName, option)
        elif option == 'command':
            continue
        else:
            value = config.get(testName, option)
            arg = option
            if value:
                arg = option +'='+ str(value)
            if other_args:
                other_args += ','+arg
            else:
                other_args = arg
# validating arguments
    if not ycsb_home:
        raise Exception(YCSBARGS['YCSB_HOME'] + " is required")
    if not operation:
        raise Exception(YCSBARGS['OPERATION'] + " is required")
    if not dbType:
        raise Exception(YCSBARGS['DBTYPE'] + " is required")
    if not workload:
        raise Exception(YCSBARGS['WORKLOAD'] + " is required")

# construct ycsb command
    ycsb_command = os.path.join(ycsb_home,'bin/ycsb')

    hbase_classpath = executeCommand(['hbase','classpath'])

    args = []
    args.append(ycsb_command)
    args.append(operation)
    args.append(dbType)
    args.append('-P')
    args.append(workload)
    args.append('-cp')
    args.append(hbase_classpath[0].strip())

    if table:
        args.append('-p')
        args.append('table='+table)
    if columnfamily:
        args.append('-p')
        args.append('columnfamily='+columnfamily)
    if principal:
        args.append('-p')
        args.append('principal='+principal)
    if keytab:
        args.append('-p')
        args.append('keytab='+keytab)
    if thread:
        args.append('-threads')
        args.append(thread)
    if target:
        args.append('-target')
        args.append(target)

    if other_args:
        for ar in other_args.split(','):
            args.append('-p')
            args.append(ar)

    return args



#This function validates the commands and tools and raise an error, command
# and tool are not in allowed list
def constructCommand(config, testName):
    """base function to construct test command/process parameters. It delegates
    the work to tool specific method.
    Args:
      config: ConfigParser
      testName: string

    Returns:
      process : list

    """
    process = []
    allowedCommands = ['hbase', 'ycsb']
    allowedTools = ['pe', 'load', 'run']
    command = config.get(testName, 'command')
    if command not in allowedCommands:
        raise Exception('command: '+command + ' is not supported')
    tool = config.get(testName, 'tool')
    if tool not in allowedTools:
        raise Exception('tool: '+tool + ' is not supported')

    if (command == 'hbase') and (tool == 'pe'):
        process.append(command)
        process.append(tool)
        process.extend(getPeArguments(config, testName))
    elif command == 'ycsb':
        process = getYCSBArguments(config, testName)

    return process


def createDirectories(dirs):
    """utility method to create directories if directories don't exist
    Args:
       dirs: list of directories to be created.
    Returns:
       None
    """
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

def createOpenFile(filepath, fileOp):
    """Open a file for given file operation e.g. to read, to write or to append
     Args:
        filepath: string path
        fileop: file open operator flag w, r, a

     Returns:
       a file object
    """
    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))
    return open(filepath, fileOp)

def log(message):
    """Prints message to console.
    Args:
        message (str): message to print
    Returns:
          None
    """
    print message

# def recordPeLatency(lines, summaryFile):
#     searchKeys = ['Min', 'Avg', 'StdDev', '50th', '75th', '95th', '99th', '99.9th', '99.99th', '99.999th', 'Max']
#     latency = {}
#     for key in searchKeys:
#         line = next(lines)
#         summaryFile.write(line[line.index(key):]+'\n')
#         latency[key] = line[line.index(key):]
#     return latency

def extractPEResults(lines):
    """It scans through the console output of `hbase pe` tool and extracts the outcome
    of the the run.

    Args:
        lines: list of console output lines

    Returns:
           dict: It returns dictionary of dictionaries. Each sub dictionary contains
           a section from the `hbase pe` output.
    """

    pe_results = {}
    SUMMARY_KEY = "Summary of timings"
    summary_results = {}
    latency_results = []
    for line in lines:
        if SUMMARY_KEY in line:
            data = line[line.index(SUMMARY_KEY):].split(":")
            key = data[0].strip()
            values = data[1].strip()[1:-1].split(',')
            summary_results[key] = values

    if not summary_results:
        summary_results['ERROR'] = "No results found, please see log files"

    pe_results['summary'] = summary_results
    pe_results['latencies'] = latency_results

    return pe_results


def extractYSCBResults(lines):
    """It scans through the console output of YSCB tool and extracts the outcome
    of the the run.

    Args:
        lines: list of console output lines

    Returns:
           dict: It returns dictionary of dictionaries. Each sub dictionary contains
           a section from the ycsb output.
    """

    ycsb_results = {}
    OVERALL_KEY = '[OVERALL]'
    CLEANUP_KEY = '[CLEANUP]'
    INSERT_KEY = '[INSERT]'
    READ_KEY = '[READ]'
    UPDATE_KEY = '[UPDATE]'

    summary_results = {}
    cleanup_results = {}
    insert_results = {}
    read_results = {}
    update_results = {}
    splittedLine = []
    for line in lines:
        if OVERALL_KEY in line:
            splittedLine = line.split(',')
            summary_results[splittedLine[1].strip()] = splittedLine[2].strip()
        elif CLEANUP_KEY in line:
            splittedLine = line.split(',')
            cleanup_results[splittedLine[1].strip()] = splittedLine[2].strip()
        elif INSERT_KEY in line:
            splittedLine = line.split(',')
            insert_results[splittedLine[1].strip()] = splittedLine[2].strip()
        elif READ_KEY in line:
            splittedLine = line.split(',')
            read_results[splittedLine[1].strip()] = splittedLine[2].strip()
        elif UPDATE_KEY in line:
            splittedLine = line.split(',')
            update_results[splittedLine[1].strip()] = splittedLine[2].strip()

    if not summary_results:
        summary_results["ERROR"] = "No results found, please see log files"

    ycsb_results[SUMMARY] = summary_results

    operations = {}
    operations[READ] = read_results
    operations['cleanup'] = cleanup_results
    operations[INSERT] = insert_results
    operations[UPDATE] = update_results
    ycsb_results[OPERATIONS] = operations

    return ycsb_results

def copyHadoopConfigFiles(targetDir):
    """ copies hadoop configuration file to result directory so configuration can be captured when tests were executed.

    Args:
        targetDir the folder where configuration files will be copied. `conf` subdirectoy is created for configuration files.

    Returns: None
    """
    configFiles = ['/etc/hadoop/conf/core-site.xml', '/etc/hadoop/conf/hdfs-site.xml', '/etc/hbase/conf/hbase-site.xml']
    confDir = os.path.join(targetDir, "conf")
    createDirectories([confDir])

    for file in configFiles:
        shutil.copy(file, confDir)

def getPERow(TestResult):
    """
    Args:
    Returns:
    """
    AverageRuntime = "AverageRuntime(ms)"
    paramRows = "--rows"

    row = {EXECUTION_TIME: "", TYPE: "", RECORDS: "", AverageRuntime: ""}

    if EXECUTION_TIME in TestResult.keys():
        row[EXECUTION_TIME] = TestResult[EXECUTION_TIME]

    if SUMMARY in TestResult.keys():
        str_timings = TestResult[SUMMARY]['Summary of timings (ms)']
        int_timinigs = map(lambda x: int(x), str_timings)
        sum = reduce(lambda x,y: x+y, int_timinigs)

        average = sum/float(len(int_timinigs))
        row[AverageRuntime] = str(average)

    if PARAMS in TestResult.keys():
        row[TYPE] = TestResult[PARAMS][TYPE]
        if paramRows in TestResult[PARAMS].keys():
            row[RECORDS] = TestResult[PARAMS][paramRows]
        else:
            row[RECORDS] = "1000000"

    return row

def getYCSBRow(TestResult):
    """

    Args:

    Returns:

    """
    RUNTIME = 'RunTime(ms)'
    THROUGHPUT = 'Throughput(ops/sec)'
    READOPSCOUNT = 'ReadOpsCount'
    READOPS_AVERAGELATENCY = 'ReadOps_AverageLatency(us)'
    INSERTOPSCOUNT = 'InsertOpsCount'
    INSERTOPS_AVERAGELATENCY = 'InsertOps_AverageLatency(us)'
    UPDATEOPSCOUNT = 'UpdateOpsCount'
    UPDATEOPS_AVERAGELATENCY = 'UpdateOps_AverageLatency(us)'

    # Initialize row
    row = {EXECUTION_TIME : '', TYPE : '', RECORDS : '', RUNTIME : '', THROUGHPUT : '',
           READOPSCOUNT : '', READOPS_AVERAGELATENCY :'',
           INSERTOPSCOUNT : '', INSERTOPS_AVERAGELATENCY : '',
           UPDATEOPSCOUNT : '', UPDATEOPS_AVERAGELATENCY : ''}

    if PARAMS in TestResult.keys():
        row[RECORDS] = TestResult[PARAMS]['recordcount']
        row[TYPE] = TestResult[PARAMS]['tool']

    if EXECUTION_TIME in TestResult.keys():
        row[EXECUTION_TIME] = TestResult[EXECUTION_TIME]

    if SUMMARY in TestResult.keys():
        row[RUNTIME] = TestResult[SUMMARY][RUNTIME]
        row[THROUGHPUT] = TestResult[SUMMARY][THROUGHPUT]

    if OPERATIONS in TestResult.keys():
        if TestResult[OPERATIONS][READ]:
            row[READOPSCOUNT] = TestResult[OPERATIONS][READ]['Operations']
            row[READOPS_AVERAGELATENCY] = TestResult[OPERATIONS][READ]['AverageLatency(us)']

        if TestResult[OPERATIONS][INSERT]:
            row[INSERTOPSCOUNT] = TestResult[OPERATIONS][INSERT]['Operations']
            row[INSERTOPS_AVERAGELATENCY] = TestResult[OPERATIONS][INSERT]['AverageLatency(us)']

        if TestResult[OPERATIONS][UPDATE]:
            row[UPDATEOPSCOUNT] = TestResult[OPERATIONS][UPDATE]['Operations']
            row[UPDATEOPS_AVERAGELATENCY] = TestResult[OPERATIONS][UPDATE]['AverageLatency(us)']

    return row

def createTabularSummary(resultFilepath):
    """ It reads `resultFile` and extract subset of output fields to create a summary of results in csv format and save
    result in results directory.

    Args:
        `resultFilepath` json file which contains the result of tests.

    Returns: None
    """
    if not os.path.exists(resultFilepath):
        log(resultFilepath +' not exists, please see log files to debug')
        return

    resultDir = os.path.dirname(resultFilepath)
    ycsbSummaryFile = os.path.join(resultDir, "ycsbSummary.csv")
    peSummaryFile = os.path.join(resultDir, "peSummary.csv")

    ycsbHeader = ['Name', 'ExecutionTime', 'TestType', 'Records', 'RunTime(ms)', 'Throughput(ops/sec)',
                  'ReadOpsCount', 'ReadOps_AverageLatency(us)', 'InsertOpsCount','InsertOps_AverageLatency(us)',
                  'UpdateOpsCount', 'UpdateOps_AverageLatency(us)']

    peHeader = [NAME, EXECUTION_TIME, TYPE, RECORDS,'AverageRuntime(ms)']

    with open(resultFilepath) as source:
        with open(ycsbSummaryFile, FILE_FLAG_CREATE_IF_NOT_EXISTS) as ycsbFile, open(peSummaryFile, FILE_FLAG_CREATE_IF_NOT_EXISTS) as peFile:
            ycsbWriter = csv.DictWriter(ycsbFile, delimiter=',', lineterminator='\n',fieldnames=ycsbHeader)
            peWriter = csv.DictWriter(peFile, delimiter=',', lineterminator='\n',fieldnames=peHeader)

            #writing headers
            ycsbWriter.writeheader()
            peWriter.writeheader()

            testResults = json.load(source)

            for testResult in testResults:
                row = {}
                row[NAME] = testResult[NAME]

                if YCSB == testResult[PARAMS][COMMAND]:
                    row.update(getYCSBRow(testResult))
                    ycsbWriter.writerow(row)
                elif HBASE == testResult[PARAMS][COMMAND]:
                    row.update(getPERow(testResult))
                    peWriter.writerow(row)
                else:
                    log(data[testName][PARAMS][COMMAND] + ' is not supported')


# Main runner
def main():
    log('Starting Tests....')
    argParser = createArgumentParser()
    args = argParser.parse_args()
    log('configFile='+args.configFile)
    testRunConfig = loadConfiguration(args.configFile)

    testNames = testRunConfig.sections()

    log(testNames)

    configFileName = os.path.split(args.configFile)[1]
    testRunId = configFileName+str(time.time())
    testResultDir = os.path.join(RESULT_DIR,testRunId)

    logFileName = os.path.join(LOG_DIR,testRunId+'.out')
    resultFileName = os.path.join(testResultDir,'result.json')

    outfile = createOpenFile(logFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)
    resultsFile = createOpenFile(resultFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)

    results = []
    for testName in testNames:
        try:
            log('running test: '+testName)
            executionTime = time.strftime("%Y-%m-%d %H:%M:%S")

            process = constructCommand(testRunConfig, testName)
            log('following process will run' + str(process))
            result = executeCommand(process)
            log('test executed, parsing starts: ' + testName)

            outfile.write("###"+testName+"###\n")
            outfile.write(result[0])
            lines = iter(result[0].splitlines())

# Extracting results from output
            testResult = {}
            extractedResults = {}
            if testRunConfig.get(testName, COMMAND) == YCSB:
                extractedResults = extractYSCBResults(lines)
            elif testRunConfig.get(testName, 'tool') == 'pe':
                extractedResults = extractPEResults(lines)

            testResult.update(extractedResults)
            testResult[EXECUTION_TIME] = executionTime
            testResult[NAME] = testName
            testResult[PARAMS] = getTestRunParams(testRunConfig, testName)

            results.append(testResult)
            log('test completed:'+testName)
        except Exception as exp:
            results[testName] = "ERROR: check log files"
            print 'Test: '+testName +' is failed ', exp
            traceback.print_exc()

# dump result of run in as json
    json.dump(results, resultsFile)
    outfile.close()
    resultsFile.close()

    copyHadoopConfigFiles(testResultDir)
    createTabularSummary(resultFileName)

if __name__ == '__main__':
    main()
