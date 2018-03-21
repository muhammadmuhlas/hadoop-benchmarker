import argparse
import ConfigParser
import subprocess
import time
import os
import traceback
import json

#create a parser which evaluate command line arguments.
def createArgumentParser():
    usage = './bin/hadoop-benchmarker.sh --config-file <configFileName>'
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('--config-file','-c' , dest='configFile', required=True)
    return parser

# configuration for tests are loaded. The configuration files are expected to be
# in conf directory
def loadConfiguration(configFile):
    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.optionxform = str
    config.readfp(open(configFile))
    return config

def executeCommand(process):
    p = subprocess.Popen(process, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = p.communicate()
    return result

def getTestRunParams(config, testName):
    params = {}
    options = config.options(testName)
    for option in options:
        value = config.get(testName, option)
        params[option] = value

    return params
#This function extracts the argument of the command.
def getPeArguments(config, testName):
    args = []
    commandArgs =''
    options = config.options(testName)
    for option in options:
        if (option != 'tool') & (option != 'command'):
            if option == 'nClients':
                commandArgs = config.get(testName, option)
                continue
            value = config.get(testName, option)
            if not value:
                args.append(option)
                continue
            args.append(option+"="+value)

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

    print process
    return process


def createDirectories(dirs):
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

def createOpenFile(filepath, fileOp):
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
            values = data[0].strip()[1:-1].split(',')
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

    ycsb_results['summary'] = summary_results

    operations = {}
    operations['read'] = read_results
    operations['cleanup'] = cleanup_results
    operations['insert'] = insert_results
    operations['update'] = update_results
    ycsb_results['operations'] = operations

    return ycsb_results

# Main runner
def main():
    log('Starting Tests....')
    argParser = createArgumentParser()
    args = argParser.parse_args()
    log('configFile='+args.configFile)
    testRunConfig = loadConfiguration(args.configFile)

    testNames = testRunConfig.sections()

    log(testNames)

    FILE_FLAG_CREATE_IF_NOT_EXISTS = "a+"
    LOG_DIR = "logs"
    RESULT_DIR = "results"
    fileName = os.path.split(args.configFile)[1]
    logFileName = os.path.join(LOG_DIR,fileName+str(time.time())+'.out')
    resultFileName = os.path.join(RESULT_DIR,fileName+str(time.time())+'.json')

    outfile = createOpenFile(logFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)
    resultsFile = createOpenFile(resultFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)

    results = {}
    for testName in testNames:
        try:
            log('running test: '+testName)
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
            if testRunConfig.get(testName, 'command') == 'ycsb':
                extractedResults = extractYSCBResults(lines)
            elif testRunConfig.get(testName, 'tool') == 'pe':
                extractedResults = extractPEResults(lines)

            testResult.update(extractedResults)
            testResult['params'] = getTestRunParams(testRunConfig, testName)

            results[testName] = testResult
            log('test completed:'+testName)
        except Exception as exp:
            results[testName] = "ERROR: check log files"
            print 'Test: '+testName +' is failed ', exp
            traceback.print_exc()

# dump result of run in as json
    json.dump(results, resultsFile)
    outfile.close()
    resultsFile.close()

if __name__ == '__main__':
    main()
