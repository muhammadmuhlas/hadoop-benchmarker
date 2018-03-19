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

#This function extracts the argument of the command.
def getToolArguments(config, testName):
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

def getYCSBArguments(config, testName):
    args = []
    ycsb_home = ''
    operation = ''
    dbType = ''
    workload = ''
    table = ''
    columnfamily = ''
    principal = ''
    keytab = ''
    thread =
    target =
    jvm_args = ''
    other_args = ''

    options = config.options(testName)
    for option in options:
        if option == 'ycsb_home':
            ycsb_home = config.get(testName, option)
        elif option == 'operation':
            operation = config.get(testName, option)
        elif option == 'dbType':
            dbType = config.get(testName, option)
        elif option == 'workload':
            workload = config.get(testName, option)
        elif option == 'table':
            table = config.get(testName, option)
        elif option == 'columnfamily':
            columnfamily = config.get(testName, option)
        elif option == 'principal':
            principal = config.get(testName, option)
        elif option == 'keytab':
            principal = config.get(testName, option)
        elif option == 'thread':
            thread = config.get(testName, option)
        elif option = 'target':
            target = config.get(testName, option)
        elif option = 'jvm-args':
            jvm_args = config.get(testName, option)
        else:
            value = config.get(testName, option)
            arg = option
            if value:
                arg = option +'='+ str(value)
            if other_args:
                other_args += ','+arg
            else:
                other_args = arg



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

    process.append(command)
    process.append(tool)
    process.extend(getToolArguments(config, testName))
    print process
    return process

def executeCommand(process):
    p = subprocess.Popen(process, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = p.communicate()
    return result

def createDirectories(dirs):
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)

def createOpenFile(filepath, fileOp):
    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))
    return open(filepath, fileOp)

def log(message):
    print message

def recordPeLatency(lines, summaryFile):
    searchKeys = ['Min', 'Avg', 'StdDev', '50th', '75th', '95th', '99th', '99.9th', '99.99th', '99.999th', 'Max']
    latency = {}
    for key in searchKeys:
        line = next(lines)
        summaryFile.write(line[line.index(key):]+'\n')
        latency[key] = line[line.index(key):]
    return latency

def main():
    print 'Starting Tests....'
    argParser = createArgumentParser()
    args = argParser.parse_args()
    print 'configFile='+args.configFile
    testRunConfig = loadConfiguration(args.configFile)

    testNames = testRunConfig.sections()

    print testNames

    FILE_FLAG_CREATE_IF_NOT_EXISTS = "a+"
    LOG_DIR = "logs"
    RESULT_DIR = "results"
    fileName = os.path.split(args.configFile)[1]
    logFileName = os.path.join(LOG_DIR,fileName+str(time.time())+'.out')
    resultFileName = os.path.join(RESULT_DIR,fileName+str(time.time())+'_results')

    outfile = createOpenFile(logFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)
    summary = createOpenFile(resultFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)

    results = {}
    for testName in testNames:
        try:
            log('running test: '+testName)
            process = constructCommand(testRunConfig, testName)
            result = executeCommand(process)
            outfile.write("###"+testName+"###\n")
            summary.write("###"+testName+"###\n")
            params = str(process)
            summary.write(str(process)+'\n')
            outfile.write(result[0])
            lines = iter(result[0].splitlines())
            latencies = []
            clientsRunTimes = ''
            for line in lines:
                if 'latency log' in line:
                    summary.write("#Latency Log#\n");
                    latencies.append(recordPeLatency(lines, summary))
                    continue
                if 'Summary of timings' in line:
                    summary.write(line[line.index("Summary of timings"):]+'\n')
                    clientsRunTimes = line[line.index("Summary of timings"):]
            log('completed test: '+testName)
            testResult = {}
            testResult['params'] = process
            testResult['latencies'] = latencies
            if not clientsRunTimes:
                clientsRunTimes = ['Run Times not found, please see log files for debugging']

            testResult['summary'] = clientsRunTimes

            results[testName] = testResult
        except Exception as exp:
            results[testName] = "ERROR: check log files"
            print 'Test: '+testName +' is failed ', exp
            traceback.print_exc()

    with open('result/results.json', 'w+') as resultfile:
        json.dump(results, resultfile)
    outfile.close()
    summary.close()
if __name__ == '__main__':
    main()
