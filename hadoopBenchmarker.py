import argparse
import ConfigParser
import subprocess
import time
import os
import traceback
import json
import shutil
import csv
import shlex
import sys


FILE_FLAG_CREATE_IF_NOT_EXISTS = "a+"
LOG_DIR = "/tmp/logs"
RESULT_DIR = "/tmp/results"

EXECUTION_TIME = 'ExecutionTime'
PARAMS = 'params'
COMMAND = 'command'
HADOOP = 'hadoop'
NAME = 'Name'
SUMMARY = 'summary'
OPERATIONS = 'operations'
TYPE = 'TestType'
RECORDS = 'Records'
APP_CONFIG = 'APP_CONFIG'
JAR_CLASS = 'jarClass'

appCfg = None

def createArgumentParser():
    usage = './bin/hadoop-benchmarker.sh --config-file <configFileName>'
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('--config-file','-c' , dest='configFile', required=True)
    return parser

def loadConfiguration(configFile):
    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.optionxform = str
    config.readfp(open(configFile))
    return config

def executeCommand(process):
    log("Command:"+str(process))
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

def getTestTerasortArguments(config, testName):
    TERASORTARGS = {
    'JAR_FILE_PATH': 'jarFilePath',
    'JAR_CLASS': 'jarClass',
    'TEST_TYPE': 'testType',
    'NUMBER_FILES': 'numberFiles',
    'FILE_SIZE': 'fileSize',
    'DIR_INPUT': 'dirInput',
    'DIR_OUTPUT': 'dirOutput'
    }

    jarFilePath = ''
    jarClass = ''
    testType = ''
    numberFiles = ''
    fileSize = ''
    testProperties = ''

    options = config.options(testName)
    for option in options:
        if option == TERASORTARGS['JAR_FILE_PATH']:
            jarFilePath = config.get(testName, option)
        elif option == TERASORTARGS['JAR_CLASS']:
            jarClass = config.get(testName, option)
        elif option == TERASORTARGS['FILE_SIZE']:
            fileSize = config.get(testName, option)
        elif option == TERASORTARGS['DIR_INPUT']:
            dirInput = config.get(testName, option)
        elif option == TERASORTARGS['DIR_OUTPUT']:
            dirOutput = config.get(testName, option)

    args = []
    args.append(HADOOP)
    args.append('jar')
    args.append(jarFilePath)
    args.append(jarClass)
    args.append(fileSize)
    args.append(dirInput)
    args.append(dirOutput)

    return args

def getTestDfsioArguments(config, testName):
    DFSIOARGS = {
    'JAR_FILE_PATH': 'jarFilePath',
    'JAR_CLASS': 'jarClass',
    'TEST_TYPE': 'testType',
    'NUMBER_FILES': 'numberFiles',
    'FILE_SIZE': 'fileSize',
    'TEST_PROPERTIES': 'testProperties'
    }

    jarFilePath = ''
    jarClass = ''
    testType = ''
    numberFiles = ''
    fileSize = ''
    testProperties = ''

    options = config.options(testName)
    for option in options:
        if option == DFSIOARGS['JAR_FILE_PATH']:
            jarFilePath = config.get(testName, option)
        elif option == DFSIOARGS['JAR_CLASS']:
            jarClass = config.get(testName, option)
        elif option == DFSIOARGS['TEST_TYPE']:
            testType = config.get(testName, option)
        elif option == DFSIOARGS['NUMBER_FILES']:
            numberFiles = config.get(testName, option)
        elif option == DFSIOARGS['FILE_SIZE']:
            fileSize = config.get(testName, option)
        elif option == DFSIOARGS['TEST_PROPERTIES']:
            testProperties = config.get(testName, option)

    args = []
    args.append(HADOOP)
    args.append('jar')
    args.append(jarFilePath)
    args.append(jarClass)

    props = testProperties.split(',')

    for prop in props:
        args.append('-D')
        args.append(prop)

    args.append('-'+testType)
    args.append('-nrFiles')
    args.append(numberFiles)
    args.append('-fileSize')
    args.append(fileSize)

    return args

def constructCommand(config, testName):
    process = []
    allowedCommands = [HADOOP]
    allowedTools = ['jar']
    command = config.get(testName, COMMAND)
    if command not in allowedCommands:
        raise Exception('command: '+command + ' is not supported')
    tool = config.get(testName, 'tool')
    if tool not in allowedTools:
        raise Exception('tool: '+tool + ' is not supported')

    if command == HADOOP and config.get(testName, JAR_CLASS) == 'TestDFSIO':
        process = getTestDfsioArguments(config, testName)
    elif command == HADOOP and config.get(testName, JAR_CLASS) == 'Terasort':
        process = getTestTerasortArguments(config, testName)

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
    print message

def addResult(results, key, line):
    data = line[line.index(key):].rsplit(":",1)
    key = data[0].strip()
    value = data[1].strip()
    results[key] = value

def extractDFSIOResults(lines):
    dfsio_results = {}
    TOTAL_MBYTES_PROCESSED_KEY = "Total MBytes processed"
    THROUGHPUT_KEY = "Throughput mb/sec"
    AVERAGE_IO_KEY = "Average IO rate mb/sec"
    IO_RATE_STD_DEVIATION_KEY = "IO rate std deviation"
    TEST_EXEC_TIME_KEY = "Test exec time sec"

    for line in lines:
        if TOTAL_MBYTES_PROCESSED_KEY in line:
            addResult(dfsio_results, TOTAL_MBYTES_PROCESSED_KEY, line)
        if THROUGHPUT_KEY in line:
            addResult(dfsio_results, THROUGHPUT_KEY, line)
        if AVERAGE_IO_KEY in line:
            addResult(dfsio_results, AVERAGE_IO_KEY, line)
        if IO_RATE_STD_DEVIATION_KEY in line:
            addResult(dfsio_results, IO_RATE_STD_DEVIATION_KEY, line)
        if TEST_EXEC_TIME_KEY in line:
            addResult(dfsio_results, TEST_EXEC_TIME_KEY, line)

    return dfsio_results

def extractTerasortResults(lines):
    terasort_results = {}
    TEST_EXEC_TIME_KEY = "Total time spent by all map tasks (ms)"

    for line in lines:
        if TEST_EXEC_TIME_KEY in line:
            addResult(terasort_results, TEST_EXEC_TIME_KEY, line)

    return terasort_results

def copyHadoopConfigFiles(targetDir):
    configFiles = ['/usr/local/hadoop/etc/hadoop/core-site.xml', '/usr/local/hadoop/etc/hadoop/hdfs-site.xml']
    confDir = os.path.join(targetDir, "conf")
    createDirectories([confDir])

    for file in configFiles:
        shutil.copy(file, confDir)

def copyResultsToHdfs(testResultDir):
    command = []
    command.append('hdfs')
    command.append('dfs')
    command.append('-copyFromLocal')
    command.append('-f')
    command.append(testResultDir)
    print type(appCfg)
    command.append(appCfg.get(APP_CONFIG,'hdfs_location'))

    executeCommand(command)

def saveServerSideConfig(resultDir, rsUrl):
    outputFile = os.path.join(resultDir, 'resourceManagerConfig.xml')

    command = "curl --negotiate -o "+ outputFile + " " + rsUrl

    executeCommand(shlex.split(command))

def main():
    log('Starting Tests....')
    argParser = createArgumentParser()
    args = argParser.parse_args()
    log('configFile='+args.configFile)
    testRunConfig = loadConfiguration(args.configFile)
    global appCfg
    appCfg = loadConfiguration(os.path.join('conf','application.cfg'))
    testNames = testRunConfig.sections()

    log(testNames)

    configFileName = os.path.split(args.configFile)[1]
    testRunId = configFileName+time.strftime("%Y%m%dT%H%M%S", time.localtime())
    testResultDir = os.path.join(RESULT_DIR,testRunId)

    logFileName = os.path.join(LOG_DIR,testRunId+'.out')
    resultFileName = os.path.join(testResultDir,'result.json')

    outfile = createOpenFile(logFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)
    resultsFile = createOpenFile(resultFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)

    table_name = testRunConfig.defaults().get('table')
    command = testRunConfig.defaults().get('command')

    results = []
    for testName in testNames:
        try:
            log('running test: '+testName)
            executionTime = time.strftime("%Y-%m-%dT%H:%M:%S")

            process = constructCommand(testRunConfig, testName)
            log('following process will run' + str(process))
            result = executeCommand(process)
            log('test executed, parsing starts: ' + testName)

            outfile.write("###"+testName+"###\n")
            outfile.write(result[0])
            lines = iter(result[0].splitlines())

            testResult = {}
            extractedResults = {}
            command = testRunConfig.get(testName, COMMAND)
            if command == HADOOP and testRunConfig.get(testName, JAR_CLASS) == 'TestDFSIO':
                extractedResults = extractDFSIOResults(lines)
            elif command == HADOOP and testRunConfig.get(testName, JAR_CLASS) == 'Terasort':
                extractedResults = extractTerasortResults(lines)


            testResult.update(extractedResults)
            testResult[EXECUTION_TIME] = executionTime
            testResult[NAME] = testName
            testResult[PARAMS] = getTestRunParams(testRunConfig, testName)

            results.append(testResult)
            log('test completed:'+testName)
        except Exception as exp:
            results.append("ERROR: check log files")
            print 'Test: '+testName +' is failed ', exp
            traceback.print_exc()

    json.dump(results, resultsFile)
    outfile.close()
    resultsFile.close()

    copyHadoopConfigFiles(testResultDir)
    copyResultsToHdfs(testResultDir)

if __name__ == '__main__':
    main()
