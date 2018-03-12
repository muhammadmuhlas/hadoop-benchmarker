import argparse
import ConfigParser
import subprocess
import time
import os
import traceback

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
    config.readfp(open('conf/'+configFile))
    return config

#This function extracts the argument of the command.
def getToolArguments(config, testName):
    args = []
    commandArgs =''
    options = config.options(testName)
    for option in options:
        if (option != 'tool') & (option != 'command'):
            if option == 'args':
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

#This function validates the commands and tools and raise an error, command
# and tool are not in allowed list
def constructCommand(config, testName):
    process = []
    allowedCommands = ['hbase']
    allowedTools = ['pe']
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


def main():
    print 'Starting Tests....'
    argParser = createArgumentParser()
    args = argParser.parse_args()
    print 'configFile='+args.configFile
    testRunConfig = loadConfiguration(args.configFile)

    testNames = testRunConfig.sections()

    print testNames

    FILE_FLAG_CREATE_IF_NOT_EXISTS = "a+"
    logFileName = "logs/"+args.configFile+str(time.time())+'.out'
    resultFileName = "results/"+args.configFile+str(time.time())+'_results.out'

    outfile = createOpenFile(logFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)
    summary = createOpenFile(resultFileName, FILE_FLAG_CREATE_IF_NOT_EXISTS)

    for testName in testNames:
        try:
            log('running test: '+testName)
            process = constructCommand(testRunConfig, testName)
            result = executeCommand(process)
            outfile.write("######"+testName+"############\n")
            summary.write("######"+testName+"############\n")
            summary.write(str(process))
            outfile.write(result[0])
            for line in result[0].splitlines():
                if 'Summary of timings' in line:
                    summary.write(line[line.index("Summary of timings"):])
            log('completed test: '+testName)
        except Exception as exp:
            print 'Test: '+testName +' is failed ', exp
            traceback.print_exc()


    outfile.close()
    summary.close()
if __name__ == '__main__':
    main()
