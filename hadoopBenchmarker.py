import argparse
import ConfigParser
import subprocess

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
    config.readfp(open('conf/'+configFile))
    return config

#This function extracts the argument of the command.
def getToolArguments(config, testName):
    args = []
    options = config.options(testName)
    for option in options:
        if (option != 'tool') & (option != 'command'):
            args.append(option)
            value = config.get(testName, option)
            if (option.startswith('--') & not value):
                continue
            args.append()
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
    p = subprocess.Popen(process)
    result = p.communicate
    print result

def main():
    print 'Starting Tests....'
    argParser = createArgumentParser()
    args = argParser.parse_args()
    print 'configFile='+args.configFile
    testRunConfig = loadConfiguration(args.configFile)

    testNames = testRunConfig.sections()
    print testNames

    for testName in testNames:
        try:
            process = constructCommand(testRunConfig, testName)
            executeCommand(process)
        except Exception as exp:
            print 'Test: '+testName +' is failed ', exp


if __name__ == '__main__':
    main()
