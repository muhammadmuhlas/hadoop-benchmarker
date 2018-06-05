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

#create a parser which evaluate command line arguments.
def createArgumentParser():
    """ A utility method to make sure correct argument are passed to hadoop benchmarker programe.
    Args:
        None

    Returns:
       parser :ArgumentParser

    """
    usage = './bin/hadoop-benchmarker.sh --schemaFile <schemaFile> --datajsonFile <datajsonFile> '
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('--schemaFile','-s' , dest='schemaFile', required=True)
    parser.add_argument('--datajsonFile','-d' , dest='datajsonFile', required=True)
    return parser

def getValue(record, columnKey):
    columnKeySplit = columnKey.split('.')

    obj = record
    for key in columnKeySplit:
        obj = obj.get(key)
    return obj

def getRecordRow(record, columnNames, columnKeys):
    row = {}
    for i in range(len(columnNames)):
        columnName = columnNames[i]
        columnKey = columnKeys[i]
        value = getValue(record, columnKey)
        row[columnName] = value
    return row


def main():
    argParser = createArgumentParser()
    args = argParser.parse_args()
    headers = []
    schema = []
    schemaFile = open(args.schemaFile, 'r')
    columnNames = schemaFile.readline().strip().split(',')
    print columnNames
    columnKeys = schemaFile.readline().strip().split(',')
    print columnKeys
    schemaFile.close()
    outputFile = os.path.split(args.datajsonFile)[1]+'.csv'
    with open(outputFile, 'a') as result, open(args.datajsonFile, 'r') as jsonFile:
        csvWriter = csv.DictWriter(result, delimiter=',', lineterminator='\n',fieldnames=columnNames)
        csvWriter.writeheader()
        jsonData = json.load(jsonFile)

        for record in jsonData:
            row = getRecordRow(record, columnNames, columnKeys)
            csvWriter.writerow(row)


if __name__ == '__main__':
    main()
