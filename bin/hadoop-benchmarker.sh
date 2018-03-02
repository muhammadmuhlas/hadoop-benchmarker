#!/bin/bash

BINPATH=`dirname $0`
python "$BINPATH/../hadoop-benchmarker.py" $@
