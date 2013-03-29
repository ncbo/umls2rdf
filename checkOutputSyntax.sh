#!/bin/bash
#===============================================================================
#
#          FILE:  checkOutputSyntax.sh
# 
#         USAGE:  ./checkOutputSyntax.sh 
# 
#   DESCRIPTION:  Apply rapper to check the syntax of turtle files created
#                   by the umls2rdf.py conversion.
# 
#       OPTIONS:  --- First argument is output path (default = 'output')
#  REQUIREMENTS:  ---
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  Darren L. Weber, Ph.D. (), darren.weber@stanford.edu
#       COMPANY:  Stanford University
#       VERSION:  1.0
#       CREATED:  03/29/2013 11:28:18 AM PDT
#      REVISION:  ---
#===============================================================================

outputPath='output'
if [ "$1" != "" ]; then
    outputPath="$1"
fi

if which rapper >/dev/null 2>&1; then
    for ttlFile in ${outputPath}/*.ttl; do
        echo
        rapper -i turtle -c --show-graphs --show-namespaces $ttlFile
    done
    echo
fi

