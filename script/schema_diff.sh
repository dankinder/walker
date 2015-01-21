#!/usr/bin/env bash

## set the shell to die for any command that returns non-zero
set -e 

## Usage info
show_help() {
cat << EOF
Usage: schema_diff.sh [-hk] <HOST> <PORT>
Description:
    Produce a diff of schema in THIS walker repo, and a running cassandra node. Note that the local schema
    is on the left in the diff, and the cassandra node schema is on the right.
Diagnostic Notes:
    * Must run out of the main walker directory.
    * cqlsh must be in your current path.
Example:
    schema_diff 127.0.0.1 9042
Arguments:
    HOST: Mandatory. Specify the host that the running cassandra node is on
    PORT: Mandatory. Specify the port that the running cassandra node is on
Options:
    -h: Display this help and exit
    -k: Write two schema files into this directory. One file is the schema for the local repo, called local.txt. And the
        second file stores the schema for the live cassandra source: it is called live.txt. 
EOF
}

## echo help and error message to stderr
die(){
    show_help >&2
    echo "ERROR:" >&2
    echo $1 >&2
    exit 1 
}


## Set up file names: could be overridden by -k
LEFT_FILE=/tmp/walker_schema_diff.$$.1.txt
RIGHT_FILE=/tmp/walker_schema_diff.$$.2.txt
DELETE_FILES=1

## Have at the options
while getopts "hk" opt; do
    case "$opt" in
        h)  show_help
            exit 0
            ;;
        k)  LEFT_FILE='local.txt'
            RIGHT_FILE='live.txt'
            DELETE_FILES=0
            ;;
        '?')
            die "Unknown option $OPTARG"
            ;;
    esac
done
shift $(($OPTIND - 1)) # set so $1==host and $2==port

## Make sure the arguments where passed in
if [ -z "$1" ]
then
    die "Failed to specify HOST and PORT"
fi
HOST=$1

if [ -z "$2" ]
then
    die "Failed to specify PORT"
fi
PORT=$2

## Let's execute
go run walker/main.go schema -o $LEFT_FILE
cqlsh -k walker -e 'describe schema' $HOST $PORT > $RIGHT_FILE
diff $LEFT_FILE $RIGHT_FILE

if [ $DELETE_FILES -eq "1" ]
then
rm -f $LEFT_FILE $RIGHT_FILE
fi
