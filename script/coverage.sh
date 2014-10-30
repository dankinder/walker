#!/usr/bin/env sh
## This code will write coverage html files. The files will end up as test/coverage.{out,html} and console/test/coverage.{out,html}
VERBOSE=""
while [ "$1" != "" ]; do
    case $1 in
        -c | --clean )           
                                rm -f ./console/test/coverage.out ./console/test/coverage.html ./test/coverage.out ./test/coverage.html
                                exit 0
                                ;;
        -v | --verbose )        VERBOSE="-v"
                                ;;
        * )                     echo "Unknown option $1"
                                exit 1
    esac
    shift
done
go test $VERBOSE github.com/iParadigms/walker/console/test -cover -coverpkg github.com/iParadigms/walker/console -coverprofile=console/test/coverage.out && \
go tool cover -html=console/test/coverage.out -o console/test/coverage.html && \
sudo -E go test $VERBOSE -tags "sudo cassandra" github.com/iParadigms/walker/test -cover -coverpkg github.com/iParadigms/walker -coverprofile=test/coverage.out && \
go tool cover -html=test/coverage.out -o test/coverage.html

