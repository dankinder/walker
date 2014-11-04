#!/usr/bin/env sh
## This code will write coverage html files. The files will end up as test/coverage.{out,html} and console/test/coverage.{out,html}
VERBOSE=""
while [ "$1" != "" ]; do
    case $1 in
        -c | --clean )           
                                rm -f {test,console/test,dnscache,cmd,cassandra}/coverage.{out,html}
                                exit 0
                                ;;
        -v | --verbose )        VERBOSE="-v"
                                ;;
        * )                     echo "Unknown option $1"
                                exit 1
    esac
    shift
done

go test $VERBOSE -tags "sudo cassandra" github.com/iParadigms/walker/console/test -cover -coverpkg github.com/iParadigms/walker/console -coverprofile=console/test/coverage.out && \
go tool cover -html=console/test/coverage.out -o console/test/coverage.html && \
sudo -E go test $VERBOSE -tags "sudo cassandra" github.com/iParadigms/walker/test -cover -coverpkg github.com/iParadigms/walker -coverprofile=test/coverage.out && \
sudo -E go tool cover -html=test/coverage.out -o test/coverage.html && \
go test $VERBOSE -tags "sudo cassandra" github.com/iParadigms/walker/dnscache -cover -coverprofile=dnscache/coverage.out && \
go tool cover -html=dnscache/coverage.out -o dnscache/coverage.html && \
go test $VERBOSE -tags "sudo cassandra" github.com/iParadigms/walker/cmd -cover -coverprofile=cmd/coverage.out && \
go tool cover -html=cmd/coverage.out -o cmd/coverage.html && \
go test $VERBOSE -tags "sudo cassandra" github.com/iParadigms/walker/cassandra -cover -coverprofile=cassandra/coverage.out && \
go tool cover -html=cassandra/coverage.out -o cassandra/coverage.html
