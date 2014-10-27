#!/usr/bin/env sh
## This code will write coverage html files. The files will end up as test/coverage.{out,html} and console/test/coverage.{out,html}
go test github.com/iParadigms/walker/console/test -cover -coverpkg github.com/iParadigms/walker/console -coverprofile=console/test/coverage.out && \
go tool cover -html=console/test/coverage.out -o console/test/coverage.html && \
sudo -E go test -tags "sudo cassandra" github.com/iParadigms/walker/test -cover -coverpkg github.com/iParadigms/walker -coverprofile=test/coverage.out && \
go tool cover -html=test/coverage.out -o test/coverage.html

