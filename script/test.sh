#!/usr/bin/env sh
sudo -E $(which go) test -tags "sudo cassandra" github.com/iParadigms/walker/test -cover -coverpkg github.com/iParadigms/walker
