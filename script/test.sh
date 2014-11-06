#!/usr/bin/env sh

# We cannot run the cassandra and console tests at the same time because
# currently they both try to create the cassandra schema. Will no longer need
# to split this up once we mock the console DB properly.

p=github.com/iParadigms/walker
sudo -E $(which go) test -tags "sudo cassandra" $p/console/test -cover -coverpkg github.com/iParadigms/walker/console && \
sudo -E $(which go) test -tags "sudo cassandra" $p/dnscache $p/cmd $p/cassandra -cover && \
sudo -E $(which go) test -tags "sudo cassandra" $p/test -cover -coverpkg github.com/iParadigms/walker
