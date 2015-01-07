#!/usr/bin/env sh

# -p 1 ensures that multiple test binaries from different subpackages don't run
# in parallel. We need this because multiple packages test with the local
# cassandra instance and can conflict.
sudo -E $(which go) test -p 1 -tags "sudo cassandra" -cover github.com/iParadigms/walker/...
