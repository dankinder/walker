#!/usr/bin/env bash

# Change directory to the root of this walker repository
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# -p 1 ensures that multiple test binaries from different subpackages don't run
# in parallel. We need this because multiple packages test with the local
# cassandra instance and can conflict.
sudo -E $(which go) test -p 1 -tags "sudo cassandra" -cover ./...
