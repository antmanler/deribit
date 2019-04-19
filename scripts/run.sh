#!/bin/bash

run() {
    ./deribit --v 4  >./logs/log.$(($(date +%s%N)/1000000)).txt 2>&1
}

while true; do
    if run; then
        echo "Finished!"
        exit 0
    fi
done
