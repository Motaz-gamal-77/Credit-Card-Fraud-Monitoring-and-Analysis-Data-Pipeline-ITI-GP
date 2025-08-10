#!/bin/bash

PID_FILE="pipeline.pids"

if [ -f "$PID_FILE" ]; then
    while read PID; do
        echo "Stopping process with PID $PID"
        kill "$PID" 2>/dev/null || echo "Process $PID already stopped."
    done < "$PID_FILE"
    rm "$PID_FILE"
    echo "All streaming jobs stopped."
else
    echo "No PID file found. Are the processes running?"
fi
