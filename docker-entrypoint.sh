#!/bin/bash 

if [ "$1" = "test" ]; then
    if [ "$2" = "--unit" ]; then
        printf "\nrunning unit tests...\n"
        nosetests --where=tests/unit
    elif [ "$2" = "--integration" ]; then
        printf "\nrunning integration tests...\n"
        nosetests --where=tests/integration
    else
        printf "\nrunning all tests...\n"
        nosetests --where=tests/unit && \
        nosetests --where=tests/integration
    fi
fi

if [ "$1" = "lint" ] || [ "$3" = "lint" ]; then
      printf "\nlinting...\n"
      pylint target_snowflake -d C,W,unexpected-keyword-arg,duplicate-code
fi