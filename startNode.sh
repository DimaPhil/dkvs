#!/usr/bin/env bash

javac -cp .:lib/* src/ru/ifmo/ctddev/filippov/dkvs/*.java src/ru/ifmo/ctddev/filippov/dkvs/messages/*.java
java -cp .:lib/*:src/* ru.ifmo.ctddev.filippov.dkvs.Node $1

