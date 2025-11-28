#!/bin/bash
ps -e -o pid,user,%mem,size,cmd --sort=%mem | grep kafka-http | awk '{$4=int($4/1024)"M";}{ print;}'
