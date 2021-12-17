#!/bin/bash

grep -oP '(?<=^ID=).+' /etc/os-release | tr -d '"' 2>/dev/null
