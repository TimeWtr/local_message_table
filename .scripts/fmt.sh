#!/bin/bash

# shellcheck disable=SC2034
PROJECT_PREFIX="github.com/TimeWtr"

#shellcheck disable=SC2034
for item in $(find . -type f -name "*.go" -not -path "./.idea/");
do
  goimports -l -w "$item";
done