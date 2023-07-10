#!/bin/bash

for file in  $(ls | grep "json" | grep -Ev "[0-9]{4}-[0-9]{2}-[0-9]{2}");
do
  mv $file ${file%.*}_$(date +"%FT%H:%M%z").json;
done;
