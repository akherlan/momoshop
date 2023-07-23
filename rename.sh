#!/bin/bash

for file in  $(ls | grep -E "(csv|json)" | grep -Ev "[0-9]{4}-[0-9]{2}-[0-9]{2}"); do
  mv $file ${file%.*}_$(date +"%FT%H:%M%z").${file##*.}
done;
