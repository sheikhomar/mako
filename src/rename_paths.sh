#!/bin/bash

# list=$'/user/s1962523/openintel-alexa1m-compact/part-01145.gz\n/user/s1962523/openintel-alexa1m-compact/part-03146.gz\n/user/s1962523/openintel-alexa1m-compact/part-04153.gz'

# Find all paths that have not yet been renamed
list="$(hadoop fs -ls /user/s1962523/openintel-alexa1m-compact | grep '/part-' | sed '1d;s/  */ /g' | cut -d\  -f8)"

# Loop through them
while read -r oldpath; do

    # Get first line of the file
    first="$(hadoop fs -cat $oldpath | zcat | head -1)"

    # Extract the date from the first line
    date=$(echo "$first" | egrep -o '"date": "[0-9]*' | egrep -o '[0-9]*')

    # Generate new path
    newpath=$(echo $oldpath | sed -En "s/(.*)(part.*)/\1$(echo $date)-\2/p")

    # Perform the rename
    hadoop fs -mv $oldpath $newpath
done <<< "$list"