#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input file is :"
echo "$1"

source .venv/bin/activate
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -archives .venv.tar.gz#.venv \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "bash -c 'source .venv/bin/activate && python mapper1.py'" \
  -reducer "bash -c 'source .venv/bin/activate && python reducer1.py'" \
  -input "$1" \
  -output /tmp/index

# This will compute average dl, since it cannot be paralleled
python app.py

hdfs dfs -ls /
