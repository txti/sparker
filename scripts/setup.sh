export SPARK_HOME=~/.local/share/spark

spark_ver=3.5.0
base_url=https://archive.apache.org/dist/spark
spark_dir=spark-$spark_ver-bin-hadoop3
spark_file=$spark_dir.tgz

# Download
wget $base_url/spark-$spark_ver/$spark_file -O ./$spark_file

rm -fr $SPARK_HOME

tar -xzf $spark_file -C ~/.local/share

mv ~/.local/share/$spark_dir $SPARK_HOME

rm $spark_file
