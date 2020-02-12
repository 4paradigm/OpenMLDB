EXPECTED_ARGS=1


if [ $# -ne $EXPECTED_ARGS ]
then
       source_file=$1
else
      source_file="ir_test.c"
fi;

echo $source_file
ir_file=$source_file".ll"
clang -Os -S -emit-llvm $source_file -o $ir_file
cat $ir_file
rm $ir_file