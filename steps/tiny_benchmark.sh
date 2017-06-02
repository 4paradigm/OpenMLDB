#! /bin/sh
#
# tiny_benchmark.sh
WORK_DIR=`pwd`
unset HEAPCHECK
echo "start to do benchmark without tcmalloc"
sh $WORK_DIR/steps/benchmark.sh

cd $WORK_DIR/build && cmake .. -DTCMALLOC_ENABLE=ON && make -j8
echo "start to do benchmark with tcmalloc"
cd $WORK_DIR
sh $WORK_DIR/steps/benchmark.sh

