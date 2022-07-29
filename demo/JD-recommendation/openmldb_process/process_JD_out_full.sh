#!/bin/bash
if [ ! -d "data_processed" ] 
then
	mkdir data_processed
        if [ ! -d "data_processed/train" ]
	then
	        mkdir data_processed/train
        fi
        if [ ! -d "data_processed/test" ]
	then
	        mkdir data_processed/test
        fi       
	if [ ! -d "data_processed/valid" ]
	then
	        mkdir data_processed/valid
        fi
fi

number="$( find "$1"/*.csv | wc -l )"
echo "total $number files"

split1=$(( 8*number/10 ))
split2=$(( 9*number/10 ))

n=0
echo "$split1 $split2"
for f in "$1"/*.csv
do
	n=$(( n+1  ))
	if [ "$n" -lt "$split1" ]
	then
		cp "$f" data_processed/train/.
	elif [ "$n" -lt "$split2" ]
	then
	        cp "$f" data_processed/valid/.
	else 
	        cp "$f" data_processed/test/.
	fi	
	echo "processing $f ..."
done

cd data_processed/train || exit
python3 /home/gtest/demo/openmldb_process/combine_convert.py train /home/gtest/demo/openmldb_process/out/train/
cd ../.. || exit

cd data_processed/valid || exit
python3 /home/gtest/demo/openmldb_process/combine_convert.py val /home/gtest/demo/openmldb_process/out/val/
cd ../.. || exit

cd data_processed/test || exit
python3 /home/gtest/demo/openmldb_process/combine_convert.py test /home/gtest/demo/openmldb_process/out/test/
cd ../.. || exit

python3 cal_table_array_size.py

rm -rf data_processed
