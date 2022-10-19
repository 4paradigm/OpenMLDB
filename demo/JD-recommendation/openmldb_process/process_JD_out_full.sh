#!/bin/bash
mkdir -p data_processed/train
mkdir -p data_processed/test
mkdir -p data_processed/valid

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
python3 ../../combine_convert.py train ../../out/train/
cd ../.. || exit

cd data_processed/valid || exit
python3 ../../combine_convert.py val ../../out/val/
cd ../.. || exit

cd data_processed/test || exit
python3 ../../combine_convert.py test ../../out/test/
cd ../.. || exit

python3 cal_table_array_size.py ./out/


rm -rf data_processed
