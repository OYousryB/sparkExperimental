#!/usr/bin/env bash
#--Generate TPCH Dataset In csv--#

size=${1:-1}
files=${2:-1}

#--dataset generation--#
./dbgen -f -s $size

declare -a arr=("customer" "part" "orders" "lineitem" "partsupp" "supplier")
for i in "${arr[@]}"
do
  mkdir -p tbl_csv/$size/$i

  #--Copy Files--#
  cp "$i.tbl" "tbl_csv/$size/$i/init_$i.csv"
  cp "$i.tbl" "tbl_csv/$size/$i/"

  #--Remvoe Last Char in each line--#
  sed -i 's/.$//' "tbl_csv/$size/$i/init_$i.csv"

  #--Seperate Files--#
  lines=`wc -l < "tbl_csv/$size/$i/init_$i.csv"`
  per_file=$((lines / files))

  for ((x=1;x<=$files;x++))
  do
    head -n $((per_file * x)) "tbl_csv/$size/$i/init_$i.csv" | tail -n +$((per_file * (x - 1)+1)) > "tbl_csv/$size/$i/$x.csv"
  done
  rm "tbl_csv/$size/$i/init_$i.csv"
done

declare -a arr=( "nation" "region")
for i in "${arr[@]}"
do
  mkdir -p tbl_csv/$size/$i

  #--Copy Files--#
  cp "$i.tbl" "tbl_csv/$size/$i/$i.csv"
  cp "$i.tbl" "tbl_csv/$size/$i/"

  sed -i 's/.$//' "tbl_csv/$size/$i/$i.csv"
done