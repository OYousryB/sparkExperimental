#!/bin/bash

source scripts/env.txt

genTable()
{
  local sf=$1
  local table=$2

  echo ./dbgen -vf -s $sf -T $table
  ./dbgen -vf -s $sf -T $table &> /dev/null &
}

genTablePara()
{
  local sf=$1
  local table=$2
  local chunks=$3

  for i in $(seq 1 $chunks); do
    genTableChunk $sf $table $chunks $i
  done
}

genTableChunk()
{
  local sf=$1
  local table=$2
  local chunks=$3
  local current=$4

  echo ./dbgen -vf -s $sf -C $chunks -S $current -T $table
  ./dbgen -vf -s $sf -C $chunks -S $current -T $table &> /dev/null &
}

for SF in $SCALES
do
  OUTPUT=$TBL_DIR
  # Clean working directory ............................
  mkdir -p $OUTPUT

  TBL_DIR=$OUTPUT

  if [[ ! -d $TBL_DIR ]]
  then
    cd tpch

    rm -rf $TBL_DIR
    mkdir -p $TBL_DIR

    start=$SECONDS

    echo "Generating CSV chunks"

    #serial generation
    genTable $SF l    #nation/region

    #parallel generation by chunks

    genTablePara $SF o $TBL_PARALLELISM    #orders/lineitem
    genTablePara $SF p $TBL_PARALLELISM    #part/partsupp
    genTablePara $SF c $TBL_PARALLELISM    #customer
    genTablePara $SF s $TBL_PARALLELISM    #supplier
    wait

    # move to target directories
    mkdir $TBL_DIR/nation
    mv nation.tbl* $TBL_DIR/nation

    mkdir $TBL_DIR/region
    mv region.tbl* $TBL_DIR/region

    mkdir $TBL_DIR/lineitem
    mv lineitem.tbl* $TBL_DIR/lineitem

    mkdir $TBL_DIR/orders
    mv orders.tbl* $TBL_DIR/orders

    mkdir $TBL_DIR/supplier
    mv supplier.tbl* $TBL_DIR/supplier

    mkdir $TBL_DIR/part
    mv part.tbl* $TBL_DIR/part

    mkdir $TBL_DIR/partsupp
    mv partsupp.tbl* $TBL_DIR/partsupp

    mkdir $TBL_DIR/customer
    mv customer.tbl* $TBL_DIR/customer

    end=$SECONDS
    echo Elapsed time $(($end - $start)) seconds.

    cd ../
  fi
done