#!/bin/bash

# NOTE: MUST BE RUN FROM CEPH MON HOST AS NON-ROOT

use_cached_data=false
pg_state=degraded
tmpdata=
while (($# > 0))
do
    case "$1" in
        --use-cached)
            use_cached_data=true
            ;;
        --data-dir)
            tmpdata=$2
            shift
            ;;
        --pg-state)
            pg_state=$2
            shift
            ;;
    esac
    shift
done

if $use_cached_data && [ -z "$tmpdata" ]; then
echo "ERROR: --use-cached requires --data-dir <path to cached data>"
exit 1
fi

if ! $use_cached_data; then
# Pools
readarray -t POOLS<<<"`sudo ceph osd pool ls`"
echo "Analysing pools: ${POOLS[@]}"

# Images
declare -A pool_rbd_images=()
f(){ pool_rbd_images[$1]="`sudo rbd -p $1 ls`"; }
msg='echo "Retrieved rbd images from pool '"'"'$p'"'"'"'
i=${#POOLS[@]};:(){ ((i>0)) && { p=${POOLS[$((--i))]} && f $p && eval $msg; :; } };:

# Objects
declare -A rbd_image_objs=()
declare -A rbd_image_pool=()
f5(){ sudo rados -p $1 ls| grep $2; }
msg='echo "Retrieving objects for rbd image '"'"'$2'"'"'" 1>&2'
f4(){ pfx=`sudo rbd -p $1 info $2| sed -r 's/\sblock_name_prefix\:\s+rbd_data\.(.+)/\1/g;t;d;'`; eval $msg && f5 $1 $pfx;  }
f3(){ timeout 10 sudo rbd -p $1 info $2 &>/dev/null && f4 $@; }
f2(){ IFS=' '; readarray -t imgs<<<`echo $2`; for img in ${imgs[@]}; do rbd_image_pool[$img]=$1; rbd_image_objs[$img]=`f3 $1 $img`; done; }
f1(){ pool=${POOLS[$1]}; [ -z "${pool_rbd_images[$pool]}" ] || f2 $pool "${pool_rbd_images[$pool]}"; }
i=${#POOLS[@]};:(){ ((i>0)) && { ((i--)) && f1 $i; :; } };:

# PGs
declare -A pool_rbd_image_obj_pg=()
echo "Fetching PGs for RBD image objects - this will take a while"
f3(){ sudo ceph osd map $1 $2| sed -r 's/.+pg\s+([\.0-9a-z]+)\s+\(([\.0-9]+)\).+/\2/g;t;d'; }
msg='echo "Retrieving PG for object '"'"'$obj'"'"'" 1>&2'
f2(){ IFS=' '; readarray -t objs<<<`echo ${rbd_image_objs[$2]}`; for obj in ${objs[@]}; do eval $msg && pg=`f3 $1 $obj`; pool_rbd_image_obj_pg[$obj]=$pg; done; }
f1(){ IFS=' '; readarray -t imgs<<<`echo ${pool_rbd_images[$1]}`; for img in ${imgs[@]}; do f2 $1 $img; done; }
i=${#POOLS[@]};:(){ ((i>0)) && { ((i--)) && f1 ${POOLS[$i]}; :; } };:

[ -n "$tmpdata" ] || tmpdata=`mktemp -d`
[ -d "$tmpdata" ] || mkdir $tmpdata
rm -f $tmpdata/*.objs
rm -f $tmpdata/*.pool
rm -f $tmpdata/pg

for img in ${!rbd_image_objs[@]}; do echo "${rbd_image_objs[$img]}" >> $tmpdata/$img.objs; done
for img in ${!rbd_image_pool[@]}; do echo "${rbd_image_pool[$img]}" >> $tmpdata/$img.pool; done
for obj in ${!pool_rbd_image_obj_pg[@]}; do echo "${pool_rbd_image_obj_pg[$obj]} $obj" >> $tmpdata/pg; done
fi

# $pg_state PGs
readarray -t degraded<<<"`sudo ceph pg dump 2>&1| grep $pg_state| awk '{print $1}'| egrep "^1"`"

echo "Data dir: $tmpdata"

# List objects in degraded PGs
objs=()
for pg in ${degraded[@]}; do
    readarray -t o<<<$(egrep "^$pg " $tmpdata/pg| awk '{print $2}')
    objs+=( ${o[@]} )
done
imgs=()
# List images from whom those objects came
for o in ${objs[@]}; do
    imgs+=( `egrep -l "^$o" $tmpdata/*| xargs basename| sed -r 's/\..+//'` )
done

echo -e "\nRBD images with objects in PGs that have '$pg_state' state:"
for img in ${imgs[@]}; do echo "$img (pool=`cat $tmpdata/$img.pool`)"; done| sort| uniq

