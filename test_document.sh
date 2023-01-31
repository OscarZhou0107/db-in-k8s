#!/bin/sh

if [ $# -eq 0 ]
  then
    echo "Usage: ./test_document.sh <#replicas>"
    exit
fi
replicas=$1
rm test_log.txt
# 每次选一个replica的数量
pgrep "o2" | xargs kill
# 用对应replica数量的toml
cp o2versioner/conf$replicas.toml o2versioner/conf.toml
cargo build
cargo run -- --sequencer &
sleep 3
# 起对应数量的proxy
for (( proxy=0; proxy<$replicas; proxy++))
do
    cargo run $proxy --dbproxy &
    sleep 2
done
cargo run -- --scheduler 
sleep 3
# 看一下一共起来的replica数量和设置的一不一样
if [ $(ps ux | grep -c o2) -ne $((replicas + 3)) ]
then
    ps ux | grep o2 > test_log.txt
    echo $replica >> test_log.txt
    exit
fi