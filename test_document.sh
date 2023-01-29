#!/bin/sh

rm test_log.txt
# 每次选一个replica的数量
for replicas in 8
do 
    # 选client的数量
    for clients in 500
    do
        # 选workload
        for mix in 1
        do 
            # kill上一次的process
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
            sleep 5
            # python3 ./load_generator/launcher.py --port 2077 --mix $mix --range 0 $clients --ip 127.0.0.1 --mock_db --path ./load_generator
            sleep 1
            # { echo perf; echo break; } | netcat localhost 9999
        done
    done
done