#!/bin/bash

MAX_THREADS=12
OUTPUT_FILE="icx_simple_output.txt"
echo "Threads,MinTime(s),FuncCalls,Integrand" > $OUTPUT_FILE

for ((threads=1; threads<=MAX_THREADS; threads++)); do
    export OMP_NUM_THREADS=$threads
    echo "Running with $OMP_NUM_THREADS threads..."

    times=()
    for run in {1..3}; do
        echo "  Run #$run"
        output=$(./simple.exe 0.0 510.0 250000000)
        time=$(echo "$output" | grep "WALL CLOCK Time" | awk '{print $4}')
        calls=$(echo "$output" | grep "FUNC CALL COUNT" | awk '{print $4}')
        integrand=$(echo "$output" | grep "integral =" | awk '{print $9}')
        times+=($time)

        if [[ $run -eq 1 ]]; then
            func_calls=$calls
            integrand_value=$integrand
        fi
         
    done

    min_time=${times[0]}
    for t in "${times[@]}"; do
        min_time=$(echo "$min_time $t" | awk '{if ($2 < $1) print $2; else print $1}')
    done

    echo "$threads,$min_time, $func_calls, $integrand_value" >> $OUTPUT_FILE
done

echo "Results saved to $OUTPUT_FILE"
echo "All runs completed."