mkdir -p output
mpicxx map_reduce.cpp -o map_reduce
mpirun -np 8 --oversubscribe map_reduce -p input