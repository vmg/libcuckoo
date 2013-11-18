bench: benchmark_cuckoo benchmark_mutex

benchmark_cuckoo:
	gcc -O3 -std=gnu99 src/test_cuckoo_mt.c lib/cuckoohash.c lib/city.c -Ilib -lpthread -o bench1

benchmark_mutex:
	gcc -DOPENHASH=1 -O3 -std=gnu99 src/test_cuckoo_mt.c lib/cuckoohash.c lib/city.c -Ilib -lpthread -o bench2
