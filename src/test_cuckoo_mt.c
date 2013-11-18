#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include <unistd.h>           /* for sleep */
#include <sys/time.h>        /* for gettimeofday */

#include "cuckoohash.h"
#include "hash_open_prot.h"

#define million 1000000

/* TODO(awreece) Get this from automake. */
#define CACHE_LINE_SIZE 64

#ifdef OPENHASH

oh_size_t hash_key(const char *key)
{
    return CityHash32(key, strlen(key));
}

int cmp_key(const char *a, const char *b)
{
	return strcmp(a, b) == 0;
}

DEFINE_OPENHASH(kv, const char*, void*, 1, hash_key, cmp_key);

static openhash_t(kv) *table;
pthread_rwlock_t lock;

#else

static cuckoo_hashtable_t* table = NULL;

#endif

static size_t power = 22;
static size_t total =  5 * million;
static volatile size_t total_inserted;

typedef struct {
    volatile size_t num_read;
    volatile size_t num_written;
    volatile size_t ops;
    volatile size_t failures;
    volatile size_t misses;
    int id;
} thread_arg_t;

typedef union {
  thread_arg_t arg;
  char padding[CACHE_LINE_SIZE];
} __attribute__((aligned (CACHE_LINE_SIZE))) padded_thread_arg_t;

static size_t value_count;
static cuckoo_header *values;

static void random_key(char *key, size_t len)
{
	static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

	size_t i;

	for (i = 0; i < len; ++i) {
		key[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
	}

	key[i] = 0;
}

static void task_init(size_t total)
{
	size_t i;

	values = malloc(total * sizeof(cuckoo_header));
	value_count = total;

	for (i = 0; i < total; ++i) {
		size_t len = 16 + rand() % 32;
		values[i].key = malloc(len + 1);
		random_key(values[i].key, len);
	}
}

static void *lookup_thread(void *arg)
{
    thread_arg_t* th = (thread_arg_t*) arg;
    th->ops         = 0;
    th->failures    = 0;
    th->num_read    = 0;
    th->num_written = 0;
	th->misses = 0;
    
    while (1) {
		void *result = NULL;
        size_t i;

		if (!total_inserted)
			continue;
		
		i = rand() % total_inserted;
        th->ops++;

#ifdef OPENHASH
		pthread_rwlock_rdlock(&lock);
		oh_iter_t it = openhash_get(kv, table, values[i].key);
		if (it < openhash_end(table))
			result = openhash_value(table, it);
		pthread_rwlock_unlock(&lock);
#else
        result = cuckoo_find(table, values[i].key);
#endif
        
        if (result == NULL) {
            th->misses++;
            continue;
        }

        if (result != &values[i]) {
            printf(
				"[reader%d] wrong value for key %zu from table\n", th->id, i);
            th->failures ++;
            continue;
        }

        th->num_read++;
    }
    
    pthread_exit(NULL);
}

static void *insert_thread(void *arg)
{
    int st;
	size_t i;

    thread_arg_t* th = (thread_arg_t*) arg;
    th->ops         = 0;
    th->failures    = 0;
    th->num_read    = 0;
    th->num_written = 0;
    
	for (i = 0; i < value_count; ++i) {
		th->ops ++;

#ifdef OPENHASH
		pthread_rwlock_wrlock(&lock);
		oh_iter_t it = openhash_set(kv, table, values[i].key, &st);
		openhash_value(table, it) = &values[i];
		pthread_rwlock_unlock(&lock);
#else
		st = cuckoo_insert(table, &values[i]);
#endif

		if (st > 0) {
			th->num_written ++;
			total_inserted++;
		} else {
			printf(
			"[writer%d] unknown error for key %zu (%d)\n", th->id, i, st);
			th->failures ++;
		}
    }

	printf("[writer%d] finished writing keys\n", th->id);
    pthread_exit(NULL);
}

static void usage() {
    printf("test_cuckoo_mt:\ttest cuckoo hash table with multiple threads\n");
    printf("\t-r #: the number of readers\n");
    printf("\t-w #: the number of writers\n");
    printf("\t-h  : show usage\n");
}

int main(int argc, char** argv) 
{

    int i;
    int num_writers = 1;
    int num_readers = 1;
    struct timeval tvs, tve; 
    double tvsd, tved, tdiff;

    char ch; 
    while ((ch = getopt(argc, argv, "r:w:h")) != -1) {
        switch (ch) {
        case 'w': num_writers = atoi(optarg); break;
        case 'r': num_readers = atof(optarg); break;
        case 'h': usage(argv[0]); exit(0); break;
        default:
            usage(argv[0]);
            exit(-1);
        }   
    }  

    printf("initializing keys\n");
    task_init(total);

    printf("initializing hash table\n");

#ifdef OPENHASH
	table = openhash_init(kv);
	pthread_rwlock_init(&lock, NULL);
#else
    table = cuckoo_init(power);
    cuckoo_report(table);
#endif

    pthread_t* readers = calloc(sizeof(pthread_t), num_readers);
    pthread_t* writers = calloc(sizeof(pthread_t), num_writers);

    // Allocate cache-line aligned arguments to prevent false sharing.
    padded_thread_arg_t* reader_args;
    // I'd like to align to padded_thread_arg_t, but don't know an alignmentof
    // operator in pure C.
    posix_memalign((void**) &reader_args,
                   CACHE_LINE_SIZE,
                   sizeof(padded_thread_arg_t) * num_readers);
    padded_thread_arg_t* writer_args;
    // I'd like to align to padded_thread_arg_t, but don't know an alignmentof
    // operator in pure C.
    posix_memalign((void**) &writer_args,
                   CACHE_LINE_SIZE,
                   sizeof(padded_thread_arg_t) * num_writers);

    // create threads as writers
    for (i = 0; i < num_writers; i ++) {
        writer_args[i].arg.id = i;
        if (pthread_create(&writers[i], NULL, insert_thread, &writer_args[i]) != 0) {
            fprintf(stderr, "Can't create thread for writer%d\n", i);
            exit(-1);
        }
    }

    // create threads as readers
    for (i = 0; i < num_readers; i ++) {
        reader_args[i].arg.id = i;
        if (pthread_create(&readers[i], NULL, lookup_thread, &reader_args[i]) != 0) {
            fprintf(stderr, "Can't create thread for reader%d\n", i);
            exit(-1);
        }
    }


    gettimeofday(&tvs, NULL); 
    tvsd = (double)tvs.tv_sec + (double)tvs.tv_usec/1000000;

    size_t* last_num_read = calloc(sizeof(size_t), num_readers);
    size_t* last_num_written = calloc(sizeof(size_t), num_writers);
    memset(last_num_read, 0, num_readers);
    memset(last_num_written, 0, num_writers);
    while (1) {
        sleep(1);
        gettimeofday(&tve, NULL); 
        tvsd = (double)tvs.tv_sec + (double)tvs.tv_usec/1000000;
        tved = (double)tve.tv_sec + (double)tve.tv_usec/1000000;
        tdiff = tved - tvsd;
        printf("[tput in MOPS] ");
        for (i = 0; i < num_readers; i ++) {
            printf("reader%d %4.2f ", i, (reader_args[i].arg.num_read - last_num_read[i])/ tdiff/ million );
            last_num_read[i] = reader_args[i].arg.num_read;
        }
        for (i = 0; i < num_writers; i ++) {
            printf("writer%d %4.2f (%zu)", i, (writer_args[i].arg.num_written - last_num_written[i])/ tdiff/ million, writer_args[i].arg.num_written );
            last_num_written[i] = writer_args[i].arg.num_written;
        }
        printf("\n");
        tvs = tve;
    }
}
