#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <memory.h>
#include <sys/file.h>
#include <limits.h>

const int MALLOC_SIZE = 32505856; // 31 MB
const int WRITE_FILE_SIZE = 124780544; // 119 MB
const int GENERATE_DATA_THREADS_NUMBER = 62; // Number of threads to fill memory with data

const int IO_BLOCK_SIZE = 23; // bytes

const char *FILE_NAME = "file"; // Prefix to use in file names
const int FILES_NUMBER = MALLOC_SIZE / WRITE_FILE_SIZE + (MALLOC_SIZE % WRITE_FILE_SIZE == 0 ? 0 : 1);
const int WRITE_TO_FILES_THREADS_NUMBER = 55;

const int READ_FROM_FILES_THREADS_NUMBER = 100;

int random_fd;
void *allocated_memory_pointer;
int global_wrote_blocks_count = 0;
int global_read_blocks_count = 0;
unsigned int max_number = INT_MIN;

struct arg_struct {
    char *file_name;
    int thread_index;
    int offset;
    int limit;
};

/**
 * Allocates memory using malloc
 * @return
 */
void *allocate_memory() {
    printf("[Allocating memory] Started\n");
    void *data_pointer = malloc(MALLOC_SIZE);
    if (data_pointer == NULL) {
        printf("[Allocating memory] Failed\n");
        exit(1);
    }
    printf("[Allocating memory] Succeeded\n");
    return data_pointer;
}

/**
 * Deallocated memory
 */
void free_memory() {
    printf("[Deallocating memory] Started\n");
    free(allocated_memory_pointer);
    printf("[Deallocating memory] Succeeded\n");
}

/**
 * Opens /dev/urandom file and stores file descriptor in
 * 'random_fd' variable
 */
void open_dev_urandom() {
    printf("[Opening /dev/urandom] Started\n");
    random_fd = open("/dev/urandom", O_RDONLY);
    if (random_fd < 0) {
        printf("[Opening /dev/urandom] Failed\n");
        exit(2);
    }
    printf("[Opening /dev/urandom] Succeeded\n");
}

/**
 * Closes /dev/urandom file
 */
void close_dev_urandom() {
    printf("[Closing /dev/urandom] - Started\n");
    close(random_fd);
    printf("[Closing /dev/urandom] - Succeeded\n");
}

/**
 * Function that is called in several threads to fill memory with data
 * Each thread fills it's own chunk of the memory region
 * @param thread_index_pointer Index of the thread
 * @return void*
 */
void *fill_with_random_data(void *thread_index_pointer) {
    int thread_index = *(int *) thread_index_pointer;
    printf("[Thread %d] - Started\n", thread_index);
    int chunk_size = MALLOC_SIZE / GENERATE_DATA_THREADS_NUMBER;
    void *start_from = (void *) (allocated_memory_pointer + thread_index * chunk_size);
    ssize_t result = read(random_fd, start_from, chunk_size);
    if (result == -1) {
        printf("[Thread %d] - Failed\n", thread_index);
        exit(3);
    }
    printf("[Thread %d] - Succeeded\n", thread_index);
}

/**
 * Fills allocated memory region with random data from /dev/urandom
 */
void fill_in_parallel() {
    pthread_t threads[GENERATE_DATA_THREADS_NUMBER];
    int thread_indexes[GENERATE_DATA_THREADS_NUMBER];

    printf("[Filling data] - Started\n");
    for (int i = 0; i < GENERATE_DATA_THREADS_NUMBER; i++) {
        thread_indexes[i] = i;
        pthread_create(&threads[i], NULL, fill_with_random_data, &thread_indexes[i]);
    }
    for (int i = 0; i < GENERATE_DATA_THREADS_NUMBER; i++) {
        pthread_join(threads[i], NULL);
    }
    printf("[Filling data] - Succeeded\n");
}

/**
 * @param i Index of the file
 * @return file_name Name of the file
 */
char *get_file_name(int i) {
    char *i_string = malloc(sizeof(i) + 1);
    sprintf(i_string, "%d", i);
    char *file_name = malloc(strlen(FILE_NAME) + strlen(i_string));
    strcpy(file_name, FILE_NAME);
    strcat(file_name, i_string);
    return file_name;
}

/**
 * @param file_name
 * @return
 */
int open_file_to_write(char *file_name) {
    int file_descriptor = open(file_name, O_CREAT | O_TRUNC | O_WRONLY | O_DIRECT, S_IRWXU | S_IRGRP | S_IROTH);
    if (file_descriptor < 0) {
        printf("[File %s] - Failed\n", file_name);
        exit(4);
    }
    return file_descriptor;
}

/**
 * @param file_name
 * @return
 */
int open_file_to_read(char *file_name) {
    int file_descriptor = open(file_name, O_CREAT | O_RDONLY | O_DIRECT, S_IRWXU | S_IRGRP | S_IROTH);
    if (file_descriptor < 0) {
        printf("[File %s] - Failed\n", file_name);
        exit(4);
    }
    return file_descriptor;
}

/**
 * Counts number of blocks of size IO_BLOCK_SIZE between 'from' and 'to' offsets
 * @param from
 * @param to
 * @return
 */
int get_number_of_blocks(int from, int to) {
    int size = to - from;
    int blocks_number = size / IO_BLOCK_SIZE;
    if (blocks_number * IO_BLOCK_SIZE < size) {
        blocks_number++;
    }
    return blocks_number;
}

/**
 * Moves file descriptor's cursor pointer to the position
 * @param file_descriptor
 * @param offset
 */
void move_cursor_to_position(int file_descriptor, int position) {
    lseek(file_descriptor, position, SEEK_SET);
}

/**
 * @param arguments See arg_struct
 * @return
 */
void *write_block(void *arguments) {
    struct arg_struct *args = (struct arg_struct *) arguments;
    printf("[Thread %d] - Started\n", args->thread_index);

    int file_descriptor = open_file_to_write(args->file_name);
    int blocks_number = get_number_of_blocks(args->offset, args->limit);

    move_cursor_to_position(file_descriptor, args->offset);

    for (int i = 0; i < blocks_number; i++) {
        flock(file_descriptor, LOCK_EX);
        int block_size = IO_BLOCK_SIZE;
        int is_last_block = i == blocks_number - 1 && args->thread_index == WRITE_TO_FILES_THREADS_NUMBER - 1;
        if (is_last_block) {
            block_size = MALLOC_SIZE - args->offset - IO_BLOCK_SIZE * (blocks_number - 1);
        }
        void *write_from_pointer = allocated_memory_pointer + args->offset + i * IO_BLOCK_SIZE;
        ssize_t bytes_wrote = write(file_descriptor, write_from_pointer, block_size);
        if (bytes_wrote == -1) {
            printf("[File %s] - Failed\n", args->file_name);
            exit(5);
        }
        global_wrote_blocks_count++;
        printf("\r[Writing in progress] %d / %d", global_wrote_blocks_count, get_number_of_blocks(0, MALLOC_SIZE));
        fflush(stdout);
        flock(file_descriptor, LOCK_UN);
    }
    close(file_descriptor);
    printf("[Thread %d] - Succeeded\n", args->thread_index);
}

/**
 * @param file_name
 * @param thread_index
 * @return
 */
struct arg_struct *get_args(char *file_name, int threads_number, int thread_index) {
    int blocks_number = get_number_of_blocks(0, MALLOC_SIZE);
    int blocks_per_thread = blocks_number / threads_number;
    struct arg_struct *args = malloc(sizeof(struct arg_struct) * 1);
    int is_last_thread = thread_index == threads_number - 1;
    args->offset = blocks_per_thread * thread_index * IO_BLOCK_SIZE;
    if (is_last_thread) {
        args->limit = MALLOC_SIZE;
    } else {
        args->limit = blocks_per_thread * (thread_index + 1) * IO_BLOCK_SIZE;
    }
    args->file_name = file_name;
    args->thread_index = thread_index;

    return args;
}

/**
 * @param file_name
 */
void write_data_to_file(char *file_name) {
    printf("[File %s] - Started\n", file_name);

    pthread_t threads[WRITE_TO_FILES_THREADS_NUMBER];

    for (int i = 0; i < WRITE_TO_FILES_THREADS_NUMBER; i++) {
        struct arg_struct *args = get_args(file_name, WRITE_TO_FILES_THREADS_NUMBER, i);
        pthread_create(&threads[i], NULL, write_block, args);
    }

    for (int i = 0; i < WRITE_TO_FILES_THREADS_NUMBER; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("[File %s] - Succeeded\n", file_name);
}

/**
 * Writes data from memory to the files
 */
void write_data_to_files() {
    printf("[Writing data to files] - Started\n");
    for (int i = 0; i < FILES_NUMBER; i++) {
        char *file_name = get_file_name(i);
        write_data_to_file(file_name);
    }
    printf("[Writing data to files] - Succeeded\n");
}

/**
 * Allocates memory and fills it with data
 */
void generate_data() {
    allocated_memory_pointer = allocate_memory();
    open_dev_urandom();
    fill_in_parallel();
    close_dev_urandom();
}

void *read_block(void *arguments) {
    struct arg_struct *args = (struct arg_struct *) arguments;
    printf("[Thread %d] - Started\n", args->thread_index);

    int file_descriptor = open_file_to_read(args->file_name);
    int blocks_number = get_number_of_blocks(args->offset, args->limit);

    for (int i = 0; i < blocks_number; i++) {
        flock(file_descriptor, LOCK_EX);
        int block_size = IO_BLOCK_SIZE;
        int is_last_block = i == blocks_number - 1 && args->thread_index == READ_FROM_FILES_THREADS_NUMBER - 1;
        if (is_last_block) {
            printf("[Thread %d] %d - %d - %d * (%d - 1) \n", args->thread_index, MALLOC_SIZE, args->offset, IO_BLOCK_SIZE , blocks_number);
            block_size = MALLOC_SIZE - args->offset - IO_BLOCK_SIZE * (blocks_number - 1);
        }
        printf("[Thread %d] - block_size = %d\n", args->thread_index, block_size);
        __uint32_t * content = (__uint32_t*) malloc(block_size);
        move_cursor_to_position(file_descriptor, args->offset + i * IO_BLOCK_SIZE);
        ssize_t bytes_read = read(file_descriptor, content, block_size);
        if (bytes_read == -1) {
            printf("[File %s] - Failed\n", args->file_name);
            exit(6);
        }
        global_read_blocks_count++;
        for (int j = 0; j < bytes_read/sizeof(__uint32_t); j ++) {
            max_number = content[j] > max_number ? content[j] : max_number;
        }
        printf("\r[Reading in progress] %d / %d blocks ", global_read_blocks_count, get_number_of_blocks(0, MALLOC_SIZE));
        fflush(stdout);
        flock(file_descriptor, LOCK_UN);
    }
    close(file_descriptor);
    printf("[Thread %d] - Succeeded\n", args->thread_index);
}

void analyze_file(char *file_name) {
    printf("[File %s] - Started\n", file_name);

    pthread_t threads[READ_FROM_FILES_THREADS_NUMBER];

    for (int i = 0; i < READ_FROM_FILES_THREADS_NUMBER; i++) {
        struct arg_struct *args = get_args(file_name, READ_FROM_FILES_THREADS_NUMBER, i);
        pthread_create(&threads[i], NULL, read_block, args);
    }

    for (int i = 0; i < READ_FROM_FILES_THREADS_NUMBER; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("[File %s] - Succeeded\n", file_name);
}

void analyze_files() {
    printf("[Analyzing files] - Started\n");
    for (int i = 0; i < FILES_NUMBER; i++) {
        char *file_name = get_file_name(i);
        analyze_file(file_name);
    }
    printf("[Analyzing files] - Succeeded. Max number is %u\n", max_number);
}

int main() {
    //while(1) {
        generate_data();
        write_data_to_files();
        analyze_files();
        free_memory();
    //}
}
