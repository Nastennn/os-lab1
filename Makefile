all: main

main: main.c
	gcc -g -pthread main.c -o main

clean:
	rm -rf *.o main
