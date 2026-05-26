CC			= mpicc
CFLAGS	= -Wall -Wextra -std=c11 -O2

all: interseccion_mpi

interseccion_mpi: interseccion_mpi.c
	$(CC) $(CFLAGS) interseccion_mpi.c -o interseccion_mpi

run: interseccion_mpi
	mpirun -np 5 ./interseccion_mpi

clean:
	rm -f interseccion_mpi