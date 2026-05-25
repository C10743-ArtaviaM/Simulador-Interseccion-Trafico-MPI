#include <stdio.h>
#include "interseccion_mpi.h"

int main(int argc, char** argv) {
    int rango;
    int cantidad_procesos;

    int cantidad_vehiculos = 10; 

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rango);
    MPI_Comm_size(MPI_COMM_WORLD, &cantidad_procesos);

    validar_cantidad_procesos(cantidad_procesos, rango);

    estructura_fase1(rango, cantidad_vehiculos);

    MPI_Finalize();
    
    return 0;
}
