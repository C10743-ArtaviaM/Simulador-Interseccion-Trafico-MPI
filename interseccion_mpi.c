/*
 * interseccion.c
 * Simulador de Interseccion de Trafico MPI
 *
 * CI-0117 Programacion Paralela & Concurrente - I Ciclo 2026
 *
 * Autores:
 * Mauricio Artavia Monge
 * Daniel Rodriguez Ruiz
 */
#include "interseccion_mpi.h"

const char* NOMBRE_CARRIL[N_CARRILES] = {"Norte", "Sur", "Este", "Oeste"};

void validar_cantidad_procesos(int cantidad_procesos, int rango) {
  if (cantidad_procesos != 5) {
    // solo el maestro es el que termina con el programa
    if (rango == 0) {
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }
}

void estructura_fase1(int rango, int cantidad_vehiculos) {
  MPI_Bcast(&cantidad_vehiculos, 1, MPI_INT, 0, MPI_COMM_WORLD);
  if (rango == 0) {
    printf(
        "[Coordinador] Sistema iniciado. N_VEHICULOS = %d distribuido a 4 "
        "carriles.\n",
        cantidad_vehiculos);
  }

  saludo_inicial(rango);
}

void saludo_inicial(int rango) {
  if (rango == 0) {
    char nombre_carril[10];

    for (int i = 1; i <= 4; i++) {
      MPI_Recv(nombre_carril, 10, MPI_CHAR, i, 0, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
      printf("[%s] listo. Esperando autorización para cruzar.\n",
             nombre_carril);
    }
  } else {
    const char* nombre_carril = NOMBRE_CARRIL[rango - 1];
    MPI_Send(nombre_carril, 10, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
  }
}

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