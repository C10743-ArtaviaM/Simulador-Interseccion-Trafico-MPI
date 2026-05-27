
#ifndef INTERSECCION_MPI_H
#define INTERSECCION_MPI_H

#include <mpi.h>
#include <stdio.h>

// Tags de carril
#define TAG_SOLICITUD 1   /* carril coordinador: pido cruzar */
#define TAG_PERMISO 2     /* coordinador carril: ya puede cruzar */
#define TAG_CRUCE_FIN 3   /* carril coordinador: terminé */
#define TAG_ESTADISTICA 4 /* carril coordinador: mis contadores */

#define N_CARRILES 4
#define N_VEHICULOS 50

// Identificadores de carril
#define NORTE 0
#define SUR 1
#define ESTE 2
#define OESTE 3

extern const char* NOMBRE_CARRIL[N_CARRILES];

void validar_cantidad_procesos(int cantidad_procesos, int rango);
void estructura_fase1(int rango, int cantidad_vehiculos);
void saludo_inicial(int rango);
void ciclo_carril(int rango, int cantidad_vehiculos);
void ciclo_coordinador(int cantidad_vehiculos);

#endif /* INTERSECCION_MPI_H */
