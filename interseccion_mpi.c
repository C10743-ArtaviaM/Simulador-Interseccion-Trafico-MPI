/*
 * interseccion_mpi.c
 * Simulador de Interseccion de Trafico MPI
 *
 * CI-0117 Programacion Paralela & Concurrente - I Ciclo 2026
 *
 * Autores:
 * Mauricio Artavia Monge
 * Daniel Rodriguez Ruiz
 */
#define _POSIX_C_SOURCE 200809L

#include "interseccion_mpi.h"

const char* NOMBRE_CARRIL[N_CARRILES] = {"Norte", "Sur", "Este", "Oeste"};
double espera_acumulada_global = 0.0;

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

/*
 * =============================================================================
 * FASE 2 — Protocolo de Cruce Completo [4.5 puntos]
 * =============================================================================
 */
void ciclo_carril(int rango, int cantidad_vehiculos) {
  const char* nombre = NOMBRE_CARRIL[rango - 1];
  double espera_acumulada = 0.0;

  for (int i = 1; i <= cantidad_vehiculos; i++) {
    int id_vehiculo = (rango * 1000) + i; /* id unico por carril */

    printf("[%s-%03d] → solicita cruce\n", nombre, i);

    /* R2.1b - Envio solicitud */
    MPI_Send(&id_vehiculo, 1, MPI_INT, 0, TAG_SOLICITUD, MPI_COMM_WORLD);

    /* R2.1c - Esperamos permiso (bloqueante) */
    double t_inicio_espera = MPI_Wtime();
    int respuesta;
    MPI_Recv(&respuesta, 1, MPI_INT, 0, TAG_PERMISO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    espera_acumulada += MPI_Wtime() - t_inicio_espera;

    /* R2.1d - Simulamos cruce */
    usleep((50 + rand() % 100) * 1000);
    printf("[%s-%03d] ✓ cruce completado.\n", nombre, i);

    /* R2.1e - Notificamos fin */
    MPI_Send(&id_vehiculo, 1, MPI_INT, 0, TAG_CRUCE_FIN, MPI_COMM_WORLD);
  }

  espera_acumulada_global = espera_acumulada;
  /* R2.5 - Señal de terminacion */
  int fin = -1;
  MPI_Send(&fin, 1, MPI_INT, 0, TAG_SOLICITUD, MPI_COMM_WORLD);

}

void reporte(int total_cruzados, double tiempo_total, double* arreglo_esperas, int cantidad_choques) {
  printf("======== REPORTE FINAL ========\n");
  printf("Vehiculos cruzados(total): %d / %d\n", total_cruzados, (N_VEHICULOS * 4));
  printf("Accidentes: %d\n", cantidad_choques);
  printf("Tiempo total de simulación:         %.4f segundos (MPI_Wtime)\n", tiempo_total);
  printf("-----------------------------------------------------------------------------\n");

  int rank_max = 1; 
  int rank_min = 1;
  
  for (int i = 2; i <= 4; i++) {
    if (arreglo_esperas[i] > arreglo_esperas[rank_max]){
        rank_max = i;
    }

    if (arreglo_esperas[i] < arreglo_esperas[rank_min]){    
        rank_min = i;
    }
  }

  printf("Estadisticas por carril (MPI_Gather):\n");
  printf("%-10s %-10s %-15s\n", "Carril", "Vehiculos", "Espera acum. (s)");
  
  for (int i = 1; i <= 4; i++) {
    printf("%-10s %-10d %-15.3f\n", NOMBRE_CARRIL[i - 1], N_VEHICULOS, arreglo_esperas[i]);
    
  }

    printf("\nMayor espera: %s\n", NOMBRE_CARRIL[rank_max - 1]);

    printf("Menor espera: %s\n", NOMBRE_CARRIL[rank_min - 1]);
}

void ciclo_coordinador(int cantidad_vehiculos) {
  /* Cola FIFO manual: guarda los ranks que esperan por permisos */
  int cola[N_CARRILES * 100];
  int cola_inicio = 0;
  int cola_fin = 0;

  int cruce_ocupado = 0;
  int terminados = 0; /* Son los carriles que enviaron id = -1 */
  int total_esperados = N_CARRILES * cantidad_vehiculos;
  int total_cruzados = 0;

  while(terminados < N_CARRILES || total_cruzados < total_esperados || cruce_ocupado || cola_inicio < cola_fin){
    /* R2.2b - Recibimos del primero que llegue */
    MPI_Status estado;
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &estado);
    int origen = estado.MPI_SOURCE;
    int tag = estado.MPI_TAG;

    if(tag == TAG_SOLICITUD){
      int id_recibido;

      MPI_Recv(&id_recibido, 1, MPI_INT, origen, TAG_SOLICITUD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

      if (id_recibido == -1) {
        /* R2.5 - Carril ha logrado terminar */
        terminados++;
      } else if (!cruce_ocupado) {
        /* Tenemos el cruce libre, damos permiso inmediato */
        int permiso = 1;

        MPI_Send(&permiso, 1, MPI_INT, origen, TAG_PERMISO, MPI_COMM_WORLD);
        cruce_ocupado = 1;
        printf("[Coordinador] ✓ %d autorizado. Cruce OCUPADO.\n", origen);
      } else {
        /* Tenemos cruce ocupado */
        cola[cola_fin++] = origen;
        printf("[Coordinador] %d en cola (%d esperando).\n", origen, cola_fin - cola_inicio);
      }
    } else if (tag == TAG_CRUCE_FIN) {
      int id_recibido;
      MPI_Recv(&id_recibido, 1, MPI_INT, origen, TAG_CRUCE_FIN, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

      /* R2.2d - Vehiculo ha terminado de cruzar */
      total_cruzados++;

      if (cola_inicio < cola_fin) {
        /* Tenemos a alguien por atender en cola */
        int siguiente = cola[cola_inicio++];
        int permiso = 1;

        MPI_Send(&permiso, 1, MPI_INT, siguiente, TAG_PERMISO, MPI_COMM_WORLD);
        printf("[Coordinador] ✓ %d autorizado (siguiente en cola)\n", siguiente);
      } else {
        /* Tenemos cola vacia, hay cruce libre */
        cruce_ocupado = 0;
        printf("[Coordinador] Cruce LIBRE. (%d/%d cruzados)\n", total_cruzados, total_esperados);
      }
    }
  }

  printf("\n[Coordinador] Simulacion completa. %d vehiculos cruzaron.\n", total_cruzados);    
}

int main(int argc, char** argv) {
  int rango;  // 0
  int cantidad_procesos;
  int cantidad_vehiculos = N_VEHICULOS;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rango);
  MPI_Comm_size(MPI_COMM_WORLD, &cantidad_procesos);

  validar_cantidad_procesos(cantidad_procesos, rango);

  estructura_fase1(rango, cantidad_vehiculos);
  double inicio = MPI_Wtime();
  /* R2.4 - Todos se sincronizan antes de iniciar la simulacion */
  MPI_Barrier(MPI_COMM_WORLD);

  if (rango == 0) {
    ciclo_coordinador(cantidad_vehiculos);
  } else {
    srand(rango * 137); /* Da una semilla distinta por carril */
    ciclo_carril(rango, cantidad_vehiculos);
  }

int cruzados_local;
if(rango == 0){
    cruzados_local = 0;
}else{
    cruzados_local = cantidad_vehiculos;
}
int cruzados_global = 0;

MPI_Reduce(&cruzados_local, &cruzados_global, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
           
/* R3.3 */
double esperas[5];

MPI_Gather(&espera_acumulada_global, 1, MPI_DOUBLE, esperas, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

/* R3.2 */
double fin = MPI_Wtime();
double tiempo_total = fin - inicio;

/* R3.4 */
if (rango == 0) {
    reporte(cruzados_global, tiempo_total, esperas, 0);
}
  MPI_Finalize();
  return 0;
}