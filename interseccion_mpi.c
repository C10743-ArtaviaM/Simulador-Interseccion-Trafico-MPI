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
    MPI_Recv(&respuesta, 1, MPI_INT, 0, TAG_PERMISO, MPI_COMM_WORLD,
             MPI_STATUS_IGNORE);
    espera_acumulada += MPI_Wtime() - t_inicio_espera;

    /* R2.1d - Simulamos cruce */
    usleep(2000 + rand() % 4000);

    printf("[%s-%03d] ✓ cruce completado.\n", nombre, i);

    /* R2.1e - Notificamos fin */
    MPI_Send(&id_vehiculo, 1, MPI_INT, 0, TAG_CRUCE_FIN, MPI_COMM_WORLD);
  }

  /* R2.5 - Señal de terminacion */
  int fin = -1;
  MPI_Send(&fin, 1, MPI_INT, 0, TAG_SOLICITUD, MPI_COMM_WORLD);

  /* Fase 3: Enviar espera acumulada al coordinador */
  MPI_Send(&espera_acumulada, 1, MPI_DOUBLE, 0, TAG_ESTADISTICA,
           MPI_COMM_WORLD);
}

void ciclo_coordinador(int cantidad_vehiculos) {
  /* Cola FIFO manual: guarda los ranks que esperan por permisos */
  int cola[N_CARRILES * 100];
  int cola_inicio = 0;
  int cola_fin = 0;
  double esperas[N_CARRILES];
  int esperas_recibidas = 0;

  int cruce_ocupado = 0;
  int terminados = 0; /* Son los carriles que enviaron id = -1 */
  int total_esperados = N_CARRILES * cantidad_vehiculos;
  int total_cruzados = 0;

  while (terminados < N_CARRILES || total_cruzados < total_esperados ||
         cruce_ocupado || cola_inicio < cola_fin ||
         esperas_recibidas < N_CARRILES) {
    /* R2.2b - Recibimos del primero que llegue */
    MPI_Status estado;
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &estado);
    int origen = estado.MPI_SOURCE;
    int tag = estado.MPI_TAG;

    if (tag == TAG_ESTADISTICA) {
      double espera;

      MPI_Recv(&espera, 1, MPI_DOUBLE, origen, TAG_ESTADISTICA, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
      esperas[esperas_recibidas++] = espera;
    } else if (tag == TAG_SOLICITUD) {
      int id_recibido;

      MPI_Recv(&id_recibido, 1, MPI_INT, origen, TAG_SOLICITUD, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);

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
        printf("[Coordinador] %d en cola (%d esperando).\n", origen,
               cola_fin - cola_inicio);
      }
    } else if (tag == TAG_CRUCE_FIN) {
      int id_recibido;
      MPI_Recv(&id_recibido, 1, MPI_INT, origen, TAG_CRUCE_FIN, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);

      /* R2.2d - Vehiculo ha terminado de cruzar */
      total_cruzados++;

      if (cola_inicio < cola_fin) {
        /* Tenemos a alguien por atender en cola */
        int siguiente = cola[cola_inicio++];
        int permiso = 1;

        MPI_Send(&permiso, 1, MPI_INT, siguiente, TAG_PERMISO, MPI_COMM_WORLD);
        printf("[Coordinador] ✓ %d autorizado (siguiente en cola)\n",
               siguiente);
      } else {
        /* Tenemos cola vacia, hay cruce libre */
        cruce_ocupado = 0;
        printf("[Coordinador] Cruce LIBRE. (%d/%d cruzados)\n", total_cruzados,
               total_esperados);
      }
    }
  }

  printf("\n[Coordinador] Simulacion completa. %d vehiculos cruzaron.\n",
         total_cruzados);
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

  /* R2.4 - Todos se sincronizan antes de iniciar la simulacion */
  MPI_Barrier(MPI_COMM_WORLD);

  if (rango == 0) {
    ciclo_coordinador(cantidad_vehiculos);
  } else {
    srand(rango * 137); /* Da una semilla distinta por carril */
    ciclo_carril(rango, cantidad_vehiculos);
  }

  MPI_Finalize();
  return 0;
}