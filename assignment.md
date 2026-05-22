<div id="header" align="center">
  <img src="https://media.giphy.com/media/M9gbBd9nbDrOTu1Mqx/giphy.gif" width="100"/>
</div>

<div align="center">
  <img src="https://media.giphy.com/media/dWesBcTLavkZuG35MI/giphy.gif" width="600" height="300"/>
</div>

---

<div align="center">
  <h1>
    <p>UNIVERSIDAD DE COSTA RICA</p>
    <p>ESCUELA DE CIENCIAS DE LA COMPUTACION E INFORMATICA</p>
    <p>CI-0117 PROGRAMACION PARALELA & CONCURRENTE — I Ciclo 2026</p>
  </h1>
</div>

<div align="center">
  <h2>
    <p>Tarea Corta — Programación Paralela y Concurrente</p>
  </h2>
</div>

<div align="center">
  <h3>
    <p>Simulador de Intersección de Tráfico con MPI</p>
  </h3>
</div>

<!-- ======================================================================= -->

<div align="center">
  <table>
    <thead>
      <tr>
        <th></th>
        <th></th>
        <th></th>
        <th></th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>Curso</strong></td>
        <td>CI-0117 Prog. Paralela y Concurrente</td>
        <td><strong>Modalidad</strong></td>
        <td>Parejas (máximo 2 personas)</td>
      </tr>
      <tr>
        <td><strong>Profesor</strong></td>
        <td>M.Sc. Sleyter Angulo</td>
        <td><strong>Valor</strong></td>
        <td>10 puntos</td>
      </tr>
      <tr>
        <td><strong>Entrega</strong></td>
        <td>Viernes 29 mayo 2026, 06:55</td>
        <td><strong>Compilador</strong></td>
        <td><code>mpicc</code> (wrapper de gcc)</td>
      </tr>
      <tr>
        <td><strong>Lenguaje</strong></td>
        <td>C (estándar C23)</td>
        <td><strong>Biblioteca</strong></td>
        <td>MPI — OpenMPI</td>
      </tr>
    </tbody>
  </table>
</div>

## 1. Contexto y Motivación

En la tarea anterior abordamos la intersección de tráfico usando **hilos POSIX y semáforos**, donde todos los carriles compartían memoria. Ahora lo atacamos desde un ángulo distinto: **MPI (Message Passing Interface).** En este modelo, cada proceso tiene su propia memoria privada y la única forma de comunicarse es enviando y recibiendo **mensajes explícitos**.

Esto refleja fielmente sistemas reales como semáforos inteligentes en intersecciones urbanas, donde cada controlador de carril es un dispositivo físico independiente y la coordinación ocurre por red. No hay variables compartidas: hay un **proceso coordinador** (rank $0$) y **cuatro procesos** carril (ranks $1$–$4$).

> **Concepto clave — Patrón Coordinador–Trabajador**
> 
> En lugar de `sem_wait()`, un carril **envía un mensaje** pidiendo permiso. En lugar de `sem_post()`, el coordinador **responde** cuando el cruce está libre. La exclusión mutua emerge del flujo de mensajes, no de una variable compartida.

### Comparativa: Semáforos vs MPI

| Aspecto | Semáforos (tarea anterior) | MPI (esta tarea) |
| :-- | :-- | :-- |
| **Memoria** | Compartida entre hilos | Privada por proceso |
| **Sincronización** | `sem_wait` / `sem_post` | `MPI_Send` / `MPI_Recv` |
| **Exclusión mutua** | Semáforo binario | Solo el coordinador da permisos |
| **Race condition** | Posible sin semáforo | Imposible — no hay mem. compartida |
| **Escala** | Mismo servidor (1 nodo) | Múltiples nodos en red |

## 2. Arquitectura del Sistema

El simulador se lanza con exactamente **5 procesos MPI**: un coordinador y cuatro carriles. El programa es un único ejecutable que se comporta diferente según el **rank** recibido.

### 2.1 Roles de los procesos

| Rank | Rol | Responsabilidad |
| :-- | :-- | :-- |
| **0** | **Coordinador** | Controlador central. Recibe solicitudes de cruce, otorga permisos uno a uno, lleva estadísticas globales y genera el reporte final. |
| **1** | **Norte** | Proceso carril. Genera `N_VEHICULOS` vehículos, envía solicitud de cruce al coordinador, espera autorización, simula el cruce y notifica la finalización. |
| **2** | **Sur** | Proceso carril. Genera `N_VEHICULOS` vehículos, envía solicitud de cruce al coordinador, espera autorización, simula el cruce y notifica la finalización. |
| **3** | **Este** | Proceso carril. Genera `N_VEHICULOS` vehículos, envía solicitud de cruce al coordinador, espera autorización, simula el cruce y notifica la finalización. |
| **4** | **Oeste** | Proceso carril. Genera `N_VEHICULOS` vehículos, envía solicitud de cruce al coordinador, espera autorización, simula el cruce y notifica la finalización. |

### 2.2 Protocolo de mensajes

Todas las comunicaciones usan `MPI_COMM_WORLD` con etiquetas definidas como constantes:

```c
#define TAG_SOLICITUD     1     /* carril → coordinador: pido cruzar */
#define TAG_PERMISO       2     /* coordinador → carril: ya puede cruzar */
#define TAG_CRUCE_FIN     3     /* carril → coordinador: terminé */
#define TAG_ESTADISTICA   4     /* carril → coordinador: mis contadores */
```

### 2.3 Flujo de un ciclo de cruce

```bash
Carril (rank 1-4):                      Coordinador (rank 0):
──────────────────────────────────────────────────────────────────────────────
1. Genera vehículo (ej: "Norte-003")    1. Espera mensaje de cualquier carril

2. MPI_Send(TAG_SOLICITUD) ──────────►  2. MPI_Recv(MPI_ANY_SOURCE, TAG_SOLICITUD)

3. MPI_Recv(TAG_PERMISO)                3. Si cruce libre → MPI_Send(TAG_PERMISO)
    (bloquea hasta recibir permiso)     4. Si ocupado → encola la solicitud
    
4. usleep() — simula el cruce           5. MPI_Recv(TAG_CRUCE_FIN)
5. MPI_Send(TAG_CRUCE_FIN) ──────────►  6. Marca cruce libre → atiende siguiente
6. Repite para el siguiente vehículo
```

## 3. Requerimientos de Implementación

La implementación se divide en tres fases. Cada fase agrega funcionalidad sobre la anterior.

> ### FASE 1 — Estructura Base MPI [3.0 puntos]
>
> Implementar el esqueleto con los 5 procesos correctamente identificados. En esta fase los carriles se presentan y el coordinador los saluda.
> 
> - **R1.1 Init / Finalize:** Todo programa MPI debe comenzar con `MPI_Init` y terminar con `MPI_Finalize`. Obligatorio en todos los procesos.
> - **R1.2 Validación:** Si el programa se lanza con un número distinto de 5 procesos, imprimir un error y abortar con `MPI_Abort(MPI_COMM_WORLD, 1)`.
> - **R1.3 Bcast de parámetros:** El coordinador define `N_VEHICULOS` y lo distribuye con `MPI_Bcast`. Garantiza que todos los procesos usen el mismo valor.
> - **R1.4 Saludo inicial:** Cada carril envía su nombre al coordinador (`MPI_Send`) y el coordinador los recibe e imprime en orden.
> 
> ```bash
> $ mpirun -np 5 ./interseccion_mpi
> 
> [Coordinador] Sistema iniciado. N_VEHICULOS=10 distribuido a 4 carriles.
> [Norte] listo. Esperando autorización para cruzar.
> [Sur] listo. Esperando autorización para cruzar.
> [Este] listo. Esperando autorización para cruzar.
> [Oeste] listo. Esperando autorización para cruzar.
> ``` 

> ### FASE 2 — Protocolo de Cruce Completo [4.5 puntos]
> 
> Implementar el protocolo completo de solicitud–permiso–notificación. El coordinador garantiza que un solo vehículo cruce a la vez. La exclusión mutua emerge del flujo de mensajes.
> - **R2.1 Proceso Carril — ciclo de vida:**
>   - **(a)** Generar vehículo con id único, ej: "`Norte-003`".
>   - **(b)** `MPI_Send(&id, 1, MPI_INT, 0, TAG_SOLICITUD, MPI_COMM_WORLD)` — pedir permiso.
>   - **(c)** `MPI_Recv(&resp, 1, MPI_INT, 0, TAG_PERMISO, ...)` — bloquear hasta recibir autorización.
>   - **(d)** `usleep(2000 + rand()%4000)` — simular el tiempo de cruce.
>   - **(e)** `MPI_Send(&id, 1, MPI_INT, 0, TAG_CRUCE_FIN, ...)` — notificar fin del cruce.
>   - **(f)** Repetir para el siguiente vehículo.
> 
> - **R2.2 Proceso Coordinador — ciclo de vida:**
>   - **(a)** Ejecutar bucle hasta que todos los 4 carriles hayan enviado sus `N_VEHICULOS` (llevar conteo por carril).
>   - **(b)** `MPI_Recv(..., MPI_ANY_SOURCE, TAG_SOLICITUD, ...)` — recibir del carril que llegue primero.
>   - **(c)** Si cruce libre → enviar permiso inmediatamente. Si no → encolar la solicitud.
>   - **(d)** Al recibir `TAG_CRUCE_FIN`, marcar cruce libre y atender el siguiente de la cola.
> 
> - **R2.3 Sin accidentes garantizados:** A diferencia de la versión con semáforos, en MPI es **arquitecturalmente imposible** que dos vehículos crucen a la vez: el coordinador nunca envía dos permisos antes de recibir el fin del primero.
> 
> - **R2.4 MPI_Barrier al inicio:** Usar `MPI_Barrier(MPI_COMM_WORLD)` después del Bcast para que todos los procesos inicien la simulación simultáneamente.
> 
> - **R2.5 Terminación limpia:** Cuando un carril termina todos sus vehículos, enviar señal `id = -1` con `TAG_SOLICITUD` para indicar que no habrá más solicitudes de ese rank.

> ### FASE 3 — Estadísticas Colectivas y Reporte [2.5 puntos]
>
> Al finalizar, recolectar estadísticas distribuidas usando operaciones colectivas MPI.
> - **R3.1 MPI_Reduce:** Cada carril lleva su contador local de vehículos cruzados. Al terminar, usar `MPI_Reduce` con `MPI_SUM` para obtener el total global en el coordinador.
> - **R3.2 MPI_Wtime:** Medir el tiempo total de simulación con `MPI_Wtime()` (equivalente MPI de `clock_gettime`). El coordinador mide desde el Barrier hasta la última notificación.
> - **R3.3 MPI_Gather:** Usar `MPI_Gather` para que el coordinador reciba el tiempo de espera acumulado de cada carril. Reportar el carril con mayor y menor espera.
> - **R3.4 Reporte:** El coordinador imprime el reporte del formato definido en la Sección $4$.
> - **R3.5 README:** Responder las preguntas de análisis de la Sección $5$.

## 4. Formato de Salida Requerido

El programa debe producir exactamente el siguiente formato (los valores varían por ejecución):

```bash
$ mpirun -np 5 ./interseccion_mpi

======================================================
SIMULADOR DE INTERSECCION DE TRAFICO CON MPI
Procesos: 5 | Carriles: 4 | Vehiculos/carril: 10
======================================================

[Coordinador]   Parametros distribuidos. Iniciando...
[Norte-001]     → solicita cruce
[Coordinador]   ✓ Norte-001 autorizado. Cruce OCUPADO.
[Sur-001]       → solicita cruce (en cola: 1 esperando)
[Norte-001]     ✓ cruce completado. Cruce LIBRE.
[Coordinador]   ✓ Sur-001 autorizado (era el siguiente en cola).
...

======== REPORTE FINAL ========

  Vehiculos cruzados (total):     40 / 40
  Accidentes:                     0 ← garantizado por arquitectura
  Tiempo de simulacion:           0.523 s (MPI_Wtime)

  Estadisticas por carril (MPI_Gather):
  Carril      Vehiculos     Espera acum. (s)
  Norte       10            0.312
  Sur         10            0.298
  Este        10            0.334   ← mayor espera
  Oeste       10            0.285   ← menor espera

  Funciones MPI usadas: MPI_Send, MPI_Recv, MPI_Bcast,
                        MPI_Barrier, MPI_Reduce, MPI_Gather
======================================================
```

## 5. Preguntas de Análisis (README)
Incluya un archivo `README.txt` o `README.md` con respuestas razonadas (mínimo $3$ oraciones cada una):

&nbsp;&nbsp;&nbsp;&nbsp; **a)** **Garantía sin accidentes:** ¿Por qué es arquitecturalmente imposible que dos vehículos crucen simultáneamente en la versión MPI? ¿Necesitamos detectar accidentes como en la versión con semáforos? Justifique.

&nbsp;&nbsp;&nbsp;&nbsp; **b)** **MPI vs Semáforos:** Compare las dos versiones del simulador. ¿Cuál es más fácil de razonar sobre su correctitud? ¿Cuál escalaría mejor a $100$ carriles? ¿Por qué?

&nbsp;&nbsp;&nbsp;&nbsp; **c)** **El coordinador como cuello de botella:** ¿Puede el coordinador convertirse en un cuello de botella? ¿Bajo qué condiciones? ¿Cómo podría diseñarse un sistema MPI sin coordinador central que igualmente garantice un solo vehículo a la vez?

&nbsp;&nbsp;&nbsp;&nbsp; **d)** **MPI_ANY_SOURCE:** El coordinador usa $MPI_ANY_SOURCE$ para recibir solicitudes. ¿Qué ventaja tiene? ¿Podría causar starvation para algún carril? ¿Cómo lo detectaría en los datos del reporte?

&nbsp;&nbsp;&nbsp;&nbsp; **e)** **Bonus (+0.5 pts):** Modifique el coordinador para usar `MPI_Irecv` (no bloqueante) en lugar de `MPI_Recv` bloqueante. ¿Qué ventaja aporta? ¿Cambia el comportamiento observable?

## 6. Estructura de Entrega

Entregue un único archivo `TareaCorta_Apellido1_Apellido2`.zip con:

```bash
TareaCorta_Apellido1_Apellido2/
├── interseccion_mpi.c    ← código fuente (único archivo .c)
├── Makefile              ← instrucciones de compilación
└── README.txt            ← análisis + instrucciones de ejecución
```

### Makefile mínimo requerido

```Makefile
CC      = mpicc
CFLAGS  = -Wall -Wextra -std=c11 -O2

all: interseccion_mpi

interseccion_mpi: interseccion_mpi.c
  $(CC) $(CFLAGS) interseccion_mpi.c -o interseccion_mpi

run: interseccion_mpi
  mpirun -np 5 ./interseccion_mpi

clean:
  rm -f interseccion_mpi
```

> ### Restricciones de implementación
> 
> - **Solo C (C11) con MPI.** No se permite C++, Python, ni ninguna otra biblioteca de paralelismo (sin pthreads, sin OpenMP, sin semáforos POSIX).
> - **Exactamente 5 procesos MPI.** El programa debe validar esto al inicio y abortar con mensaje si se lanza con otro número.
> - **Un único archivo fuente.** Todo el código en `interseccion_mpi.c`.
> - **Debe compilar sin errores con** `mpicc -Wall -Wextra -std=c11`. Entregas que no compilen reciben $0$.
> - **No usar variables globales para comunicación.** Toda coordinación debe ser por mensajes MPI.

## 7. Rúbrica de Evaluación

| # | Criterio | Pts. | Verificación |
| :---: | :---- | :---: | :---- |
| 1 | Fase 1: MPI_Init/Finalize, validación de 5 procesos, MPI_Bcast, saludo inicial | **1.0** | 5 procesos, Bcast correcto |
| 2 | Fase 2: Protocolo solicitud–permiso–fin completo (los 3 tags implementados) | **2.0** | 0 accidentes en 10 ejecuciones |
| 3 | Fase 2: Coordinador gestiona cola, MPI_ANY_SOURCE, terminación por señal -1 | **1.5** | Termina siempre, sin deadlock |
| 4 | Fase 2: MPI_Barrier sincroniza el inicio de la simulación | **1.0** | Todos inician y terminan bien |
| 5 | Fase 3: MPI_Reduce (total), MPI_Wtime (tiempo), MPI_Gather (espera por carril) | **1.5** | Reporte con formato correcto |
| 6 | Fase 3: README con respuestas a preguntas a–d (mínimo 3 oraciones c/u) | **2.0** | Respuestas técnicamente correctas |
| 7 | Código limpio: constantes para tags, comentarios, sin warnings | **0.5** | 0 warnings con - Wall -Wextra |
| + | Bonus: MPI_Irecv en coordinador + análisis en README (pregunta e) | **0.5** | Comportamiento equivalente |
|  | **TOTAL** | **10 pts** |  |

## 8. Entrega y Política de Atraso

> 📅 Fecha límite: Domingo 17 de mayo de 2026, 23:55 (hora de Costa Rica)
> 
> Entrega a través del sistema de asignaciones del curso. No se aceptan entregas por correo.

- **Penalización por atraso:** No se recibe la tarea.
- **Defensa oral:** Cada integrante debe poder explicar cualquier línea del código. Ante dudas, se realizará una defensa oral corta.
- **Integridad académica:** Queda estrictamente prohibido copiar código de otras parejas, repositorios públicos o herramientas de IA. Violaciones resultan en 0 y reporte al comité disciplinario.

## 9. Funciones MPI Útiles para esta Tarea

| Función | Para qué usarla en esta tarea |
| :--- | :--- |
| `MPI_Init` / `MPI_Finalize` | Obligatorio al inicio y al final de todo programa MPI. |
| `MPI_Comm_rank` / `MPI_Comm_size` | Saber quién soy (rank) y cuántos procesos hay en total. |
| `MPI_Abort(MPI_COMM_WORLD, 1)` | Terminar todos si se detecta error (ej: $N ≠ 5$). |
| `MPI_Bcast(&n, 1, MPI_INT, 0, ...)` | Distribuir `N_VEHICULOS` desde coordinador a todos. |
| `MPI_Barrier(MPI_COMM_WORLD)` | Sincronizar el inicio: todos esperan al más lento. |
| `MPI_Send` / `MPI_Recv` | Solicitar permiso, otorgar permiso, notificar fin. |
| `MPI_Recv(..., MPI_ANY_SOURCE, ...)` | Coordinador recibe del carril que llegue primero. |
| `MPI_Reduce(..., MPI_SUM, ...)` | Sumar vehículos de todos los carriles en coordinador. |
| `MPI_Gather(...)` | Recolectar tiempos de espera por carril. |
| `MPI_Wtime()` | Medir tiempo con precisión (equivale a clock_gettime). |
| `MPI_Irecv (bonus)` | Recepción no bloqueante para coordinador más eficiente. |

> ### 💡 Sugerencias de desarrollo
> - Instalar MPI: `sudo apt install libopenmpi-dev openmpi-bin` (Ubuntu/Debian).
> - Comience por la Fase 1: verifique que los 5 procesos se identifican antes de agregar mensajes.
> - Pruebe primero con `N_VEHICULOS=2` para observar el flujo completo antes de usar valores grandes.
> - Use constantes (`#define TAG_SOLICITUD 1`) en lugar de números directamente en el código.
> - El coordinador (rank $0$) nunca genera ni cruza vehículos. Su único rol es gestionar permisos.
> - MPI no garantiza orden global de los printf. Agregue el rank o nombre al inicio de cada línea de salida.

---

<div align="center" style="font-size: 14px">
  <i> M.Sc. Sleyter Angulo — CI-0117 Programación Paralela y Concurrente — I Ciclo 2026 — UCR </i>
</div>

<div align="center" style="font-size: 10px;">
  <i> @C10743 - Mauricio Artavia Monge </i>
</div>