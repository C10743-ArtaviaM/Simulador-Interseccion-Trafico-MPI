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

## Instrucciones

### Compilacion

Para la correcta ejecucion del programa desde la terminal, primero se debe compilar utilizando el siguiente comando

```bash
make all
```

A su vez, para ejecutarlo, se utiliza el comando a continuacion

```bash
make run
```

Si desea limpiar los archivos de ejecucion, se puede utilizar el comando que se presenta a continuacion

```bash
make clean
```

## 5. Preguntas de Análisis

**(a)** **Garantía sin accidentes:** ¿Por qué es arquitecturalmente imposible que dos vehículos crucen simultáneamente en la versión MPI? ¿Necesitamos detectar accidentes como en la versión con semáforos? Justifique.

**`R//`** Es tecnicamente imposible porque el cruce se encuentra serializado por el coordinador, el cual es un proceso que es unico y secuncial. Este mantiene un flag `cruce_ocupado` y solo nos envia un `TAG_PERMISO` cuando el flag se encuentra en $0$. Ademas, no hay posibilidad de que dos `TAG_PERMISO` se envien sin que haya primero un `TAG_CRUCE_FIN` entre estos.

Por otro lado, no se necesita detectar accidentes, ya que mientras con los semaforos usabamos memoria compartida, en MPI no se usa, si no que lo que usamos es paso de mensajes, lo que elimina los recursos compartidos por completo.

**(b)** **MPI vs Semáforos:** Compare las dos versiones del simulador. ¿Cuál es más fácil de razonar sobre su correctitud? ¿Cuál escalaría mejor a $100$ carriles? ¿Por qué?

**`R//`** Creemos es mas facil MPI, ya que en esta se podria decir que el vehiculo pide, y el coordinador le responde, y nadie mas puede cruzar mientras el cruce este ocupado. Por otro lado, la version con semaforos se debe analizar mas, para asi evitar los choques, las secciones criticas, las race conditions, entre otros problemas que se encuentran al usar memoria compartida.

En cuanto la escabilidad, creemos escalaria mejor con memoria compartida, ya que agregar mas hilos termina siendo mas barato, ya que la version MPI con coordinador no escalaria bien, ya que todos los carriles mandan mensajes al mismo proceso 0. Haciendo que dure mas respondiendo 1 a 1 a sus solicitudes.

**(c)** **El coordinador como cuello de botella:** ¿Puede el coordinador convertirse en un cuello de botella? ¿Bajo qué condiciones? ¿Cómo podría diseñarse un sistema MPI sin coordinador central que igualmente garantice un solo vehículo a la vez?

**`R//`** Si, como se menciono previamente, si se usan muchos carriles (un $N$ grande), este geraria solicitudes de una forma mas rapida que lo que el coordinador podria atendenderlas. Una condicion es que los tiempos de cruces sean muy cortos, haciendo entonces que la cola se llene rapido.  O tambien que el Hardware con el proceso 0 le toque compartir core con otros procesos.

Podria diseñarse por medio de timestamp, ej, un carro llega y le dice al resto "Quiero cruzar, llegue a las 17:00:05" entonces los que llegaron despues le dicen que esta bien, que pase, y entonces van pasando 1 a 1 segun su tiempo de llegada. En caso de que 2 lleguen al mismo momento, se dejaria pasar al que sea de menor numero de manera arbitraria.

**(d)** **MPI_ANY_SOURCE:** El coordinador usa `MPI_ANY_SOURCE` para recibir solicitudes. ¿Qué ventaja tiene? ¿Podría causar starvation para algún carril? ¿Cómo lo detectaría en los datos del reporte?

**`R//`** La ventaja es que sin el `MPI_ANY_SOURCE`, el coordinador haria polling en orden fijo, y se podria bloquear esperando mensaje de un carril que tal vez aun no envia nada. Con este, el coordinador reacciona al primero que llegue maximizando asi el cruce.

Tecnicamente el starvation puede existir, pero en la practica es poco probable con pocos carriles.

Se podria detectar en el caso de que uno de los carriles termine con un numero de tiempo de espera que sea claramente diferente al resto.

**(e)** **Bonus (+0.5 pts):** Modifique el coordinador para usar `MPI_Irecv` (no bloqueante) en lugar de `MPI_Recv` bloqueante. ¿Qué ventaja aporta? ¿Cambia el comportamiento observable?