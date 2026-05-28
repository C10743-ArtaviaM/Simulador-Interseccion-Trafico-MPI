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

**(b)** **MPI vs Semáforos:** Compare las dos versiones del simulador. ¿Cuál es más fácil de razonar sobre su correctitud? ¿Cuál escalaría mejor a $100$ carriles? ¿Por qué?

**(c)** **El coordinador como cuello de botella:** ¿Puede el coordinador convertirse en un cuello de botella? ¿Bajo qué condiciones? ¿Cómo podría diseñarse un sistema MPI sin coordinador central que igualmente garantice un solo vehículo a la vez?

**(d)** **MPI_ANY_SOURCE:** El coordinador usa `MPI_ANY_SOURCE` para recibir solicitudes. ¿Qué ventaja tiene? ¿Podría causar starvation para algún carril? ¿Cómo lo detectaría en los datos del reporte?

**(e)** **Bonus (+0.5 pts):** Modifique el coordinador para usar `MPI_Irecv` (no bloqueante) en lugar de `MPI_Recv` bloqueante. ¿Qué ventaja aporta? ¿Cambia el comportamiento observable?