/*
    Practica 1.4: Sincronizacion Cliente-Servidor en MPI con OpenMP
    Gomez Ibarra Josue Alejandro
    Munoz Madriz Nadya Janneth

    Ejecutar con 5 procesos (1 servidor + 4 clientes):
        mpiexec -n 5 Proyecto1.exe
*/

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <mpi.h>
#include <omp.h>
#include "LogNodo.h"
#include "Banco.h"

static const double SALDO_INICIAL = 10000.00;
static const int OPS_BASICA = 5;
static const int OPS_MASIVA = 100000;

// Lee un entero; si el usuario escribe letras, limpia la entrada y devuelve 'porDefecto'
static int leerEntero(int porDefecto) {
    int v;
    if (scanf("%d", &v) != 1) {
        int c;
        while ((c = getchar()) != '\n' && c != EOF) {}
        return porDefecto;
    }
    return v;
}

static void imprimirIntegrantes(LogNodo& log) {
    log.escribir("============================================================");
    log.escribir("INTEGRANTES:");
    log.escribir("Gomez Ibarra Josue Alejandro");
    log.escribir("Munoz Madriz Nadya Janneth");
    log.escribir("============================================================");
}

int main(int argc, char** argv) {
    // Los hilos OpenMP de los clientes llaman a MPI (dentro de una seccion critica),
    // por eso se pide soporte de hilos.
    int provided = 0;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank, numProcesos;
    MPI_Comm_size(MPI_COMM_WORLD, &numProcesos);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    char nombreEquipo[MPI_MAX_PROCESSOR_NAME];
    int longitudNombre;
    MPI_Get_processor_name(nombreEquipo, &longitudNombre);

    LogNodo log(nombreEquipo, rank);

    if (rank == 0) imprimirIntegrantes(log);   // primera linea de la ejecucion

    log.escribir("[Equipo: %s] [Nodo MPI: %d] [Rol: %s] [Hilos OpenMP detectados: %d] [Nivel de hilos MPI: %d]",
                 nombreEquipo, rank, (rank == 0) ? "Servidor" : "Cliente",
                 omp_get_max_threads(), provided);

    if (provided < MPI_THREAD_SERIALIZED) {
        log.escribir("[Equipo: %s] [Nodo MPI: %d] ADVERTENCIA: MPI no ofrece soporte de hilos suficiente.",
                     nombreEquipo, rank);
    }

    int numClientes = numProcesos - 1;
    int opcion = -1;
    double saldo = SALDO_INICIAL;   // solo el servidor (nodo 0) la usa y la modifica

    if (numProcesos < 2) {
        if (rank == 0) log.escribir("Se necesitan al menos 2 procesos (1 servidor + 1 cliente). Use: mpiexec -n 5 Proyecto1.exe");
        opcion = 4;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    while (opcion != 4) {
        if (rank == 0) {
            log.escribir(" ");
            log.escribir("==== SISTEMA BANCARIO CLIENTE-SERVIDOR (MPI + OpenMP) ====");
            log.escribir("1. Iniciar simulacion basica (%d operaciones por cliente)", OPS_BASICA);
            log.escribir("2. Iniciar simulacion masiva (%d operaciones por cliente)", OPS_MASIVA);
            log.escribir("3. Consultar saldo final del servidor");
            log.escribir("4. Salir");
            printf("Opcion: ");
            opcion = leerEntero(0);
            log.escribir("Opcion seleccionada: %d", opcion);
        }
        MPI_Bcast(&opcion, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (opcion == 1 || opcion == 2) {
            int opsPorCliente = (opcion == 1) ? OPS_BASICA : OPS_MASIVA;
            bool detallado = (opcion == 1);   // en la masiva no se imprime operacion por operacion

            MPI_Barrier(MPI_COMM_WORLD);
            double t0 = MPI_Wtime();

            if (rank == 0) saldo = SALDO_INICIAL;
            double saldoInicial = saldo;
            double netoLocal = 0.0;
            ResultadoServidor res = {0, 0, 0, 0.0};

            if (rank == 0) res = ejecutarServidor(log, numClientes, opsPorCliente, saldo, detallado);
            else           netoLocal = ejecutarCliente(log, rank, opsPorCliente, detallado);

            // Cada cliente reporta lo que el vio; el servidor lo compara con su historial
            double netoClientes = 0.0;
            MPI_Reduce(&netoLocal, &netoClientes, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

            MPI_Barrier(MPI_COMM_WORLD);
            double tiempo = MPI_Wtime() - t0;

            if (rank == 0) {
                long long esperadas = (long long)numClientes * opsPorCliente;
                double saldoEsperado = saldoInicial + res.netoAprobado;
                bool conteoOk  = (res.procesadas == esperadas);
                bool saldoOk   = std::fabs(saldo - saldoEsperado) < 0.01;
                bool clientesOk = std::fabs(res.netoAprobado - netoClientes) < 0.01;

                log.escribir("---- RESUMEN DE LA SIMULACION ----");
                log.escribir("[Equipo: %s] [Servidor: Nodo 0] [Clientes: %d] [Operaciones por cliente: %d]",
                             nombreEquipo, numClientes, opsPorCliente);
                log.escribir("Transacciones procesadas: %lld (esperadas: %lld)", res.procesadas, esperadas);
                log.escribir("Aprobadas: %lld | Rechazadas: %lld", res.aprobadas, res.rechazadas);
                log.escribir("Saldo inicial: $%.2f | Saldo final consolidado: $%.2f", saldoInicial, saldo);
                log.escribir("Tiempo total (MPI_Wtime): %f s | Rendimiento: %.0f transacciones/s",
                             tiempo, (tiempo > 0.0) ? (double)res.procesadas / tiempo : 0.0);
                log.escribir("Verificacion: conteo %s | saldo = inicial + neto aprobado %s | neto servidor = neto clientes %s",
                             conteoOk ? "OK" : "ERROR", saldoOk ? "OK" : "ERROR", clientesOk ? "OK" : "ERROR");
                log.escribir("CONSISTENCIA DEL SALDO: %s",
                             (conteoOk && saldoOk && clientesOk) ? "CORRECTA (sin condiciones de carrera)" : "ERROR");
            } else {
                log.escribir("[Equipo: %s] [Nodo MPI: %d] [Operaciones enviadas: %d] [Neto aprobado visto por el cliente: $%.2f] [Tiempo total: %f s]",
                             nombreEquipo, rank, opsPorCliente, netoLocal, tiempo);
            }
        }
        else if (opcion == 3) {
            if (rank == 0) {
                log.escribir("[Equipo: %s] [Servidor: Nodo 0] [Operacion: Consulta de saldo final] [Saldo: $%.2f]",
                             nombreEquipo, saldo);
            }
        }
        else if (opcion != 4) {
            if (rank == 0) log.escribir("Opcion invalida.");
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) imprimirIntegrantes(log);   // ultima linea de la ejecucion

    log.cerrar();   // cerrar el log ANTES de MPI_Finalize
    MPI_Finalize();
    return 0;
}
