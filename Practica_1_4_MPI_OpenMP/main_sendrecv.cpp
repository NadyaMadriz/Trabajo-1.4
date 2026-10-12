/*
    Programa principal - VERSION 1: MPI_Send / MPI_Recv
    Gomez Ibarra Josue Alejandro
    Munoz Madriz Nadya Janneth
*/

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mpi.h>
#include <omp.h>
#include "OperacionesArreglos.h"

void imprimirIntegrantes() {
    printf("============================================================\n");
    printf("INTEGRANTES:\n");
    printf("Gomez Ibarra Josue Alejandro\n");
    printf("Munoz Madriz Nadya Janneth\n");
    printf("============================================================\n\n");
}

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

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    setvbuf(stdout, NULL, _IONBF, 0);   // salida sin buffer: se ve al instante

    int rank, numProcesos;
    MPI_Comm_size(MPI_COMM_WORLD, &numProcesos);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    char nombreEquipo[MPI_MAX_PROCESSOR_NAME];
    int longitudNombre;
    MPI_Get_processor_name(nombreEquipo, &longitudNombre);

    if (rank == 0) {
        imprimirIntegrantes(); // primera instruccion de la ejecucion
    }

    printf("[Equipo: %s] [Proceso MPI: %d] listo (version Send/Recv).\n", nombreEquipo, rank);
    MPI_Barrier(MPI_COMM_WORLD);

    OperacionesArreglos ops(nombreEquipo, rank);

    int N = 0, localN = 0, opcion = -1;
    int *A = nullptr, *B = nullptr, *Resultado = nullptr;              // solo el maestro los mantiene completos
    int *A_local = nullptr, *B_local = nullptr, *R_local = nullptr;    // seccion de cada proceso

    do {
        if (rank == 0) {
            printf("\n==== OPERACIONES CON ARREGLOS DISTRIBUIDOS (MPI + OpenMP) ====\n");
            printf("Version: MPI_Send / MPI_Recv\n");
            printf("1. Crear arreglos\n");
            printf("2. Sumar arreglos\n");
            printf("3. Restar arreglos\n");
            printf("4. Multiplicar arreglos\n");
            printf("5. Calcular el cuadrado del arreglo A\n");
            printf("6. Salir\n");
            printf("Opcion: ");
            opcion = leerEntero(0);
        }
        MPI_Bcast(&opcion, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (opcion == 1) {
            int aleatorioInt = 0, detalladoInt = 0;
            if (rank == 0) {
                printf("Tamano de los arreglos N (debe ser multiplo de %d): ", numProcesos);
                N = leerEntero(0);
                printf("Valores: 1 = aleatorios [1,1000000], 0 = consecutivos: ");
                aleatorioInt = leerEntero(0);
                printf("Mostrar mensajes detallados por elemento? 1 = Si, 0 = No: ");
                detalladoInt = leerEntero(0);
            }
            MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
            MPI_Bcast(&aleatorioInt, 1, MPI_INT, 0, MPI_COMM_WORLD);
            MPI_Bcast(&detalladoInt, 1, MPI_INT, 0, MPI_COMM_WORLD);
            bool aleatorio = (aleatorioInt == 1);
            bool detallado = (detalladoInt == 1);

            if (N <= 0 || N % numProcesos != 0) {
                if (rank == 0) printf("N debe ser positivo y multiplo de %d. Intente de nuevo.\n", numProcesos);
                continue;
            }

            // liberar arreglos previos si existian
            delete[] A_local; delete[] B_local; delete[] R_local;
            delete[] A; delete[] B; delete[] Resultado;
            A = B = Resultado = nullptr;

            localN = N / numProcesos;
            int offsetGlobal = rank * localN;

            A_local = new int[localN];
            B_local = new int[localN];
            R_local = new int[localN];

            // Cada proceso genera su propia seccion (usando OpenMP internamente)
            ops.crearArregloMPI(A_local, localN, offsetGlobal, aleatorio, 1000000, detallado);
            ops.crearArregloMPI(B_local, localN, offsetGlobal, aleatorio, 1000000, detallado);

            if (rank == 0) {
                A = new int[N];
                B = new int[N];
                Resultado = new int[N];
                for (int i = 0; i < localN; i++) { A[i] = A_local[i]; B[i] = B_local[i]; }
                for (int p = 1; p < numProcesos; p++) {
                    MPI_Recv(A + p * localN, localN, MPI_INT, p, 100, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    MPI_Recv(B + p * localN, localN, MPI_INT, p, 101, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }
                printf("[Equipo: %s] [Proceso MPI: 0] Arreglos A y B creados y reunidos (N = %d).\n", nombreEquipo, N);
            } else {
                MPI_Send(A_local, localN, MPI_INT, 0, 100, MPI_COMM_WORLD);
                MPI_Send(B_local, localN, MPI_INT, 0, 101, MPI_COMM_WORLD);
            }
        }
        else if (opcion >= 2 && opcion <= 5) {
            if (A_local == nullptr) {
                if (rank == 0) printf("Primero debe crear los arreglos (opcion 1).\n");
            } else {
                int detalladoInt = 0;
                if (rank == 0) {
                    printf("Mostrar mensajes detallados por elemento? 1 = Si, 0 = No: ");
                    detalladoInt = leerEntero(0);
                }
                MPI_Bcast(&detalladoInt, 1, MPI_INT, 0, MPI_COMM_WORLD);
                bool detallado = (detalladoInt == 1);

                MPI_Barrier(MPI_COMM_WORLD);
                double tInicio = MPI_Wtime();

                // El maestro distribuye las secciones de A (y B, si aplica) a cada trabajador
                if (rank == 0) {
                    for (int i = 0; i < localN; i++) {
                        A_local[i] = A[i];
                        if (opcion != 5) B_local[i] = B[i];
                    }
                    for (int p = 1; p < numProcesos; p++) {
                        MPI_Send(A + p * localN, localN, MPI_INT, p, 200, MPI_COMM_WORLD);
                        if (opcion != 5)
                            MPI_Send(B + p * localN, localN, MPI_INT, p, 201, MPI_COMM_WORLD);
                    }
                } else {
                    MPI_Recv(A_local, localN, MPI_INT, 0, 200, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    if (opcion != 5)
                        MPI_Recv(B_local, localN, MPI_INT, 0, 201, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }

                int offsetGlobal = rank * localN;
                switch (opcion) {
                    case 2: ops.sumar(A_local, B_local, R_local, localN, offsetGlobal, detallado); break;
                    case 3: ops.restar(A_local, B_local, R_local, localN, offsetGlobal, detallado); break;
                    case 4: ops.multiplicar(A_local, B_local, R_local, localN, offsetGlobal, detallado); break;
                    case 5: ops.cuadrado(A_local, R_local, localN, offsetGlobal, detallado); break;
                }

                // El maestro reune los resultados parciales
                if (rank == 0) {
                    for (int i = 0; i < localN; i++) Resultado[i] = R_local[i];
                    for (int p = 1; p < numProcesos; p++) {
                        MPI_Recv(Resultado + p * localN, localN, MPI_INT, p, 300, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    }
                } else {
                    MPI_Send(R_local, localN, MPI_INT, 0, 300, MPI_COMM_WORLD);
                }

                double tFin = MPI_Wtime();

                if (rank == 0) {
                    const char* nombreOp = (opcion == 2) ? "Suma" : (opcion == 3) ? "Resta"
                                          : (opcion == 4) ? "Multiplicacion" : "Cuadrado";
                    printf("\n--- Resultado: %s ---\n", nombreOp);
                    if (N <= 40) {
                        printf("Arreglo resultado: ");
                        for (int i = 0; i < N; i++) printf("%d ", Resultado[i]);
                        printf("\n");
                    }
                    printf("Tiempo de ejecucion: %f segundos\n", tFin - tInicio);
                }
            }
        }
        else if (opcion != 6) {
            if (rank == 0) printf("Opcion invalida.\n");
        }

    } while (opcion != 6);

    delete[] A_local; delete[] B_local; delete[] R_local;
    delete[] A; delete[] B; delete[] Resultado;

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        imprimirIntegrantes(); // ultima instruccion de la ejecucion
    }

    ops.cerrarLog();           // cerrar log ANTES de MPI_Finalize
    MPI_Finalize();
    return 0;
}
