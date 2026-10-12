/*
    Programa principal - VERSION 2: MPI_Scatter / MPI_Gather
    Gomez Ibarra Josue Alejandro
    Munoz Madriz Nadya Janneth

    Realiza exactamente las mismas operaciones que la version con
    MPI_Send / MPI_Recv, pero sustituyendo la comunicacion por las
    primitivas colectivas MPI_Scatter y MPI_Gather, para poder comparar
    ambos mecanismos de comunicacion.

    Compilar (ejemplo con OpenMPI):
        mpic++ -fopenmp -o programa_scattergather main_scatter_gather.cpp OperacionesArreglos.cpp

    Ejecutar con 5 procesos (1 maestro + 4 trabajadores):
        mpirun -np 5 ./programa_scattergather
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

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, numProcesos;
    MPI_Comm_size(MPI_COMM_WORLD, &numProcesos);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    char nombreEquipo[MPI_MAX_PROCESSOR_NAME];
    int longitudNombre;
    MPI_Get_processor_name(nombreEquipo, &longitudNombre);

    if (rank == 0) {
        imprimirIntegrantes(); // primera instruccion de la ejecucion
    }

    printf("[Equipo: %s] [Proceso MPI: %d] listo (version Scatter/Gather).\n", nombreEquipo, rank);
    MPI_Barrier(MPI_COMM_WORLD);

    OperacionesArreglos ops(nombreEquipo, rank);

    int N = 0, localN = 0, opcion = -1;
    int *A = nullptr, *B = nullptr, *Resultado = nullptr;              // solo el maestro los mantiene completos
    int *A_local = nullptr, *B_local = nullptr, *R_local = nullptr;    // seccion de cada proceso

    do {
        if (rank == 0) {
            printf("\n==== OPERACIONES CON ARREGLOS DISTRIBUIDOS (MPI + OpenMP) ====\n");
            printf("Version: MPI_Scatter / MPI_Gather\n");
            printf("1. Crear arreglos\n");
            printf("2. Sumar arreglos\n");
            printf("3. Restar arreglos\n");
            printf("4. Multiplicar arreglos\n");
            printf("5. Calcular el cuadrado del arreglo A\n");
            printf("6. Salir\n");
            printf("Opcion: ");
            scanf("%d", &opcion);
        }
        MPI_Bcast(&opcion, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (opcion == 1) {
            int aleatorioInt = 0, detalladoInt = 0;
            if (rank == 0) {
                printf("Tamaño de los arreglos N (debe ser multiplo de %d): ", numProcesos);
                scanf("%d", &N);
                printf("Valores: 1 = aleatorios [1,1000000], 0 = consecutivos por proceso: ");
                scanf("%d", &aleatorioInt);
                printf("Mostrar mensajes detallados por elemento? 1 = Si, 0 = No: ");
                scanf("%d", &detalladoInt);
            }
            MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
            MPI_Bcast(&aleatorioInt, 1, MPI_INT, 0, MPI_COMM_WORLD);
            MPI_Bcast(&detalladoInt, 1, MPI_INT, 0, MPI_COMM_WORLD);
            bool aleatorio = (aleatorioInt == 1);
            bool detallado = (detalladoInt == 1);

            if (N % numProcesos != 0) {
                if (rank == 0) printf("N debe ser multiplo de %d. Intente de nuevo.\n", numProcesos);
                continue;
            }

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
            }

            // Se reunen las secciones generadas por cada proceso en el maestro
            MPI_Gather(A_local, localN, MPI_INT, A, localN, MPI_INT, 0, MPI_COMM_WORLD);
            MPI_Gather(B_local, localN, MPI_INT, B, localN, MPI_INT, 0, MPI_COMM_WORLD);

            if (rank == 0) {
                printf("[Equipo: %s] [Proceso MPI: 0] Arreglos A y B creados y reunidos (N = %d).\n", nombreEquipo, N);
            }
        }
        else if (opcion >= 2 && opcion <= 5) {
            if (A == nullptr && rank == 0) {
                printf("Primero debe crear los arreglos (opcion 1).\n");
            } else if (A_local == nullptr) {
                // los procesos trabajadores tambien deben validar; no hacen nada si no hay datos
            } else {
                int detalladoInt = 0;
                if (rank == 0) {
                    printf("Mostrar mensajes detallados por elemento? 1 = Si, 0 = No: ");
                    scanf("%d", &detalladoInt);
                }
                MPI_Bcast(&detalladoInt, 1, MPI_INT, 0, MPI_COMM_WORLD);
                bool detallado = (detalladoInt == 1);

                MPI_Barrier(MPI_COMM_WORLD);
                double tInicio = MPI_Wtime();

                // El maestro reparte las secciones de A (y B, si aplica) con Scatter
                MPI_Scatter(A, localN, MPI_INT, A_local, localN, MPI_INT, 0, MPI_COMM_WORLD);
                if (opcion != 5)
                    MPI_Scatter(B, localN, MPI_INT, B_local, localN, MPI_INT, 0, MPI_COMM_WORLD);

                int offsetGlobal = rank * localN;
                switch (opcion) {
                    case 2: ops.sumar(A_local, B_local, R_local, localN, offsetGlobal, detallado); break;
                    case 3: ops.restar(A_local, B_local, R_local, localN, offsetGlobal, detallado); break;
                    case 4: ops.multiplicar(A_local, B_local, R_local, localN, offsetGlobal, detallado); break;
                    case 5: ops.cuadrado(A_local, R_local, localN, offsetGlobal, detallado); break;
                }

                // El maestro reune los resultados parciales con Gather
                MPI_Gather(R_local, localN, MPI_INT, Resultado, localN, MPI_INT, 0, MPI_COMM_WORLD);

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

    } while (opcion != 6);

    delete[] A_local; delete[] B_local; delete[] R_local;
    delete[] A; delete[] B; delete[] Resultado;

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        imprimirIntegrantes(); // ultima instruccion de la ejecucion
    }

    MPI_Finalize();
    return 0;
}
