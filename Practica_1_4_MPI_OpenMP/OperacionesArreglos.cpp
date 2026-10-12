#include "OperacionesArreglos.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <climits>
#include <omp.h>

OperacionesArreglos::OperacionesArreglos(const char* nombreEquipo, int rank) {
    std::strncpy(equipo, nombreEquipo, sizeof(equipo) - 1);
    equipo[sizeof(equipo) - 1] = '\0';
    procesoMPI = rank;

    char nombreArchivo[400];
    std::snprintf(nombreArchivo, sizeof(nombreArchivo),
                  "log_equipo_%s_nodo_%d.txt", equipo, procesoMPI);
    log.open(nombreArchivo, std::ios::out | std::ios::trunc);
}

OperacionesArreglos::~OperacionesArreglos() {
    cerrarLog();
}

void OperacionesArreglos::cerrarLog() {
    if (log.is_open()) {
        log.flush();
        log.close();
    }
}

// Imprime en pantalla Y escribe en el log del nodo (seguro entre hilos)
void OperacionesArreglos::registrar(const char* mensaje) {
    #pragma omp critical(registro_log)
    {
        std::printf("%s\n", mensaje);
        std::fflush(stdout);
        if (log.is_open()) log << mensaje << "\n";
    }
}

// ---------- Llenado ----------

void OperacionesArreglos::llenarSecuencial(int* seccion, int cantidad,
                                           int offsetGlobal, bool detallado) {
    #pragma omp parallel for
    for (int i = 0; i < cantidad; i++) {
        int valor = offsetGlobal + i + 1;   // consecutivo: 1, 2, 3, ... N
        seccion[i] = valor;
        if (detallado) {
            char msg[512];
            std::snprintf(msg, sizeof(msg),
                "[Equipo: %s] [Proceso MPI: %d] [Hilo OpenMP: %d] [Posicion: %d] "
                "[Operacion: Llenar Secuencial] [Valor: %d]",
                equipo, procesoMPI, omp_get_thread_num(), offsetGlobal + i, valor);
            registrar(msg);
        }
    }
}

void OperacionesArreglos::llenarAleatorio(int* seccion, int cantidad,
                                          int offsetGlobal, bool detallado) {
    static unsigned int llamadas = 0;   // para que A y B no salgan iguales
    llamadas++;

    // Semilla propia de cada nodo: tiempo + rank MPI
    srand((unsigned int)time(NULL) + (unsigned int)procesoMPI * 7919u + llamadas * 104729u);
    unsigned int base = (unsigned int)rand();

    #pragma omp parallel
    {
        // Cada hilo con su propia secuencia (rand() no es seguro entre hilos)
        unsigned int estado = base + (unsigned int)omp_get_thread_num() * 2654435761u;

        #pragma omp for
        for (int i = 0; i < cantidad; i++) {
            estado = estado * 1103515245u + 12345u;
            int valor = 1 + (int)((estado >> 8) % 1000000u);
            seccion[i] = valor;
            if (detallado) {
                char msg[512];
                std::snprintf(msg, sizeof(msg),
                    "[Equipo: %s] [Proceso MPI: %d] [Hilo OpenMP: %d] [Posicion: %d] "
                    "[Operacion: Llenar Aleatorio] [Valor: %d]",
                    equipo, procesoMPI, omp_get_thread_num(), offsetGlobal + i, valor);
                registrar(msg);
            }
        }
    }
}

void OperacionesArreglos::crearArregloMPI(int* seccion, int cantidad, int offsetGlobal,
                                          bool aleatorio, int valorMax, bool detallado) {
    (void)valorMax; // el rango fijo es 1..1,000,000
    if (aleatorio) llenarAleatorio(seccion, cantidad, offsetGlobal, detallado);
    else           llenarSecuencial(seccion, cantidad, offsetGlobal, detallado);
}

// ---------- Aritméticas elemento a elemento ----------

#define MSG_ELEMENTO(nombreOp, valor)                                              \
    {                                                                              \
        char msg[512];                                                             \
        std::snprintf(msg, sizeof(msg),                                            \
            "[Equipo: %s] [Proceso MPI: %d] [Hilo OpenMP: %d] [Posicion: %d] "     \
            "[Operacion: %s] [Valor: %d]",                                         \
            equipo, procesoMPI, omp_get_thread_num(), offsetGlobal + i,            \
            nombreOp, (int)(valor));                                               \
        registrar(msg);                                                            \
    }

void OperacionesArreglos::sumar(int* a, int* b, int* resultado, int cantidad,
                                int offsetGlobal, bool detallado) {
    #pragma omp parallel for
    for (int i = 0; i < cantidad; i++) {
        resultado[i] = a[i] + b[i];
        if (detallado) MSG_ELEMENTO("Suma", resultado[i])
    }
}

void OperacionesArreglos::restar(int* a, int* b, int* resultado, int cantidad,
                                 int offsetGlobal, bool detallado) {
    #pragma omp parallel for
    for (int i = 0; i < cantidad; i++) {
        resultado[i] = a[i] - b[i];
        if (detallado) MSG_ELEMENTO("Resta", resultado[i])
    }
}

void OperacionesArreglos::multiplicar(int* a, int* b, int* resultado, int cantidad,
                                      int offsetGlobal, bool detallado) {
    #pragma omp parallel for
    for (int i = 0; i < cantidad; i++) {
        resultado[i] = a[i] * b[i];
        if (detallado) MSG_ELEMENTO("Multiplicacion", resultado[i])
    }
}

void OperacionesArreglos::cuadrado(int* a, int* resultado, int cantidad,
                                   int offsetGlobal, bool detallado) {
    #pragma omp parallel for
    for (int i = 0; i < cantidad; i++) {
        resultado[i] = a[i] * a[i];
        if (detallado) MSG_ELEMENTO("Cuadrado", resultado[i])
    }
}

// ---------- Reducciones locales (nivel 1: OpenMP) ----------

long long OperacionesArreglos::sumatoriaLocal(int* arreglo, int cantidad) {
    long long suma_local = 0;
    #pragma omp parallel for reduction(+:suma_local)
    for (int i = 0; i < cantidad; i++) suma_local += arreglo[i];
    return suma_local;
}

int OperacionesArreglos::maximoLocal(int* arreglo, int cantidad) {
    int max_local = INT_MIN;
    #pragma omp parallel for reduction(max:max_local)
    for (int i = 0; i < cantidad; i++)
        if (arreglo[i] > max_local) max_local = arreglo[i];
    return max_local;
}

int OperacionesArreglos::minimoLocal(int* arreglo, int cantidad) {
    int min_local = INT_MAX;
    #pragma omp parallel for reduction(min:min_local)
    for (int i = 0; i < cantidad; i++)
        if (arreglo[i] < min_local) min_local = arreglo[i];
    return min_local;
}
