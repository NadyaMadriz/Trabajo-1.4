#ifndef OPERACIONESARREGLOS_H
#define OPERACIONESARREGLOS_H

#include <fstream>

class OperacionesArreglos {
private:
    char equipo[256];
    int procesoMPI;
    std::ofstream log;

public:
    OperacionesArreglos(const char* nombreEquipo, int rank);
    ~OperacionesArreglos();

    // Imprime en pantalla y escribe en el log del nodo
    void registrar(const char* mensaje);

    // Cierra el log de forma segura (llamar antes de MPI_Finalize)
    void cerrarLog();

    void crearArregloMPI(int* seccion, int cantidad, int offsetGlobal,
                         bool aleatorio, int valorMax, bool detallado);

    void llenarSecuencial(int* seccion, int cantidad, int offsetGlobal,
                          bool detallado);

    void llenarAleatorio(int* seccion, int cantidad, int offsetGlobal,
                         bool detallado);

    void sumar(int* a, int* b, int* resultado, int cantidad,
               int offsetGlobal, bool detallado);

    void restar(int* a, int* b, int* resultado, int cantidad,
                int offsetGlobal, bool detallado);

    void multiplicar(int* a, int* b, int* resultado, int cantidad,
                     int offsetGlobal, bool detallado);

    void cuadrado(int* a, int* resultado, int cantidad,
                  int offsetGlobal, bool detallado);

    long long sumatoriaLocal(int* arreglo, int cantidad);
    int maximoLocal(int* arreglo, int cantidad);
    int minimoLocal(int* arreglo, int cantidad);
};

#endif
