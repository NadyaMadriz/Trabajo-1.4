#ifndef LOGNODO_H
#define LOGNODO_H

#include <fstream>

/*
    Log local de cada nodo: toda impresion se escribe en pantalla Y en
    log_equipo_[NOMBRE_PC]_nodo_[RANK].txt. Es seguro entre hilos OpenMP.
*/
class LogNodo {
private:
    char equipo[256];
    int rank;
    std::ofstream archivo;

public:
    LogNodo(const char* nombreEquipo, int rank);
    ~LogNodo();

    const char* getEquipo() const { return equipo; }
    int getRank() const { return rank; }

    // Igual que printf: imprime en consola y escribe la misma linea en el log
    void escribir(const char* formato, ...);

    // Cierra el archivo de forma segura (llamar ANTES de MPI_Finalize)
    void cerrar();
};

#endif
