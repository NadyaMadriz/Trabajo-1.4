#include "LogNodo.h"
#include <cstdio>
#include <cstring>
#include <cstdarg>

LogNodo::LogNodo(const char* nombreEquipo, int rankMPI) {
    std::strncpy(equipo, nombreEquipo, sizeof(equipo) - 1);
    equipo[sizeof(equipo) - 1] = '\0';
    rank = rankMPI;

    char nombreArchivo[400];
    std::snprintf(nombreArchivo, sizeof(nombreArchivo),
                  "log_equipo_%s_nodo_%d.txt", equipo, rank);
    archivo.open(nombreArchivo, std::ios::out | std::ios::trunc);
}

LogNodo::~LogNodo() {
    cerrar();
}

void LogNodo::escribir(const char* formato, ...) {
    char buffer[2048];
    va_list args;
    va_start(args, formato);
    std::vsnprintf(buffer, sizeof(buffer), formato, args);
    va_end(args);

    // Se agrega el salto de linea al final para escribir la linea completa de una sola vez
    std::strncat(buffer, "\n", sizeof(buffer) - std::strlen(buffer) - 1);

    #pragma omp critical(log_nodo)
    {
        std::fputs(buffer, stdout);
        std::fflush(stdout);
        if (archivo.is_open()) archivo << buffer;
    }
}

void LogNodo::cerrar() {
    if (archivo.is_open()) {
        archivo.flush();
        archivo.close();
    }
}
