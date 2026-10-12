#ifndef BANCO_H
#define BANCO_H

#include "LogNodo.h"
#include "Transaccion.h"

/*
    Logica del sistema bancario Cliente-Servidor
    Gomez Ibarra Josue Alejandro
    Munoz Madriz Nadya Janneth
*/

struct ResultadoServidor {
    long long procesadas;    // transacciones recibidas y procesadas
    long long aprobadas;     // aprobadas (incluye consultas)
    long long rechazadas;    // rechazadas por fondos insuficientes
    double netoAprobado;     // depositos aprobados - retiros aprobados
};

// Nodo 0: bucle de recepcion (MPI_ANY_SOURCE). Termina al recibir TAG_FIN de todos los clientes.
// 'saldo' es la variable critica de la cuenta; solo este proceso la modifica.
ResultadoServidor ejecutarServidor(LogNodo& log, int numClientes, int opsPorCliente,
                                   double& saldo, bool detallado);

// Nodos 1..N-1: generan transacciones con OpenMP y las envian al servidor.
// Devuelve el neto aprobado segun lo que el propio cliente vio (para verificar consistencia).
double ejecutarCliente(LogNodo& log, int rank, int opsPorCliente, bool detallado);

#endif
