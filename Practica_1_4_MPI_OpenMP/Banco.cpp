#include "Banco.h"
#include <mpi.h>
#include <omp.h>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <ctime>

// Generador congruencial simple, uno por hilo (rand() no es seguro entre hilos)
static unsigned int siguiente(unsigned int& estado) {
    estado = estado * 1103515245u + 12345u;
    return estado >> 8;
}

// ============================================================
//  SERVIDOR (nodo 0)
//  La exclusion mutua sobre 'saldo' la da el propio bucle: el servidor
//  atiende UNA solicitud a la vez (MPI_Recv -> procesar -> MPI_Send).
// ============================================================
ResultadoServidor ejecutarServidor(LogNodo& log, int numClientes, int opsPorCliente,
                                   double& saldo, bool detallado) {
    ResultadoServidor res = {0, 0, 0, 0.0};

    // Historial dinamico de transacciones procesadas
    long long total = (long long)numClientes * opsPorCliente;
    RegistroServidor* historial = new RegistroServidor[total > 0 ? total : 1];

    int finalizados = 0;
    MPI_Status st;
    Transaccion t;
    RespuestaServidor resp;

    while (finalizados < numClientes) {
        MPI_Recv(&t, (int)sizeof(Transaccion), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG,
                 MPI_COMM_WORLD, &st);

        if (st.MPI_TAG == TAG_FIN) {   // un cliente termino
            finalizados++;
            continue;
        }

        std::memset(&resp, 0, sizeof(resp));
        if (t.tipo_operacion == OP_DEPOSITO) {
            saldo += t.monto;
            resp.aprobada = true;
            std::snprintf(resp.mensaje, sizeof(resp.mensaje), "Deposito aprobado");
        } else if (t.tipo_operacion == OP_RETIRO) {
            if (saldo >= t.monto) {
                saldo -= t.monto;
                resp.aprobada = true;
                std::snprintf(resp.mensaje, sizeof(resp.mensaje), "Retiro aprobado");
            } else {
                resp.aprobada = false;
                std::snprintf(resp.mensaje, sizeof(resp.mensaje), "Rechazado: fondos insuficientes");
            }
        } else {   // consulta
            resp.aprobada = true;
            std::snprintf(resp.mensaje, sizeof(resp.mensaje), "Consulta realizada");
        }
        resp.saldo_resultante = saldo;

        MPI_Send(&resp, (int)sizeof(RespuestaServidor), MPI_BYTE, st.MPI_SOURCE,
                 TAG_RESP, MPI_COMM_WORLD);

        if (res.procesadas < total) {
            historial[res.procesadas].t = t;
            historial[res.procesadas].aprobada = resp.aprobada;
            historial[res.procesadas].saldo = saldo;
        }
        res.procesadas++;

        if (detallado) {
            log.escribir("[Equipo: %s] [Proceso MPI: 0 (Servidor)] [Cliente: %d] [Hilo OpenMP: %d] "
                         "[Operacion: %s] [Monto: $%.2f] [Estado: %s] [Saldo: $%.2f]",
                         log.getEquipo(), t.id_cliente, t.id_hilo_openmp,
                         nombreOperacion(t.tipo_operacion), t.monto,
                         resp.aprobada ? "Aprobado" : "Rechazado", saldo);
        }
    }

    // Estadisticas calculadas a partir del historial
    long long n = (res.procesadas < total) ? res.procesadas : total;
    for (long long i = 0; i < n; i++) {
        if (historial[i].aprobada) {
            res.aprobadas++;
            if (historial[i].t.tipo_operacion == OP_DEPOSITO) res.netoAprobado += historial[i].t.monto;
            else if (historial[i].t.tipo_operacion == OP_RETIRO) res.netoAprobado -= historial[i].t.monto;
        } else {
            res.rechazadas++;
        }
    }

    delete[] historial;
    return res;
}

// ============================================================
//  CLIENTE (nodos 1..N-1)
//  Los hilos OpenMP generan las transacciones en paralelo. Como cada
//  cliente solo debe tener UNA peticion en vuelo (para que cada hilo
//  reciba SU respuesta), el par Send/Recv va dentro de una seccion critica.
// ============================================================
double ejecutarCliente(LogNodo& log, int rank, int opsPorCliente, bool detallado) {
    // Historiales dinamicos (prohibido usar arreglos estaticos o std::vector)
    Transaccion* historial = new Transaccion[opsPorCliente];
    RespuestaServidor* respuestas = new RespuestaServidor[opsPorCliente];

    // Semilla unica por nodo: tiempo + rank
    std::srand((unsigned int)std::time(NULL) + (unsigned int)rank * 7919u);
    unsigned int base = (unsigned int)std::rand();

    double neto = 0.0;

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        unsigned int estado = base + (unsigned int)tid * 2654435761u + 1u;

        #pragma omp for schedule(static) reduction(+:neto)
        for (int i = 0; i < opsPorCliente; i++) {
            // --- generar la transaccion ---
            Transaccion t;
            t.id_cliente = rank;
            t.id_hilo_openmp = tid;
            unsigned int r = siguiente(estado) % 100;
            if (r < 40)      t.tipo_operacion = OP_DEPOSITO;   // 40%
            else if (r < 75) t.tipo_operacion = OP_RETIRO;     // 35%
            else             t.tipo_operacion = OP_CONSULTA;   // 25%
            t.monto = (t.tipo_operacion == OP_CONSULTA)
                      ? 0.0
                      : (double)(siguiente(estado) % 100000u + 100u) / 100.0;   // 1.00 a 1000.99
            historial[i] = t;

            // --- enviar y esperar la confirmacion sincrona ---
            RespuestaServidor resp;
            #pragma omp critical(mpi_cliente)
            {
                MPI_Send(&t, (int)sizeof(Transaccion), MPI_BYTE, 0, TAG_TRANS, MPI_COMM_WORLD);
                MPI_Recv(&resp, (int)sizeof(RespuestaServidor), MPI_BYTE, 0, TAG_RESP,
                         MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            respuestas[i] = resp;

            if (resp.aprobada) {
                if (t.tipo_operacion == OP_DEPOSITO) neto += t.monto;
                else if (t.tipo_operacion == OP_RETIRO) neto -= t.monto;
            }

            if (detallado) {
                log.escribir("[Equipo: %s] [Proceso MPI: %d] [Hilo OpenMP: %d] [Operacion: %s] "
                             "[Monto: $%.2f] [Estado: %s] [Saldo Final: $%.2f]",
                             log.getEquipo(), rank, tid, nombreOperacion(t.tipo_operacion),
                             t.monto, resp.aprobada ? "Aprobado" : "Rechazado",
                             resp.saldo_resultante);
            }
        }
    }

    // Avisar al servidor que este cliente termino
    Transaccion fin;
    fin.id_cliente = rank;
    fin.id_hilo_openmp = 0;
    fin.tipo_operacion = OP_FINALIZAR;
    fin.monto = 0.0;
    MPI_Send(&fin, (int)sizeof(Transaccion), MPI_BYTE, 0, TAG_FIN, MPI_COMM_WORLD);

    delete[] historial;
    delete[] respuestas;
    return neto;
}
