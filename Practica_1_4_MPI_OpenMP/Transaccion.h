#ifndef TRANSACCION_H
#define TRANSACCION_H

/*
    Estructuras de intercambio de mensajes Cliente <-> Servidor
    Gomez Ibarra Josue Alejandro
    Munoz Madriz Nadya Janneth
*/

// Etiquetas (tags) de los mensajes MPI
#define TAG_TRANS 1    // cliente -> servidor: transaccion
#define TAG_RESP  2    // servidor -> cliente: respuesta
#define TAG_FIN   99   // cliente -> servidor: ya no enviare mas

// Tipos de operacion
#define OP_FINALIZAR 0
#define OP_DEPOSITO  1
#define OP_RETIRO    2
#define OP_CONSULTA  3

struct Transaccion {
    int id_cliente;        // Rank del proceso MPI cliente
    int id_hilo_openmp;    // Hilo de OpenMP que origino la solicitud
    int tipo_operacion;    // 1: Deposito, 2: Retiro, 3: Consulta, 0: Finalizar
    double monto;          // Monto a depositar o retirar
};

struct RespuestaServidor {
    bool aprobada;             // true si la transaccion fue exitosa
    double saldo_resultante;   // Saldo actualizado tras la operacion
    char mensaje[100];         // Mensaje descriptivo del servidor
};

// Registro que el servidor guarda de cada transaccion procesada (historial dinamico)
struct RegistroServidor {
    Transaccion t;
    bool aprobada;
    double saldo;
};

inline const char* nombreOperacion(int tipo) {
    switch (tipo) {
        case OP_DEPOSITO: return "Deposito";
        case OP_RETIRO:   return "Retiro";
        case OP_CONSULTA: return "Consulta";
        default:          return "Finalizar";
    }
}

#endif
