
import logging
import os
import socket

from protocol.protocol import Protocol

class Client:
    def __init__(self, port, ip):
        self._agencia = os.getenv('CLI_ID', "")
        self._nombre = os.getenv('NOMBRE', "")
        self._apellido = os.getenv('APELLIDO', "")
        self._documento = os.getenv('DOCUMENTO', "")
        self._nacimiento = os.getenv('NACIMIENTO', "")
        self._numero = os.getenv('NUMERO', "")

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((ip, port))

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self.close_connection()
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        try:
            protocol = Protocol(self._socket)
            bet = self._get_bet()
            protocol.send_bets(bet)

            ack = protocol.receive_ack()
            if ack:
                logging.info(f'action: apuesta_enviada | result: success | dni: {self._documento} | numero: {self._numero}')
            else:
                logging.info(f'action: apuesta_enviada | result: fail | dni: {self._documento} | numero: {self._numero}')
        except (OSError, Exception) as e:
            logging.error("action: send_bets | result: fail | error: {e}")
        finally:
            self._close_connection()

    def _get_bet(self):
        return self._agencia + "," + self._nombre + "," + self._apellido + "," + self._documento + "," + self._nacimiento + "," + self._numero

    def _close_connection(self):
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
