
import logging
import socket

from protocol.protocol import Protocol

class Client:
    def __init__(self, port, ip):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((ip, port))

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        
        protocol = Protocol(self._socket)
        bets = "blablabla"

        protocol.send_bet(bets)

        ack = protocol.receive_ack()

        if ack: #revisar logs
            logging.info(f'action: send_bet | result: success | bet: {bets}')
        else:
            logging.info(f'action: send_bet | result: fail | bet: {bets}')

        protocol.close_connection()
