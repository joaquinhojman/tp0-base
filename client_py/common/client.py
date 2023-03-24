
import logging
import os
import socket

from protocol.protocol import Protocol

class Client:
    def __init__(self, port, ip, bets_per_batch):
        self._agencia = os.getenv('CLI_ID', "")
        self._bets_file = os.getenv('BETS_FILE', "")
        self._bets_per_batch = bets_per_batch
        
        self._bets_readed = 0
        self._f = open(self._bets_file, 'r')

        self._ip = ip
        self._port = port
        self._socket = None
        
    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self.close_connection()
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        eof = False
        try:
            while not eof:    
                bets, eof = self._get_bets()            
                
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.connect((self._ip, self._port))

                protocol = Protocol(self._socket)
                protocol.send_bets(bets, eof)

                ack = protocol.receive_ack()
                if ack:
                    logging.info(f'action: apuesta_enviada | result: success')
                else:
                    logging.info(f'action: apuesta_enviada | result: fail')
        except (OSError, Exception) as e:
            logging.error(f'action: send_bets | result: fail | error: {e}')
        finally:
            self._close_connection()

    def _get_bets(self):
        bets = []
        eof = False
        for _i in range(self._bets_per_batch):
            line = self._f.readline()
            if not line: #End of file?
                eof = True
                break
            line = self._agencia + "," + line.rstrip()
            bets.append(line)
        
        if eof == False: # could happen that next line is end of file
            x = self._f.tell()
            line = self._f.readline()
            self._f.seek(x) #return to previous position
            if not line:
                eof = True

        return ";".join(bets), eof

    def _close_connection(self):
        self._f.close()
        self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()