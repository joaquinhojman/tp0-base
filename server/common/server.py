import socket
import logging

from protocol.protocol import Protocol

from common.utils import store_bets, parse_client_bets, load_bets, has_won

class Server:
    def __init__(self, port, listen_backlog, agencias):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._sigterm_received = False
        self._agencias = agencias
        self._remaining_eofs = agencias

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._sigterm_received = True
        self._server_socket.shutdown(socket.SHUT_RDWR)
        self._server_socket.close()
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        while not self._sigterm_received and self._remaining_eofs > 0:
            client_sock = self.__accept_new_connection()
            if client_sock is None: break
            eof = self.__handle_client_connection(client_sock)
            self._remaining_eofs -= 1 if eof else 0

        logging.info(f'action: sorteo | result: success')
        winners = self._verify_winners()
        

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        eof = False
        try:
            protocol = Protocol(client_sock)
            msg, eof = protocol.receive()
            
            bets = parse_client_bets(msg)
            store_bets(bets)
            
            protocol.send_ack(True)
        except OSError as e:
            logging.error(f'action: receive_message | result: fail | error: {e}')
        except Exception as e:
            try:
                protocol.send_ack(False)
            except OSError as e:
                logging.error(f'action: receive_message | result: fail | error: {e}')
            logging.error(f'action: apuesta_almacenada | result: fail | error: {e}')
        finally:
            self._close_client_connection(client_sock)
            return eof

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info(f'action: accept_connections | result: in_progress')
        try:      
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            return None

    def _close_client_connection(self, client_socket):
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()

    def _verify_winners(self):
        '''
        Return a dictionary with the winners of the bets. Agency as key, document winners list as value.
        '''
        winners = {}
        for i in range(1, self._agencias + 1):
            winners[i] = []
        bets = load_bets()
        for bet in bets:
            if has_won(bet):
                winners[bet.agency].append(bet.document)
        return winners
