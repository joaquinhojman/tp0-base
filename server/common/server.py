import socket
import logging
import multiprocessing
from time import sleep

from protocol.protocol import Protocol

from common.utils import store_bets, parse_client_bets, load_bets, has_won

class Server:
    def __init__(self, port, port_results, listen_backlog, agencias, timeout):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)

        self._server_socket_results = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket_results.bind(('', port_results))
        self._server_socket_results.listen(listen_backlog)

        self._sigterm_received = False
        self._timeout = timeout
        self._agencias = agencias

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info(f'action: Handle SIGTERM | result: in_progress')
        self._sigterm_received = True
        try:
            self._server_socket.close()
        except:
            pass
        try:
            self._server_socket_results.close()
        except:
            pass
        logging.info(f'action: Handle SIGTERM | result: success')

    def run(self):
        try:
            while not self._sigterm_received:
                self._run()
        except Exception as e:
            logging.error(f'action: run | result: fail | error: {e}')
            self._sigterm_handler()
            return

    def _run(self):
        file_lock = multiprocessing.Lock()
        barrier = multiprocessing.Barrier(self._agencias + 1) # +1 for the main process

        #phase 1: recv bets
        proccesses = []
        while len(proccesses) != self._agencias:
            client_sock = self.__accept_new_connection(self._server_socket)
            if client_sock is None: break
            p = multiprocessing.Process(target=self._handle_client_connection_phase_1, args=(client_sock, file_lock, barrier))
            proccesses.append(p.start()) 
        
        if len(proccesses) != self._agencias: #something went wrong
            logging.error(f'action: recv bets | result: fail')
            self._join_proccesses(proccesses)
            return
        
        try:
            barrier.wait(timeout=self._timeout) # wait for all clients send their bets
        except Exception as e:
            logging.error(f'action: barrier | result: fail | error: {e}')
            self._join_proccesses(proccesses)
            return

        self._join_proccesses(proccesses)
        logging.info(f'action: sorteo | result: success')
        winners = self._verify_winners()

        #phase 2: send results
        proccesses = []
        while len(proccesses) != self._agencias:
            client_sock = self.__accept_new_connection(self._server_socket_results)
            if client_sock is None: break
            p = multiprocessing.Process(target=self._handle_client_connection_phase_2, args=(client_sock, winners))
            proccesses.append(p.start())
        self._join_proccesses(proccesses)

    def _handle_client_connection_phase_1(self, socket, lock, barrier: multiprocessing.Barrier):
        self.__handle_client_connection_bets(socket, lock)
        try:
            barrier.wait()
        except Exception as e:
            logging.error(f'action: barrier_handle_client_connection_phase_1 | result: fail | error: {e}')
        
    def _handle_client_connection_phase_2(self, socket, winners):
        self.__handle_client_connection_results(socket, winners)

    def _join_proccesses(self, proccesses):
        for p in proccesses:
            if p is None: continue
            p.join(self._timeout) # 5 minutes... its a lot of time

    def __handle_client_connection_bets(self, client_sock, file_lock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        eof = False
        try:
            protocol = Protocol(client_sock)
            while not eof:
                msg, eof = protocol.receive()
                
                bets = parse_client_bets(msg)
                file_lock.acquire()
                store_bets(bets)
                file_lock.release()

                protocol.send_ack(True)
        except OSError as e:
            logging.error(f'action: receive_message | result: fail | error: {e}')
        except Exception as e:
            try:
                protocol.send_ack(False)
            except OSError as _e:
                logging.error(f'action: receive_message | result: fail | error: {_e}')
            logging.error(f'action: apuesta_almacenada | result: fail | error: {e}')
        finally:
            self._close_client_connection(client_sock)

    def __accept_new_connection(self, server_socket: socket):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info(f'action: accept_connections | result: in_progress')
        try:
            server_socket.settimeout(self._timeout)
            c, addr = server_socket.accept()
            server_socket.settimeout(None)
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            return None

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

    def __handle_client_connection_results(self, client_sock, winners):
        '''
        Send the winners to the client.
        '''
        try:
            protocol = Protocol(client_sock)
            client_id, _eof = protocol.receive()
            protocol.send(",".join(winners[int(client_id)]))
            _ack = protocol.receive_ack()
        except (OSError, Exception) as e:
            logging.error(f'action: consulta_ganadores | result: fail | error: {e}')
        finally:
            self._close_client_connection(client_sock)

    def _close_client_connection(self, client_socket):
        client_socket.close()
