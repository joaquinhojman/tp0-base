from configparser import ConfigParser
import logging
import os
import socket

def _initialize_config():
    config = ConfigParser(os.environ)
    config.read("protocol.ini")

    config_params = {}
    try:
        config_params["cant_bytes_for_len"] = int(os.getenv('CANT_BYTES_FOR_LEN', config["DEFAULT"]["CANT_BYTES_FOR_LEN"]))
        config_params["cant_bytes_for_ack"] = int(os.getenv('CANT_BYTES_FOR_ACK', config["DEFAULT"]["CANT_BYTES_FOR_ACK"]))
        config_params["max_cant_bytes_for_packet"] = int(os.getenv('MAX_CANT_BYTES_FOR_PACKET', config["DEFAULT"]["MAX_CANT_BYTES_FOR_PACKET"]))
        config_params["success"] = int(os.getenv('SUCCESS', config["DEFAULT"]["SUCCESS"]))
        config_params["error"] = int(os.getenv('ERROR', config["DEFAULT"]["ERROR"]))
        config_params["cant_bytes_for_eof"] = int(os.getenv('CANT_BYTES_FOR_EOF', config["DEFAULT"]["CANT_BYTES_FOR_EOF"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))
    
    return config_params

class Protocol:
    def __init__(self, socket):
        self._socket = socket
        config_params = _initialize_config()
        self._cant_bytes_for_len = config_params["cant_bytes_for_len"]
        self._max_cant_bytes_for_packet = config_params["max_cant_bytes_for_packet"]
        self._success_ack = config_params["success"]
        self._error_ack = config_params["error"]
        self._cant_bytes_for_ack = config_params["cant_bytes_for_ack"]
        self._eof = config_params["success"]
        self._not_eof = config_params["error"]
        self._cant_bytes_for_eof = config_params["cant_bytes_for_eof"]

    def receive(self):
        """
        Receive a bet from the client.
        """
        packet_len = self._receive_packet_len()
        eof = self._receive_eof()
        
        expected_bytes = 0
        bytes_received = 0
        bet = ""
        bet_bytes = bytearray()
        while bytes_received < packet_len:
            if expected_bytes == 0:
                if packet_len - bytes_received > self._max_cant_bytes_for_packet:
                    expected_bytes = self._max_cant_bytes_for_packet
                else:
                    expected_bytes = packet_len - bytes_received
            
            received = self._socket.recv(expected_bytes)

            if received is None:
                raise OSError("Received None from socket on recv bets")

            expected_bytes -= len(received) #for possible short read

            bytes_received += len(received)
            bet_bytes += received
        bet = bet_bytes.decode('utf-8')
        addr = self._socket.getpeername()
        #logging.info(f'action: receive | result: success | ip: {addr[0]} | msg: {bet} | eof: {eof}')
        
        return bet, eof

    def _receive_packet_len(self):
        """
        Receive the packet length from the client.
        """
        received = 0
        packet_len_bytes = bytearray(self._cant_bytes_for_len)
        while received < self._cant_bytes_for_len:
            received = self._socket.recv(self._cant_bytes_for_len - received)
            if received is None:
                raise OSError("Received None from socket on rec packet len")
            packet_len_bytes += received
            received = len(packet_len_bytes)

        packet_len = int.from_bytes(packet_len_bytes, byteorder='big')
        return packet_len

    def _receive_eof(self):
        """
        Receive if there will come more batches, or if is this the last.
        """
        received = 0
        eof_bytes = bytearray(self._cant_bytes_for_eof)
        while received < self._cant_bytes_for_eof:
            received = self._socket.recv(self._cant_bytes_for_eof - received)
            if received is None:
                raise OSError("Received None from socket on recv eof")
            eof_bytes += received
            received = len(eof_bytes)

        eof = True if int.from_bytes(eof_bytes, byteorder='big') == self._eof else False
        return eof

    def receive_ack(self):
        """
        Receive an ack from the server.
        """
        received = 0
        ack_bytes = bytearray(self._cant_bytes_for_ack)
        while received < self._cant_bytes_for_ack:
            received = self._socket.recv(self._cant_bytes_for_ack - received)
            if received is None:
                raise OSError("Received None from socket on recv ack")
            ack_bytes += received
            received = len(ack_bytes)

        ack = True if int.from_bytes(ack_bytes, byteorder='big') == self._success_ack else False

        addr = self._socket.getpeername()
        #logging.info(f'action: receive_ack | result: success | ip: {addr[0]} | msg: {ack}')
        return ack

    def send(self, bets: str, eof: bool = False):
        """
        Send bets to server.
        """
        bets_bytes = bets.encode('utf-8')

        bets_len = len(bets_bytes)
        self._send_packet_len(bets_len)
        self._send_eof(eof)
        
        expected_bytes_to_send = 0
        bytes_sended = 0
        while bytes_sended < bets_len:
            if expected_bytes_to_send == 0:
                if bets_len - bytes_sended > self._max_cant_bytes_for_packet:
                    expected_bytes_to_send = self._max_cant_bytes_for_packet
                else:
                    expected_bytes_to_send = bets_len - bytes_sended

            sent = self._socket.send(bets_bytes[bytes_sended:bytes_sended + expected_bytes_to_send])
            if sent != -1:
                bytes_sended += sent
            else:
                raise OSError("Socket connection broken during send bets")
            expected_bytes_to_send -= sent

        addr = self._socket.getpeername()
        #logging.info(f'action: send | result: success | ip: {addr[0]} | msg: {bets}')

    def _send_packet_len(self, bets_len: int):
        bets_len_bytes = bets_len.to_bytes(self._cant_bytes_for_len, byteorder='big')
        sended = 0
        while sended < self._cant_bytes_for_len:
            sent = self._socket.send(bets_len_bytes[sended:self._cant_bytes_for_len])
            if sent != -1:
                sended += sent
            else:
                raise OSError("Socket connection broken during send bets len")

    def _send_eof(self, eof: bool):
        """
        Send to the server if there will come more batches, or if is this the last.
        """
        eof = self._eof if eof == True else self._not_eof
        eof_bytes = eof.to_bytes(self._cant_bytes_for_eof, byteorder='big')
        sended = 0
        while sended < self._cant_bytes_for_eof:
            sent = self._socket.send(eof_bytes[sended:self._cant_bytes_for_eof])
            if sent != -1:
                sended += sent
            else:
                raise OSError("Socket connection broken during send eof")

    def send_ack(self, ack: bool):
        """
        Send a bets ack to the client.
        """
        ack_response = self._success_ack if ack == True else self._error_ack
        ack_bytes = ack_response.to_bytes(self._cant_bytes_for_ack, byteorder='big')
        sended = 0
        while sended < self._cant_bytes_for_ack:
            sent = self._socket.send(ack_bytes[sended:self._cant_bytes_for_ack])
            if sent != -1:
                sended += sent
            else:
                raise OSError("Socket connection broken during send ack")
        addr = self._socket.getpeername()
        #logging.info(f'action: send_ack | result: success | ip: {addr[0]} | msg: {ack}')
