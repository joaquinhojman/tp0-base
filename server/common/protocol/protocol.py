
class Protocol:
    def __init__(self, socket):
        self._socket = socket

    def receive_bet(self):
        """
        Receive a bet from the client.
        """
        packet_len = self._receive_packet_len()
        
        bytes_received = 0
        bet = ""
        while bytes_received < packet_len:
            if packet_len - bytes_received > 8192:
                bet += self._socket.recv(8192).decode('utf-8')
            else:
                bet += self._socket.recv(packet_len - bytes_received).decode('utf-8')
            bytes_received = len(bet)
        return bet

    def _receive_packet_len(self):
        """
        Receive the packet length from the client.
        """
        packet_len_bytes = self._socket.recv(4)
        packet_len = int.from_bytes(packet_len_bytes, byteorder='big')
        return packet_len
