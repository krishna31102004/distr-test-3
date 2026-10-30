import struct
import threading

_byte_count_lock = threading.Lock()
_bytes_sent = 0
_bytes_received = 0

def send_msg(sock, data):
    global _bytes_sent
    header = struct.pack('!I', len(data)) # representing it into 4 bytes
    sock.sendall(header) # bytes of data
    sock.sendall(data) # the actual data
    with _byte_count_lock:
        _bytes_sent += 4 + len(data)



def recv_exact(sock, n):
    buf = b'' # empty buffer
    while len(buf) < n:
        chunk = sock.recv(n-len(buf))  # expect to receive remianing bytes of data
        if not chunk:
            raise ConnectionError("socket closed before expected data arrived")
        buf += chunk  # add received chunk to the buffer 
    return buf  # return final data received
        

def recv_msg(sock):
    global _bytes_received
    header = recv_exact(sock, 4) # header (4 bytes)
    length = struct.unpack('!I', header)[0] # reverses bytes to integer, returns a tuple so we use [0] to get the first value
    data = recv_exact(sock, length) # raw message
    with _byte_count_lock:
        _bytes_received += 4 + len(data)
    return data


def get_byte_counts():
    with _byte_count_lock:
        return _bytes_sent, _bytes_received
