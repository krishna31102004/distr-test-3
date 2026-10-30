import struct

def send_msg(sock, data):
    header = struct.pack('!I', len(data)) # representing it into 4 bytes
    sock.sendall(header) # bytes of data
    sock.sendall(data) # the actual data



def recv_exact(sock, n):
    buf = b'' # empty buffer
    while len(buf) < n:
        chunk = sock.recv(n-len(buf))  # expect to receive remianing bytes of data
        if not chunk:
            raise ConnectionError("socket closed before expected data arrived")
        buf += chunk  # add received chunk to the buffer 
    return buf  # return final data received
        

def recv_msg(sock):
    header = recv_exact(sock, 4) # header (4 bytes)
    length = struct.unpack('!I', header)[0] # reverses bytes to integer, returns a tuple so we use [0] to get the first value
    data = recv_exact(sock, length) # raw message
    return data
    
