import sys
import socket
import subprocess
import threading
import shlex


from protocol import send_msg, recv_msg

if len(sys.argv) != 2 and len(sys.argv) != 3:
    print("Use: python3 server.py <vm_number> [port]")  # port for tests
    sys.exit(1) 

VM_NUMBER = sys.argv[1] # ["server.py", "2"] for vm_number == 2
LOG_FILE = f"machine.{VM_NUMBER}.log"


HOST = ''  
 # Port to listen on (non-privileged ports are > 1023)
if len(sys.argv) == 3:
    PORT = int(sys.argv[2])
else:
    PORT = 65432

def handle_client(conn, addr):
    print(f"connected by {addr}")
    try:
        data = recv_msg(conn) # from protocol.py
        pattern = data.decode()
        try: 
            args = shlex.split(pattern) #lexical analyzer
        except ValueError as e:
            output = f"FILENAME:{LOG_FILE}\ngrep error: invalid pattern syntax"
            send_msg(conn, output.encode())
            conn.close()
            return

        result = subprocess.run(
            ["grep"] + args + [LOG_FILE],
            capture_output=True,
            text=True
        )  # grep 

        if result.returncode == 0:  # matches found
            body = result.stdout
        elif result.returncode == 1: # no matches
            body = ""
        else:
            body = f'grep error: {result.stderr}'  # error
        output = f"FILENAME:{LOG_FILE}\n{body}"
        send_msg(conn, output.encode())
    except Exception as e:
        print(f"error handling {addr}: {e}")
    finally:
        conn.close() # ensure connection closes


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # setting socket options
    s.bind((HOST, PORT))
    s.listen(16)
    print(f"Server listening on {PORT}")
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr)) # a thread for each machine
        t.start()
