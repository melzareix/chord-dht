import socket
import struct


def test_api_get_key():
    s = socket.socket()
    s.connect(("127.0.0.1", 36979))
    s.send(struct.pack(">HH", 36, 651) + key)
    res = s.recv(1024)
    print("=>", res[36:].decode("utf-8"))
    print("Result:", res)
    s.close()

def test_api_put_key(val):
    s = socket.socket()
    s.connect(("127.0.0.1", 36979))
    value = val.encode("utf-8")
    length = 8 + len(key) + len(value)
    s.send(struct.pack(">HHHBB", length, 650, 300, 5, 0) + key + value)
    res = s.recv(1024)
    print("Result:", res)
    s.close()


while True:
    inp = input("op-key-value\n")
    res = inp.split("-")
    key = res[1].encode("utf-8").ljust(32, b"\0")
    if res[0] == "get":
        test_api_get_key()
    else:
        test_api_put_key(res[2])
