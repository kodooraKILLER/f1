import socket
import struct

# UDP setup
UDP_IP = "0.0.0.0"
UDP_PORT = 20777  # Default F1 UDP port

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

# Packet header format (24 bytes, adjust if needed)
header_struct = struct.Struct('<HBBBBQfIBB')  # Example for F1 2020+ spec
car_telemetry_struct = struct.Struct('<HfffBbHBBH4H4B4BH4f4B')

while True:
    data, addr = sock.recvfrom(65507)
    header_data = data[:header_struct.size]
    car_data = data[header_struct.size:header_struct.size + car_telemetry_struct.size]
    unpacked_header = header_struct.unpack(header_data)
    print("Header unpacked:", unpacked_header)
    try:
        car = car_telemetry_struct.unpack(car_data)
        print("Car info:",car)
    except Exception as e:
        print("minor kunju")