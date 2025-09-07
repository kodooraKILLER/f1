import socket
import time
import os
import re

UDP_PORT = 6969  # The port to send UDP packets to (same as F1 telemetry)
UDP_IP = "127.0.0.1"  # Localhost
INPUT_FILE = "udp_captured_data_2508281420.log"  # Change to your actual file name if needed
SEND_HZ = 20  # Send at 20Hz
SEND_INTERVAL = 1.0 / SEND_HZ

def replay_udp_packets():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file '{INPUT_FILE}' not found.")
        return

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"Replaying UDP packets from '{INPUT_FILE}' to {UDP_IP}:{UDP_PORT} at {SEND_HZ} Hz...")

    with open(INPUT_FILE, "rb") as f:
        for line in f:
            # Each line: timestamp:::LEN=xxx:::[binary data]\n
            try:
                # Find the separator after the length info
                sep = b":::"
                first = line.find(sep)
                second = line.find(sep, first + len(sep))
                if first == -1 or second == -1:
                    continue
                # The actual UDP packet data starts after the second separator and length info
                # Find the next ':::' after LEN=xxx:::
                pattern = re.compile(rb"^.*?:::+LEN=\d+:::(.*)\n?$")
                match = pattern.match(line)
                packet_data = None
                if match:
                    packet_data = match.group(1).rstrip(b'\n')
                # data_start = second + len(sep)
                # # Remove trailing newline if present
                # packet_data = line[data_start:].rstrip(b'\n')
                if packet_data:
                    sock.sendto(packet_data, (UDP_IP, UDP_PORT))
                    print(f"Sent {len(packet_data)} bytes")
                    time.sleep(SEND_INTERVAL)
            except Exception as e:
                print(f"Error parsing or sending line: {e}")
                continue

    sock.close()
    print("Replay finished.")

if __name__ == "__main__":
    replay_udp_packets()