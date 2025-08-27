import socket
import struct

# Define the structure for the packet header based on the PDF
# Each packet starts with this header.
PACKET_HEADER_FORMAT = struct.Struct('<HBBBBBQfIIBB')
CAR_TELEMETRY_FORMAT = struct.Struct('<HfffBbHBBH4H4B4BH4f4B')
class PacketHeader:
    def __init__(self, data):
        # Unpack the header data using the format string
        # '<' means little-endian byte order
        # 'H' is unsigned short (2 bytes)
        # 'B' is unsigned char (1 byte)
        # 'Q' is unsigned long long (8 bytes)
        # 'f' is float (4 bytes)
        # 'I' is unsigned int (4 bytes)
        (
            self.m_packetFormat,
            self.m_gameYear,
            self.m_gameMajorVersion,
            self.m_gameMinorVersion,
            self.m_packetVersion,
            self.m_packetId,
            self.m_sessionUID,
            self.m_sessionTime,
            self.m_frameIdentifier,
            self.m_overallFrameIdentifier,
            self.m_playerCarIndex,
            self.m_secondaryPlayerCarIndex
        ) = PACKET_HEADER_FORMAT.unpack(data)

# Define the structure for the car telemetry data
class CarTelemetryData:
    def __init__(self, data):
        # Unpack the telemetry data for a single car
        # 'H' for speed (uint16)
        # 'f' for throttle, steer, brake (float)
        # 'b' for gear (int8)
        # 'B' for clutch, drs, revLightsPercent (uint8)
        # 'H' for engineRPM, revLightsBitValue (uint16)
        # '4H' for brakesTemperature
        # '4B' for tyresSurfaceTemperature
        # '4B' for tyresInnerTemperature
        # 'H' for engineTemperature
        # '4f' for tyresPressure
        # '4B' for surfaceType (one for each wheel)
        (
            self.m_speed,
            self.m_throttle,
            self.m_steer,
            self.m_brake,
            self.m_clutch,
            self.m_gear,
            self.m_engineRPM,
            self.m_drs,
            self.m_revLightsPercent,
            self.m_revLightsBitValue,
            *self.other_fields
        ) = CAR_TELEMETRY_FORMAT.unpack(data)


# Define the main structure for the telemetry packet
class PacketCarTelemetryData:
    PACKET_ID = 6  # The ID for the Car Telemetry packet

    def __init__(self, data):
        self.m_header = PacketHeader(data[:PACKET_HEADER_FORMAT.size])
        self.m_carTelemetryData = []
        # The car telemetry data starts after the header (29 bytes)
        start_byte = PACKET_HEADER_FORMAT.size
        # Each car's telemetry data is 58 bytes long according to the PDF
        car_data_size = CAR_TELEMETRY_FORMAT.size 
        # Loop through all 22 possible cars
        for _ in range(22):
            end_byte = start_byte + car_data_size
            car_data = data[start_byte:end_byte]
            # Ensure there's enough data to unpack
            if len(car_data) == car_data_size:
                self.m_carTelemetryData.append(CarTelemetryData(car_data))
            start_byte = end_byte

def main():
    """
    Listens for F1 25 UDP telemetry packets and prints the player's car speed.
    """
    # Standard F1 game UDP port
    udp_port = 20777
    
    # Create a UDP socket
    # AF_INET for IPv4
    # SOCK_DGRAM for UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # Bind the socket to the port. 
    # An empty string for the IP address means it will listen on all available interfaces.
    try:
        sock.bind(("0.0.0.0", udp_port))
        print(f"Listening for F1 25 telemetry on port {udp_port}...")
    except OSError as e:
        print(f"Error: Could not bind to port {udp_port}. Is another application using it?")
        print(e)
        return

    # Main loop to receive and process data
    while True:
        data, _ = sock.recvfrom(60577)
        # print(".",end="", flush=True)
        try:
            # Receive data from the socket. Buffer size is set to a value large enough for the biggest packet.
            
            # Unpack just the packet ID from the header to check the packet type
            # The packetId is the 6th byte (index 5)
            header = PacketHeader(data[:PACKET_HEADER_FORMAT.size])
            # print(f"+{header.m_packetId}",end="", flush=True)

            # Check if the packet is a Car Telemetry packet
            if header.m_packetId == PacketCarTelemetryData.PACKET_ID:
                print("*",end="", flush=True)
                telemetry_packet = PacketCarTelemetryData(data)
                
                # Get the index of the player's car from the header
                player_car_index = telemetry_packet.m_header.m_playerCarIndex
                # print("Player car information : Car", player_car_index)
                
                # Get the telemetry data for the player's car
                player_car_telemetry = telemetry_packet.m_carTelemetryData[player_car_index]
                
                # Print the player's car speed

                print(f"Speed: {player_car_telemetry.m_speed} km/h ,Gear: {player_car_telemetry.m_gear}, Throttle: {player_car_telemetry.m_throttle*100}%, Brake: {player_car_telemetry.m_brake*100}%")

                # print("----")
                # print("other car information: ")

                # for i, car in enumerate(telemetry_packet.m_carTelemetryData):
                #     print(f"Car {i}: Speed={car.m_speed} km/h, Gear={car.m_gear}, Throttle={car.m_throttle*100}%, Brake={car.m_brake*100}%")
                # print("====")
                # break

        except KeyboardInterrupt:
            print("\nStopping listener.")
            break
        except IndexError:
            # This can happen if a packet is received before the session is fully active.
            # We can safely ignore it and wait for the next valid packet.
            continue
        except Exception as e:
            print(f"\nAn error occurred: {e}")
            # Continue listening even if one packet is malformed
            continue

if __name__ == "__main__":
    main()
    print("BYE")
