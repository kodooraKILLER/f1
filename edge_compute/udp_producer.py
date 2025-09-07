import socket
import json
import time
import random

def generate_dummy_data():
    ITERATOR=0
    RESET_FUEL_LEVEL = 100.0
    HERTZ=20
    current_fuel = RESET_FUEL_LEVEL
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = ('localhost', 20777)    
    print("Starting UDP data producer. Press Ctrl+C to stop.")
    print(ITERATOR,"+",end="",flush=True)
    try:
        start_time = time.time()
        while True:
            session_time = time.time() - start_time
            fuel_consumed_rate = random.uniform(0.005, 0.08)
            current_fuel -= fuel_consumed_rate
            if current_fuel < 0:
                ITERATOR+=1
                current_fuel = RESET_FUEL_LEVEL*ITERATOR
                # print("ITER UPDATE TO ",ITERATOR)
                print(ITERATOR,"+",end="",flush=True)
            
            packet = {
                'player_car_index': 0,
                'fuelInTank': current_fuel,
                'sessionTime': session_time
            }
            data_to_send = json.dumps(packet).encode('utf-8')
            print(".",end="",flush=True)
            udp_socket.sendto(data_to_send, server_address)
            time.sleep(1/HERTZ) 
            
    except KeyboardInterrupt:
        print("Stopping UDP data producer.")
    finally:
        udp_socket.close()

if __name__ == "__main__":
    generate_dummy_data()