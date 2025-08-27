
import tkinter as tk
class SpeedometerDashboard:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("F1 25 Live Speedometer")
        self.speed_var = tk.StringVar()
        self.gear_var = tk.StringVar()
        self.throttle_var = tk.StringVar()
        self.brake_var = tk.StringVar()

        tk.Label(self.root, text="Speed (km/h):", font=("Arial", 16)).pack()
        tk.Label(self.root, textvariable=self.speed_var, font=("Arial", 32), fg="blue").pack()
        tk.Label(self.root, text="Gear:", font=("Arial", 16)).pack()
        tk.Label(self.root, textvariable=self.gear_var, font=("Arial", 24)).pack()
        tk.Label(self.root, text="Throttle (%):", font=("Arial", 16)).pack()
        tk.Label(self.root, textvariable=self.throttle_var, font=("Arial", 24)).pack()
        tk.Label(self.root, text="Brake (%):", font=("Arial", 16)).pack()
        tk.Label(self.root, textvariable=self.brake_var, font=("Arial", 24)).pack()

    def update(self, speed, gear, throttle, brake):
        self.speed_var.set(f"{speed}")
        self.gear_var.set(f"{gear}")
        self.throttle_var.set(f"{throttle:.1f}")
        self.brake_var.set(f"{brake:.1f}")

    def run(self):
        self.root.mainloop()