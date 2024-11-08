import tkinter as tk
from tkinter import ttk, scrolledtext
import paho.mqtt.client as mqtt
from tkcalendar import DateEntry  # Import DateEntry for date selection

# MQTT Configuration
broker_address = "192.168.10.6"  # Replace with your broker address
publish_topic = ""
subscribe_topic = ""

# Create MQTT Client
client = mqtt.Client()

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    msg = f"Connected with result code {rc}\n"
    if subscribe_topic:  # Subscribe only if the topic is set
        client.subscribe(subscribe_topic)
        msg += f"Subscribed to {subscribe_topic}\n"
    message_window.insert(tk.END, msg)
    message_window.see(tk.END)

def on_message(client, userdata, message):
    msg = f"Received message on {message.topic}: {message.payload.decode()}\n"
    message_window.insert(tk.END, msg)
    message_window.see(tk.END)

def connect_broker():
    try:
        client.connect(broker_address, 1883, 60)
        client.loop_start()
        if subscribe_topic:
            client.subscribe(subscribe_topic)
        message_window.insert(tk.END, "Connected to MQTT broker\n")
        message_window.see(tk.END)
    except Exception as e:
        print(f"Failed to connect to MQTT broker: {e}")

def send_on():
    relay_id = relay_select.get()
    duration = duration_select.get()
    command = f"on {relay_id}" if duration == "Immediate" else f"on {relay_id} {duration}"
    publish_command(command)

def send_off():
    relay_id = relay_select.get()
    publish_command(f"off {relay_id}")

def publish_command(command):
    if publish_topic:
        result = client.publish(publish_topic, command)
        print(f"Sent {command} to topic {publish_topic}" if result[0] == 0 else f"Failed to send message to topic {publish_topic}")

def schedule_task():
    relay_id = relay_select.get()
    schedule_type = schedule_type_select.get()
    start_time, end_time = start_time_select.get(), end_time_select.get()
    if schedule_type == "Daily":
        command = f"schedule {relay_id} {start_time} {end_time}"
    else:
        date = one_time_date_entry.get()
        command = f"schedule {relay_id} once {date} {start_time} {end_time}"
    publish_command(command)

def clear_messages():
    message_window.delete('1.0', tk.END)

def delete_schedule_task():
    relay_id = relay_select.get()
    task_type, start_time = schedule_type_select.get().lower(), start_time_select.get()
    command = f"delete {relay_id} {task_type} {start_time}" if start_time else f"delete {relay_id} {task_type}"
    publish_command(command)

def schedule_tasks():
    relay_id = relay_select.get()
    publish_command(f"tasks {relay_id}")

def setup_gui():
    global relay_select, duration_select, start_time_select, end_time_select, schedule_type_select, one_time_date_entry, message_window, pub_entry, sub_entry

    window = tk.Tk()
    window.title("MQTT Relay Control")
    window.configure(bg="#f0f0f5")  # Light grey background

    # Add a placeholder for logo
    logo_frame = tk.Frame(window, bg="#4a90e2")  # Blue color for logo frame background
    logo_frame.grid(row=0, column=0, columnspan=3, sticky="nsew", pady=10)
    logo_label = tk.Label(logo_frame, text="MNS ROBOTICS", font=("Helvetica", 16, "bold"), bg="#4a90e2", fg="white")
    logo_label.pack()

    # Dropdown for Publish Topic
    ttk.Label(window, text="Publish Topic:", background="#f0f0f5").grid(row=1, column=0, sticky=tk.W)
    pub_entry = ttk.Combobox(window, values=["relay"])
    pub_entry.grid(row=1, column=1)

    # Dropdown for Subscribe Topic
    ttk.Label(window, text="Subscribe Topic:", background="#f0f0f5").grid(row=2, column=0, sticky=tk.W)
    sub_entry = ttk.Combobox(window, values=["status"])
    sub_entry.grid(row=2, column=1)

    # Connect button
    connect_button = tk.Button(window, text="Connect", command=lambda: connect_topics(pub_entry.get(), sub_entry.get()), bg="#4a90e2", fg="white")
    connect_button.grid(row=1, column=2, rowspan=2)

    # Relay Selection
    ttk.Label(window, text="Select Relay:", background="#f0f0f5").grid(row=3, column=0, sticky=tk.W)
    relay_select = ttk.Combobox(window, values=[1, 2, 3])
    relay_select.grid(row=3, column=1)

    # Duration Selection
    ttk.Label(window, text="Select Duration:", background="#f0f0f5").grid(row=4, column=0, sticky=tk.W)
    duration_select = ttk.Combobox(window, values=["Immediate"] + [str(i) for i in range(1, 26)])
    duration_select.grid(row=4, column=1)

    # Buttons for Relay Control
    tk.Button(window, text="Turn On", command=send_on, bg="#28a745", fg="white").grid(row=4, column=2)
    tk.Button(window, text="Turn Off", command=send_off, bg="#dc3545", fg="white").grid(row=7, column=2)

    # Schedule Task Section
    ttk.Label(window, text="Schedule Task:", background="#f0f0f5").grid(row=6, column=0, sticky=tk.W)
    schedule_type_select = ttk.Combobox(window, values=["Daily", "One-Time"])
    schedule_type_select.grid(row=6, column=1)

    # Time Selection
    ttk.Label(window, text="Select Time:", background="#f0f0f5").grid(row=7, column=0, sticky=tk.W)
    ttk.Label(window, text="Start Time:", background="#f0f0f5").grid(row=8, column=0, sticky=tk.W)
    start_time_select = ttk.Combobox(window, values=[f"{h:02d}:{m:02d} {am_pm}" for h in range(1, 13) for m in [0, 15, 30, 45] for am_pm in ["AM", "PM"]])
    start_time_select.grid(row=8, column=1)

    ttk.Label(window, text="End Time:", background="#f0f0f5").grid(row=9, column=0, sticky=tk.W)
    end_time_select = ttk.Combobox(window, values=[f"{h:02d}:{m:02d} {am_pm}" for h in range(1, 13) for m in [0, 15, 30, 45] for am_pm in ["AM", "PM"]])
    end_time_select.grid(row=9, column=1)

    # One-Time Date Selection
    ttk.Label(window, text="One-Time Date:", background="#f0f0f5").grid(row=10, column=0, sticky=tk.W)
    one_time_date_entry = DateEntry(window, date_pattern='yyyy-mm-dd')
    one_time_date_entry.grid(row=10, column=1)

    tk.Button(window, text="Schedule Tasks", command=schedule_tasks, bg="#ffc107", fg="white").grid(row=12, column=0)
    tk.Button(window, text="Delete Schedule Task", command=delete_schedule_task, bg="#ff7f50", fg="white").grid(row=12, column=1)
    tk.Button(window, text="Schedule", command=schedule_task, bg="#007bff", fg="white").grid(row=9, column=2)

    # Clear Messages button
    clear_button = tk.Button(window, text="Clear Messages", command=clear_messages, bg="#343a40", fg="white")
    clear_button.grid(row=12, column=2)

    # Add a blank row to create space
    window.grid_rowconfigure(12, minsize=30)  # Adjust 'minsize' to control the space height

    # Window for displaying messages
    message_window = scrolledtext.ScrolledText(window, wrap=tk.WORD, width=50, height=10)
    message_window.grid(row=13, column=0, columnspan=3)

    window.mainloop()

def connect_topics(publish, subscribe):
    global publish_topic, subscribe_topic
    publish_topic, subscribe_topic = publish.strip(), subscribe.strip()
    if publish_topic or subscribe_topic:
        connect_broker()

# Assign callbacks
client.on_connect, client.on_message = on_connect, on_message

# Start GUI
setup_gui()
