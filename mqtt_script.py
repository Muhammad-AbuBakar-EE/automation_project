############################# Imports ##################################
import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
from gpiozero import MCP3208
import threading
from datetime import datetime
from time import sleep
from datetime import timedelta
import schedule
import numpy as np
import time
import re
import json
import os
import math
import signal
import sys
######################## Variables & Pins Init #########################
publish_status_count = 0
is_relay_on = [False, False, False]
task_type_flag = [False, False, False]
stop_one_time_task = False

check_thresholds = [0]*3
motors_index = [None]*3
count_readings = [0]*3

relay_lock = threading.Lock()
sensor_lock = threading.Lock()
schedule_lock = threading.Lock()
flag_lock = threading.Lock()

# Create a global stop event
stop_event = threading.Event()

# GPIO setup for relay
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
relay_pins = [13,19,26]
for pin in relay_pins:
    GPIO.setup(pin, GPIO.OUT)
    GPIO.output(pin, 1) # Start with the relays OFF

# Define the MQTT broker and topics
broker = "localhost"  # Assuming the broker is running on the same Pi
topic_pub = "status"  # Topic for publishing the relay status
topic_sub = "relay"   # Topic to control the relay

NUM_SENSORS = 5
adc_max1 = 3200
adc_min1 = 0 
volt_multi = 233
volt_multi_p = volt_multi*(math.sqrt(2))
volt_multi_n = - volt_multi_p
############################ Key Interrupt #############################
def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
######################## Voltage Sensors Thread ########################
def map_adc_to_volts(adc_value, adc_min, adc_max, volt_min, volt_max):
	return ((adc_value-adc_min)*((volt_max-volt_min)/(adc_max-adc_min))) + volt_min
	
relay_voltages = np.zeros(NUM_SENSORS)
relay_sensors = np.zeros(NUM_SENSORS)

channels = [MCP3208(i) for i in range(NUM_SENSORS)]

def get_voltage():
    # ~ global is_relay_on
    global relay_voltages
    global relay_sensors
    while not stop_event.is_set():
        with sensor_lock:
            for i in range(NUM_SENSORS):
                sum_of_squares = 0
                N = 0
                channel = MCP3208(i)
                start_time = time.time()
                while (time.time() - start_time) < 0.2:  # 200 ms
                    adc_value = ((channel.value) * 4096)
                    volt_inst = map_adc_to_volts(adc_value, adc_min1, adc_max1, volt_multi_n, volt_multi_p)
                    sum_of_squares += volt_inst ** 2
                    N += 1
                    sleep(0.001)  # Small sleep for stability
            
                relay_voltages[i] = np.sqrt(sum_of_squares / N)
            # ~ 0.8962
            relay_sensors = np.around(relay_voltages * 2, 2)
        # ~ print("Vrms:", relay_sensors, "VAC")
        sleep(1)  # Adjust to your preferred measurement frequency
########################## Interrupted Tasks File ########################
INTERRUPTED_TASKS_FILE = 'interrupted.json'

# Save interrupted task to a file
def save_interrupted_task(task):
    if os.path.exists(INTERRUPTED_TASKS_FILE):
        with open(INTERRUPTED_TASKS_FILE, 'r') as f:
            interrupted_tasks = json.load(f)
    else:
        interrupted_tasks = []
    
    interrupted_tasks.append(task)

    with open(INTERRUPTED_TASKS_FILE, 'w') as f:
        json.dump(interrupted_tasks, f, indent=4)
########################## Save & Load Schedule ########################
# Schedule files
SCHEDULE_FILE = 'schedules.json'
COMPLETED_TASKS_FILE = 'completed_tasks.json'

# Initialize schedule data
schedule_data = {
    "daily_schedules": [],
    "one_time_tasks": []
}

# Load schedules from JSON file
def load_schedules():
    global schedule_data
    if os.path.exists(SCHEDULE_FILE):
        with open(SCHEDULE_FILE, 'r') as f:
            schedule_data = json.load(f)
    else:
        save_schedules()  # Create an empty schedule file if it doesn't exist

# Save schedules to JSON file
def save_schedules():
    with open(SCHEDULE_FILE, 'w') as f:
        json.dump(schedule_data, f, indent=4)
######################### Completed Tasks File #########################
# Save completed task to a separate file
def save_completed_task(task):
    if os.path.exists(COMPLETED_TASKS_FILE):
        with open(COMPLETED_TASKS_FILE, 'r') as f:
            completed_tasks = json.load(f)
    else:
        completed_tasks = []
    
    completed_tasks.append(task)

    with open(COMPLETED_TASKS_FILE, 'w') as f:
        json.dump(completed_tasks, f, indent=4)
################### MQTT & Scheduling Related Functions ################
# Publish relay status
def publish_status(client, status):
    client.publish(topic_pub, status)
    print(f"Published status: {status}")

# Validate time format
def is_valid_time_format(time_str):
    pattern = r'^(0[1-9]|1[0-2]):[0-5][0-9] (AM|PM)$'
    return re.match(pattern, time_str) is not None
    
# Delete Scheduled Tasks
def delete_task(client, relay_id, task_type, start_time=None):
    global schedule_data
    deleted = False

    # Lock the schedule to avoid concurrency issues
    with schedule_lock:
        if task_type == 'daily':
            tasks = schedule_data['daily_schedules']
        elif task_type == 'one-time':
            tasks = schedule_data['one_time_tasks']
        else:
            publish_status(client, f"Invalid task type: {task_type}")
            return

        if start_time:
            # Find and remove the task with the specified start time for the given relay
            tasks_to_delete = [task for task in tasks if task['relay_id'] == relay_id and task['start'] == start_time]
            if tasks_to_delete:
                for task in tasks_to_delete:
                    tasks.remove(task)
                deleted = True
                remove_scheduled_task(relay_id, start_time)  # Cancel the scheduled job
        else:
            # Remove all tasks for the relay
            tasks_to_delete = [task for task in tasks if task['relay_id'] == relay_id]
            if tasks_to_delete:
                for task in tasks_to_delete:
                    tasks.remove(task)
                deleted = True
                # Remove all scheduled jobs for this relay
                remove_scheduled_task(relay_id)

        if deleted:
            save_schedules()  # Update the schedule JSON file
            publish_status(client, f"Deleted task(s) for relay {relay_id + 1} from {task_type}.")
        else:
            publish_status(client, f"No matching task found for relay {relay_id + 1} in {task_type}.")

def remove_scheduled_task(relay_id, start_time=None):
    print(f"Removing scheduled tasks for Relay {relay_id}, Start Time: {start_time}")
    
    if start_time:
        job_key_on = (relay_id, start_time)
        job_on = scheduled_jobs.pop(job_key_on, None)  # Remove the ON job
        print(f"Attempting to cancel ON job: {job_key_on}, Found: {job_on}")
        
        if job_on:
            schedule.cancel_job(job_on)
            print(f"Cancelled scheduled ON task for Relay {relay_id + 1} at {start_time}")
            publish_status(client, f"Cancelled scheduled ON task for Relay {relay_id + 1} at {start_time}")
        
        # Look for the OFF job
        job_key_off = next((key for key, job in scheduled_jobs.items() if key[0] == relay_id and key[1] != start_time), None)
        print(f"Found OFF job key: {job_key_off}")
        
        if job_key_off:
            job_off = scheduled_jobs.pop(job_key_off, None)
            if job_off:
                schedule.cancel_job(job_off)
                print(f"Cancelled scheduled OFF task for Relay {relay_id + 1} after {start_time}")
                publish_status(client, f"Cancelled scheduled OFF task for Relay {relay_id + 1} after {start_time}")
    else:
        # Cancel all jobs for the relay
        jobs_to_cancel = [(relay, time) for (relay, time) in scheduled_jobs if relay == relay_id]
        print(f"Jobs to cancel for Relay {relay_id}: {jobs_to_cancel}")
        
        for job_key in jobs_to_cancel:
            job = scheduled_jobs.pop(job_key, None)
            if job:
                schedule.cancel_job(job)
                print(f"Cancelled all scheduled tasks for Relay {relay_id + 1}")
                publish_status(client, f"Cancelled all scheduled tasks for Relay {relay_id + 1}")

# show list of scheduled tasks
def show_tasks_for_relay(client, relay_id):
    with schedule_lock:
        # Filter tasks by relay_id
        tasks = [task for task in schedule_data['daily_schedules'] if task['relay_id'] == relay_id]
    
        # Format tasks for display
        if tasks:
            task_list = "\n".join([f"Task: {t['start']} - {t['end']}" for t in tasks])
        else:
            task_list = "No daily tasks scheduled."

    # Handle one-time tasks
        one_time_tasks = [task for task in schedule_data['one_time_tasks'] if task['relay_id'] == relay_id and not task['completed']]
        if one_time_tasks:
            task_list += "\n" + "\n".join([f"One-time Task: {t['start']} - {t['end']} on {t['date']}" for t in one_time_tasks])
        else:
            task_list += "\nNo one-time tasks scheduled."

        # Publish the tasks
        publish_status(client, f"Tasks for Relay {relay_id + 1}:\n{task_list}")

# Handle incoming MQTT messages
def on_message(client, userdata, message):
    global is_relay_on
    global relay_sensors
    # ~ relay_id = None
    msg = message.payload.decode()
    print(f"Received message: {msg}")
    
    # Check if the message is empty or consists only of whitespace
    if not msg or not msg.strip():
        publish_status(client, "Invalid command: Message is empty or contains only whitespace.")
        return

    parts = msg.lower().split()
    
    # Check if the command is valid
    if len(parts) == 0 or parts[0] not in ["on", "off", "schedule", "tasks", "delete"]:
        publish_status(client, "Invalid command: Please enter a valid command (on, off, schedule, tasks, delete).")
        return
        
    command = parts[0]  # "on", "off", or "schedule"
    
    if command in ["on", "off", "schedule","tasks", "delete"]:
        if len(parts) > 1:
            try:
                relay_id = int(parts[1]) - 1  # Convert to 0-based index
            except ValueError:
                publish_status(client, f"Invalid Relay ID {parts[1]}")
                return
        else:
            publish_status(client, "Invalid Command")
            return
            
        if command == "tasks":
            show_tasks_for_relay(client, relay_id)
            return
        
        if command == "delete":
            print(f"Delete command received: {parts}")  # Debugging
            if len(parts) >= 3:
                try:
                    relay_id = int(parts[1]) - 1  # Convert to 0-based index
                    task_type = parts[2]  # "daily" or "one-time"
                    
                    # Handle specific start time deletion
                    if len(parts) == 5:  # Command like "delete 1 daily 12:00 AM"
                        start_time = f"{parts[3]} {parts[4].upper()}"
                        print(f"Delete specific task for relay {relay_id + 1}, task type {task_type}, start time {start_time}")  # Debugging
                        delete_task(client, relay_id, task_type, start_time)
                    else:  # Command like "delete 1 daily" or "delete 1 one-time"
                        print(f"Delete all tasks for relay {relay_id + 1}, task type {task_type}")  # Debugging
                        delete_task(client, relay_id, task_type)
                except ValueError:
                    publish_status(client, f"Invalid Relay ID {parts[1]}")
                    return
            else:
                publish_status(client, "Invalid delete command format.")
                
        with relay_lock:
        # Check if relay_id is out of valid range
            if relay_id < 0 or relay_id >= len(relay_pins):
                publish_status(client, f"Invalid Relay ID {relay_id + 1}")  # Use provided ID for feedback
                return
            else:
                with sensor_lock:
                    if relay_sensors[4] < 250:
                        publish_status(client, f"Breaker OFF")
                        return
                    elif command == "on" and relay_sensors[3] > 250:
                        # ~ sleep(1)
                        if not any(is_relay_on):
                            publish_status(client, f"Manual Switch ON, Turn it OFF")
                            return
    if command == "on":
        if len(parts) > 2 and parts[2].isdigit():  # If a time duration is provided (e.g., "on 5")
            duration_minutes = int(parts[2])  # Get the duration in minutes
            safe_window = 5  # Adding 5 minutes grace time
            total_duration_minutes = duration_minutes + safe_window

            # Get the current time in 12-hour format
            current_time = datetime.now().strftime('%I:%M %p')

            # Calculate the end time after adding the total duration
            end_time_dt = datetime.now() + timedelta(minutes=total_duration_minutes)
            end_time = end_time_dt.strftime('%I:%M %p')  # Convert back to 12-hour format
            with schedule_lock:             
            # Check if turning on for the specified duration conflicts with any existing tasks
                if not has_overlap(current_time, end_time, relay_id):
                    # If no overlap, proceed to turn on for the specified duration
                    duration_seconds = duration_minutes * 60  # Convert minutes to seconds
                    schedule_on(relay_id, "non-scheduled", duration_seconds)
                else:
                    # If there's a conflict, inform the user
                    publish_status(client, f"Motor {relay_id} cannot be turned ON for {duration_minutes} minutes due to a schedule conflict.")
        else:
            # Get the current time in 12-hour format
            current_time = datetime.now().strftime('%I:%M %p')
            safe_window = 5
            # Calculate the end time after adding the total duration
            end_time_dt = datetime.now() + timedelta(minutes=safe_window)
            end_time = end_time_dt.strftime('%I:%M %p')  # Convert back to 12-hour format
            with schedule_lock:
                # Check if turning on for the specified duration conflicts with any existing tasks
                if not has_overlap(current_time, end_time, relay_id):
                    # If no duration is provided, simply turn on without checking conflicts
                    schedule_on(relay_id, "non-scheduled")
                else:
                    # If there's a conflict, inform the user
                    publish_status(client, f"Motor {relay_id} can't be turned ON due to safe operation of a schedule in few mins.")
    elif command == "off":
        schedule_off(relay_id)
    elif command == "schedule":
        if len(parts) == 6:  # "schedule relay_id HH:MM AM/PM HH:MM AM/PM"
            start_time = f"{parts[2]} {parts[3].upper()}"
            end_time = f"{parts[4]} {parts[5].upper()}"
            relay_id = int(parts[1])-1

            if is_valid_time_format(start_time) and is_valid_time_format(end_time):
                set_daily_schedule(start_time, end_time, relay_id)
            else:
                publish_status(client, "Please use HH:MM AM/PM.")
        elif len(parts) == 8:  # "schedule relay_id once YYYY-MM-DD HH:MM AM/PM HH:MM AM/PM"
            date = parts[3]
            start_time = f"{parts[4]} {parts[5].upper()}"
            end_time = f"{parts[6]} {parts[7].upper()}"
            relay_id = int(parts[1])-1

            if is_valid_time_format(start_time) and is_valid_time_format(end_time):
                set_one_time_task(date, start_time, end_time, relay_id)
            else:
                publish_status(client, "Please use HH:MM AM/PM.")
        else:
            publish_status(client, "Invalid schedule command format.")

def schedule_on(relay_id, task_type, duration=None):
    global is_relay_on
    global task_type_flag
    
    if task_type == "non-scheduled":
        with flag_lock:
            task_type_flag[relay_id] = True
    
    with relay_lock:
        if not is_relay_on[relay_id]:  # Check if the relay is already ON
            GPIO.output(relay_pins[relay_id], 0)
            is_relay_on[relay_id] = True  # Update the flag
            if duration:
                threading.Timer(duration, schedule_off,args=(relay_id,)).start()  # Turn off after the duration
                publish_status(client, f"Motor {relay_id+1} ON for {duration/60} minutes")
            else:
                publish_status(client, f"Motor {relay_id+1} turned-ON")
            

# Function to turn off the relay
def schedule_off(relay_id):
    global is_relay_on
    
    with flag_lock:
        task_type_flag[relay_id] = False
        
    with relay_lock:
        if is_relay_on[relay_id] and motors_index[relay_id] is None:  # Check if the relay is already OFF
            GPIO.output(relay_pins[relay_id], 1)
            publish_status(client, f"Motor {relay_id+1} turned-OFF")
            is_relay_on[relay_id] = False  # Update the flag
            check_thresholds[relay_id] = 0
            count_readings[relay_id] = 0
        elif is_relay_on[relay_id] and motors_index[relay_id] == relay_id:
            GPIO.output(relay_pins[relay_id], 1)
            publish_status(client, f"Motor {relay_id+1} turned-OFF")
            is_relay_on[relay_id] = False  # Update the flag
            motors_index[relay_id] = False
            check_thresholds[relay_id] = 0
            count_readings[relay_id] = 0

# ~ # Convert 12-hour time format (with AM/PM) to 24-hour format
def convert_to_24hr_format(time_str):
    time_obj = datetime.strptime(time_str, '%I:%M %p')
    return time_obj.strftime('%H:%M')
    
# Helper function to check if start time is before end time
def is_valid_time_range(start_time, end_time):
    start_dt = datetime.strptime(start_time, '%I:%M %p')
    end_dt = datetime.strptime(end_time, '%I:%M %p')
    return start_dt < end_dt

# Helper function to check if two time ranges overlap
def is_overlapping(start_time, end_time, existing_start, existing_end):
    # Convert to datetime for comparison
    start_dt = datetime.strptime(start_time, '%I:%M %p')
    end_dt = datetime.strptime(end_time, '%I:%M %p')
    existing_start_dt = datetime.strptime(existing_start, '%I:%M %p')
    existing_end_dt = datetime.strptime(existing_end, '%I:%M %p')
    # Check for overlap
    return (start_dt < existing_end_dt) and (end_dt > existing_start_dt)

# Check if the new task overlaps with any existing tasks
def has_overlap(start_time, end_time, relay_id, date=None):
    if date:
        # Check for overlap in one-time tasks
        for task in schedule_data["one_time_tasks"]:
            if task["date"] == date and task["relay_id"] == relay_id:
                if is_overlapping(start_time, end_time, task["start"], task["end"]):
                    return True
        # Check for overlap in daily schedules
        for task in schedule_data["daily_schedules"]:
            if task["relay_id"] == relay_id:
                if is_overlapping(start_time, end_time, task["start"], task["end"]):
                    return True
    else:
        # Check for overlap in daily schedules
        for task in schedule_data["daily_schedules"]:
            if task["relay_id"] == relay_id:
                if is_overlapping(start_time, end_time, task["start"], task["end"]):
                    return True
        # Check for overlap in one-time tasks
        for task in schedule_data["one_time_tasks"]:
            if task["relay_id"] == relay_id:
                if is_overlapping(start_time, end_time, task["start"], task["end"]):
                    return True
    return False
    
    
    # Dictionary to store scheduled jobs for cancellation
scheduled_jobs = {}
    
# Set daily schedule with overlap validation
def set_daily_schedule(start_time, end_time, relay_id):
    # ~ global schedule_data
    if not is_valid_time_range(start_time, end_time):
        publish_status(client, "Invalid time range: Start time must be before end time.")
        return
    with schedule_lock:
        if not has_overlap(start_time, end_time, relay_id):
            # Save the daily schedule
            schedule_data["daily_schedules"].append({"start": start_time, "end": end_time, "relay_id": relay_id})
            save_schedules()
        else:
            publish_status(client, "Schedule conflict: Task overlaps with an existing daily task.")
            return

    start_time_24 = convert_to_24hr_format(start_time)
    end_time_24 = convert_to_24hr_format(end_time)
    
    # Schedule the ON and OFF tasks
    on_job = schedule.every().day.at(start_time_24).do(lambda: schedule_on(relay_id, "scheduled", None))
    off_job = schedule.every().day.at(end_time_24).do(lambda: schedule_off(relay_id))
    
    # Store the jobs in the scheduled_jobs dictionary
    scheduled_jobs[(relay_id, start_time)] = on_job
    scheduled_jobs[(relay_id, end_time)] = off_job
    
    publish_status(client, f"Scheduled daily Motor {relay_id+1} ON at {start_time_24} and OFF at {end_time_24}")
        
        
# Reapplying schedules 
def reapply_daily_schedules():
    with schedule_lock:
        for daily_schedule in schedule_data["daily_schedules"]:
            start_time_24 = convert_to_24hr_format(daily_schedule["start"])
            end_time_24 = convert_to_24hr_format(daily_schedule["end"])
            relay_id = daily_schedule["relay_id"]
            # Use lambda to pass the relay_id argument to schedule_on and schedule_off
            schedule.every().day.at(start_time_24).do(lambda: schedule_on(relay_id, "scheduled", None))
            schedule.every().day.at(end_time_24).do(lambda: schedule_off(relay_id))
            
# Deadline Passed Check
def deadline_passed(date, start_time):
    current_date = datetime.now().date().isoformat()
    current_time = datetime.now().strftime('%I:%M %p')  # 12-hour format with AM/PM

    # Convert both times to datetime objects for comparison
    start_dt = datetime.strptime(start_time, '%I:%M %p')  # Start time with AM/PM format
    current_dt = datetime.strptime(current_time, '%I:%M %p')  # Current time with AM/PM
    
    if (date == current_date) and (start_dt < current_dt):
       return True
    

# Set one-time task with overlap validation
def set_one_time_task(date, start_time, end_time, relay_id):
    if not is_valid_time_range(start_time, end_time):
        publish_status(client, "Invalid time range: Start time must be before end time.")
        return
        
    if deadline_passed(date, start_time):
        publish_status(client, "Invalid time range: Start time already passed.")
        return 
    
    with schedule_lock:
        if has_overlap(start_time, end_time, relay_id, date):
            publish_status(client, "Schedule conflict: Task overlaps with an existing task on the specified date.")
            return
        else:
            # Add the one-time task to the list without scheduling a repeating event
            schedule_data["one_time_tasks"].append({
                "relay_id": relay_id,
                "date": date,
                "start": start_time,
                "end": end_time,
                "completed": False
            })
            save_schedules()
            publish_status(client, f"One-time task scheduled on {date} from {start_time} to {end_time} for Motor {relay_id+1}")

# To execute and safe removal of one-time tasks
def execute_and_remove_one_time_tasks():
    global is_relay_on
    global relay_sensors
    global check_thresholds
    global motors_index
    global count_readings
    today = datetime.now().date().isoformat()
    current_time = datetime.now().strftime('%H:%M')
    
    with schedule_lock:
        for task in schedule_data["one_time_tasks"][:]:  # Make a copy of the list to allow deletion
            task_date = task["date"]
            start_time_24 = convert_to_24hr_format(task["start"])
            end_time_24 = convert_to_24hr_format(task["end"])
            relay_id = task["relay_id"]

            # Check if the task is for today and has not been executed
            if task_date == today and not task.get("completed"):
                if start_time_24 <= current_time < end_time_24:
                    with relay_lock: 
                        if not is_relay_on[relay_id]:
                            GPIO.output(relay_pins[relay_id], 0) # Turn ON relay
                            publish_status(client, f"Motor {relay_id+1} turned-ON for one-time task scheduled at {start_time_24}")
                            is_relay_on[relay_id] = True  # Update the relay state
                            motors_index[relay_id] = relay_id
                        else:
                            with sensor_lock:
                                check_thresholds[relay_id] += relay_sensors[relay_id]
                                count_readings[relay_id]+=1
                                if count_readings[relay_id] == 10:
                                    # ~ publish(client, f"{check_array[relay_id]} and {count[relay_id]}")
                                    average = int(check_thresholds[relay_id])/10
                                    # ~ print(f"average {average}")
                                    if average < 250:
                                        GPIO.output(relay_pins[relay_id], 1) # Turn OFF relay
                                        is_relay_on[relay_id] = False
                                        motors_index[relay_id] = None
                                        publish_status(client, f"Motor {relay_id+1} Interrupted, Power-OFF")
                                        interrupted_time = datetime.now().strftime('%I:%M %p')
                                        task["completed"] = "interrupted"
                                        save_interrupted_task(task)
                                        schedule_data["one_time_tasks"].remove(task)
                                        save_schedules()
                                        check_thresholds[relay_id] = 0
                                        count_readings[relay_id] = 0
                                    else:
                                        check_thresholds[relay_id] = 0
                                        count_readings[relay_id] = 0
                elif current_time >= end_time_24:
                    with relay_lock: 
                        if is_relay_on[relay_id]:  # Only turn OFF if the relay is currently ON
                            GPIO.output(relay_pins[relay_id], 1)  # Turn OFF relay
                            publish_status(client, f"Motor {relay_id+1} turned-OFF for completed one-time task ending at {end_time_24}")
                            is_relay_on[relay_id] = False
                            motors_index[relay_id] = None
                            check_thresholds[relay_id] = 0
                            count_readings[relay_id] = 0
                
                        # Move task to completed, delete from one-time, and save updates
                        task["completed"] = True
                        save_completed_task(task)
                        schedule_data["one_time_tasks"].remove(task)
                        save_schedules()
########################### Scheduler Thread ###########################
# Modify your run_scheduler function to include the new function
def run_scheduler():
    while not stop_event.is_set():
        # Execute and remove one-time tasks if they match time criteria
        execute_and_remove_one_time_tasks()

        # Run daily tasks via schedule.run_pending()
        schedule.run_pending()
        
        time.sleep(1)
############################ Thread Init ###############################
# Start the scheduler in a separate thread
scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.daemon = True 
scheduler_thread.start()

# Thread for voltage reading
voltage_thread = threading.Thread(target=get_voltage)
voltage_thread.daemon = True  # Ensures thread exits when the main program exits
voltage_thread.start()
####################### Ensure Broker Connection #######################
# ~ # Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        publish_status(client, "Connected to broker")
        client.subscribe(topic_sub)
    else:
        print(f"Failed to connect, return code {rc}")
########################### MQTT Broker Setup ##########################
# Create an MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
####################### Schedule Recovery on Reboot ####################
# Load schedules at startup
load_schedules()

# reapplying all schedules in case of reboot 
if schedule.jobs:
    pass
    # ~ print("There are scheduled tasks.")
else:
    # ~ print("reapplying schedules")
    reapply_daily_schedules()
########################################################################
# Connect to the broker
client.connect(broker)

# Start the loop to process messages in the background
client.loop_start()
############################## Main Loop ###############################
# Main program loop
try:
    while not stop_event.is_set():
        # Pre-calculate time values outside the loop
        present_time = datetime.now().strftime('%I:%M %p')
        safe_window = 5
        limit_time_dt = datetime.now() + timedelta(minutes=safe_window)
        limit_time = limit_time_dt.strftime('%I:%M %p')

        # First loop: Check for any non-scheduled task conflict
        with flag_lock:
            for i, flag in enumerate(task_type_flag):
                if not flag:
                    continue  # Skip to the next task if flag is False

                # Check for overlap only if there's a flag set for this task
                with schedule_lock:
                    if has_overlap(present_time, limit_time, i):
                        with relay_lock:
                            if is_relay_on[i]:  # Only proceed if the relay is on
                                GPIO.output(relay_pins[i], 1)
                                # ~ publish_status(client, f"Motor {i+1} Turned-OFF - Upcoming Scheduled Task")
                                is_relay_on[i] = False  # Update relay status
                                check_thresholds[i] = 0
                                count_readings[i] = 0
                                task_type_flag[i] = False  # Reset the task flag

        # Second loop: Detect interruption due to power-off
        with schedule_lock, sensor_lock:
            for i, relay_on in enumerate(is_relay_on):
                if motors_index[i] is None and relay_on:
                    check_thresholds[i] += relay_sensors[i]
                    count_readings[i] += 1

                    if count_readings[i] == 10:
                        average = check_thresholds[i] / 10
                        print(average)
                        if average < 250:
                            interrupted_time = datetime.now().strftime('%Y-%m-%d %I:%M %p')
                            task = {"relay_id": i, "intruption_time": interrupted_time}
                            schedule_off(i)
                            # ~ publish_status(client, f"Motor {i+1} Interrupted, Power-OFF")
                            save_interrupted_task(task)
                            check_thresholds[i] = 0
                            count_readings[i] = 0
                        else:
                            check_thresholds[i] = 0
                            count_readings[i] = 0
                        

                        
        time.sleep(0.5)  # Keep the script running
finally:
    # Signal all threads to stop
    stop_event.set()
    
    # Join threads to ensure they close properly
    scheduler_thread.join()
    voltage_thread.join()
    
    client.loop_stop()
    client.disconnect()
    GPIO.cleanup()  # Clean up GPIO settings
    print("Disconnected from broker and GPIO cleaned up.")

