from flask import Flask, render_template, request, jsonify
import paho.mqtt.client as mqtt
from datetime import datetime
import threading

app = Flask(__name__)

# Initialize MQTT client
mqtt_client = mqtt.Client()

# Track the number of active connections
active_clients = 0
MAX_CLIENTS = 2  # Maximum number of allowed clients

# MQTT settings
MQTT_BROKER = "192.168.10.6"  # Change as needed
MQTT_PORT = 1883  # Standard port for MQTT

# Initialize a list to store messages (this will be passed to the frontend)
message_log = []

# MQTT message callback function
def on_message(client, userdata, message):
    msg = f"Received message on {message.topic}: {message.payload.decode()}\n"
    message_log.append(msg)  # Store the received message in the list

    # Optionally, print the message to the console or log it
    print(msg)

# Middleware to limit the number of active clients
@app.before_request
def limit_clients():
    global active_clients
    if active_clients >= MAX_CLIENTS:
        return jsonify(status="Too many clients connected, try again later."), 429

# Connect to MQTT broker
@app.route('/connect', methods=['POST'])
def connect_mqtt():
    global active_clients
    if mqtt_client.is_connected():  # Check if already connected
        return jsonify(status="Already connected to MQTT broker"), 400

    publish_topic = request.form['publish_topic']
    subscribe_topic = request.form['subscribe_topic']

    try:
        mqtt_client.on_message = on_message  # Assign the on_message function as the callback
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.subscribe(subscribe_topic)

        # Start the MQTT client loop in a separate thread to handle incoming messages asynchronously
        threading.Thread(target=mqtt_client.loop_start).start()

        # Increment the active clients count upon successful connection
        active_clients += 1

        return jsonify(status="Connected to MQTT broker")
    except Exception as e:
        return jsonify(status=f"Failed to connect: {str(e)}"), 500
    
# Disconnect from MQTT broker
@app.route('/disconnect', methods=['POST'])
def disconnect_mqtt():
    global active_clients
    try:
        mqtt_client.loop_stop()  # Stop the MQTT client loop
        mqtt_client.disconnect()  # Disconnect from the broker

        # Decrement the active clients count upon successful disconnection
        active_clients = max(0, active_clients - 1)

        return jsonify(status="Disconnected from MQTT broker")
    except Exception as e:
        return jsonify(status=f"Failed to disconnect: {str(e)}"), 500

# Root route to display the page (index.html)
@app.route('/')
def home():
    return render_template('index.html')

# Route to fetch the latest messages for display
@app.route('/get_messages', methods=['GET'])
def get_messages():
    return jsonify(messages=message_log)

# Route to clear the message log
@app.route('/clear_messages', methods=['POST'])
def clear_messages():
    global message_log
    message_log = []  # Clear the message log
    return jsonify(status="Messages cleared")

# Turn relay on
@app.route('/send_on', methods=['POST'])
def send_on():
    relay_id = request.form['relay_id']
    duration = request.form.get('duration', 'Immediate')
    command = f"on {relay_id} {duration}" if duration != 'Immediate' else f"on {relay_id}"
    mqtt_client.publish("relay", command)
    return jsonify(status=f"Relay {relay_id} turned on for {duration} minutes" if duration != 'Immediate' else f"Relay {relay_id} turned on immediately")

# Turn relay off
@app.route('/send_off', methods=['POST'])
def send_off():
    relay_id = request.form['relay_id']
    mqtt_client.publish("relay", f"off {relay_id}")
    return jsonify(status=f"Relay {relay_id} turned off")

# Schedule task
@app.route('/schedule_task', methods=['POST'])
def schedule_task():
    relay_id = request.form['relay_id']
    schedule_type = request.form['schedule_type']
    start_time = request.form['start_time']
    end_time = request.form['end_time']
    one_time_date = request.form.get('one_time_date', '')

    # Convert start_time and end_time to 12-hour format with AM/PM
    start_time_12hr = datetime.strptime(start_time, "%H:%M").strftime("%I:%M %p")
    end_time_12hr = datetime.strptime(end_time, "%H:%M").strftime("%I:%M %p")

    if schedule_type == 'Daily':
        command = f"schedule {relay_id} {start_time_12hr} {end_time_12hr}"
    else:  # One-Time
        command = f"schedule {relay_id} once {one_time_date} {start_time_12hr} {end_time_12hr}"

    mqtt_client.publish("relay", command)
    print(command)
    return jsonify(status=f"Scheduled task for Relay {relay_id}")

# Delete scheduled task
@app.route('/delete_schedule_task', methods=['POST'])
def delete_schedule_task():
    relay_id = request.form['relay_id']
    schedule_type = request.form['schedule_type']
    start_time = request.form.get('start_time', '')

    # Convert start_time to 12-hour format with AM/PM
    if start_time:
        start_time_obj = datetime.strptime(start_time, '%H:%M')  # Convert to 24-hour time object
        start_time = start_time_obj.strftime('%I:%M %p')  # Convert to 12-hour AM/PM format

    # Construct the command for deleting the schedule
    command = f"delete {relay_id} {schedule_type.lower()} {start_time}" if start_time else f"delete {relay_id} {schedule_type.lower()}"
    
    # Publish the delete command over MQTT
    mqtt_client.publish("relay", command)
    return jsonify(status=f"Deleted scheduled task for Relay {relay_id}")

# Schedule tasks
@app.route('/schedule_tasks', methods=['POST'])
def schedule_tasks():
    relay_id = request.form['relay_id']
    
    command = f"tasks {relay_id}"

    mqtt_client.publish("relay", command)
    return jsonify(status=f"Scheduled tasks for Relay {relay_id}")

# Middleware to handle client disconnects
@app.after_request
def decrement_client_count(response):
    global active_clients
    # Reduce active clients when a response is sent
    if response.status_code == 200:
        active_clients = max(0, active_clients - 1)
    return response

# if __name__ == '__main__':
#     app.run(debug=True)
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)

