import ast
import re
import signal
import selectors
import socket
import sys
import threading
import time
from queue import Queue
from pathlib import Path
from typing import Annotated, Any

import paho.mqtt.client as mqtt
import typer
from dateutil import parser
from decouple import config
from loguru import logger


ACT_HOL_PARTER = 33
CMD_CT = 39
ACT_LIVING = 40
ACT_ATELIER = 42
PMP_PARTER = 43
ACT_ETAJ_BABACI = 44
ACT_FLAVIA_HOL = 45
PMP_ETAJ = 47

OUTPUTS = {
    ACT_HOL_PARTER: {
        "name": "ACT Hol Parter",
        "topic": "openmotics/output/33/state",
        "state": 0,
        "timestamp": 0,
    },
    CMD_CT: {
        "name": "CMD CT",
        "topic": "openmotics/output/39/state",
        "state": 0,
        "timestamp": 0,
    },
    ACT_LIVING: {
        "name": "ACT Living",
        "topic": "openmotics/output/40/state",
        "state": 0,
        "timestamp": 0,
    },
    ACT_ATELIER: {
        "name": "ACT Atelier",
        "topic": "openmotics/output/42/state",
        "state": 0,
        "timestamp": 0,
    },
    PMP_PARTER: {
        "name": "PMP Parter",
        "topic": "openmotics/output/43/state",
        "state": 0,
        "timestamp": 0,
    },
    ACT_ETAJ_BABACI: {
        "name": "ACT Etaj Babaci",
        "topic": "openmotics/output/44/state",
        "state": 0,
        "timestamp": 0,
    },
    ACT_FLAVIA_HOL: {
        "name": "ACT Flavia+Hol",
        "topic": "openmotics/output/45/state",
        "state": 0,
        "timestamp": 0,
    },
    PMP_ETAJ: {
        "name": "PMP Etaj",
        "topic": "openmotics/output/47/state",
        "state": 0,
        "timestamp": 0,
    },
}

BASE_DIR = Path(__file__).resolve().parent.parent

RETRY_LIMIT = 5

monitor_thread_started: threading.Event = threading.Event()
shutdown_event: threading.Event = threading.Event()
output_state_queue = Queue()
selector = selectors.DefaultSelector()

app = typer.Typer()


class MQTTLogConfig:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)

        return cls._instance

    def __init__(self, daemonize: bool = False) -> None:
        self.is_daemon = daemonize
        self.log_level_console: str = str(config("LOG_LEVEL_CONSOLE", default="INFO"))
        self.log_level_file: str = str(config("LOG_LEVEL_FILE", default="INFO"))
        self.log_file_rotation: str = str(config("LOG_ROTATION", default="10 MB"))
        self.log_file: Path = Path(f"{BASE_DIR}/log") / str(config("LOG_FILE"))
        self.log_file.parent.mkdir(exist_ok=True, parents=True)

        self.std_err_logger = None
        self.file_logger = None

        self.format_console: str = str(
            config(
                "LOG_FORMAT_CONSOLE",
                default=(
                    "<green>{time:YYYY.MM.DD HH:mm:ss}</green> | "
                    "{level} {level.icon} | {function: >20} | "
                    "<level>{message}</level>"
                ),
            )
        )
        self.format_logfile: str = str(
            config(
                "LOG_FORMAT_FILE",
                default="{time:YYYY.MM.DD HH:mm:ss} | {level} | {module}:{thread}:{function}:{line} | <level>{message}</level>",
            )
        )

    def _setup_stderr_logger(self) -> None:
        # add stderr logger only if not in daemon mode
        if not self.is_daemon:
            self.std_err_logger = logger.add(sys.stderr, level=self.log_level_console, format=self.format_console)

    def _setup_file_logger(self) -> None:
        if self.file_logger is not None:
            logger.remove(self.file_logger)

        self.file_logger = logger.add(
            self.log_file,
            level=self.log_level_file,
            rotation=self.log_file_rotation,
            format=self.format_logfile,
            backtrace=True,
        )

    def setup_logging(self) -> None:
        logger.remove()

        self._setup_stderr_logger()
        self._setup_file_logger()

    def update_logging_level(self, new_level: str) -> None:
        if self.log_level_file == new_level:
            return

        self.log_level_file = new_level
        self._setup_file_logger()


def check_and_close_socket():
    logger.info("Check for dangling socket connections ...")
    socket_path = str(config("SOCKET_PATH", default="/tmp/mqtt_automation.sock"))
    if Path(socket_path).exists():
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(socket_path)
            logger.info("Socket exists and is open. Closing it.")
            sock.close()
            logger.info("Socket closed")
        except ConnectionRefusedError:
            logger.info("Socket is already closed")
    else:
        logger.info("Socket does not exist")

    Path(socket_path).unlink(missing_ok=True)


def read_from_socket(conn, mask):
    client_address = conn.getpeername()
    logger.info(f"Read from socket {conn} from {client_address}")
    data = conn.recv(1024)
    if not data:
        logger.info(f"Socket connection {conn} from {client_address} closed")
        selector.unregister(conn)
        conn.close()
    else:
        command = data.decode("utf-8")
        if command == "status":
            if monitor_thread_started.is_set():
                conn.sendall(b"Running")
            else:
                conn.sendall(b"Not running")
        else:
            conn.sendall(b"Unknown command")


def accept_socket_connection(sock, mask):
    """Callback for new connections"""
    try:
        conn, address = sock.accept()
        logger.info("Accepted socket connection ...")
        conn.setblocking(False)

        selector.register(conn, selectors.EVENT_READ, read_from_socket)
    except Exception as e:
        logger.exception(f"Error handling socket connection: {e}")


def start_socket_listener():
    socket_path = str(config("SOCKET_PATH", default="/tmp/mqtt_automation.sock"))
    check_and_close_socket()

    retries = 0
    while not shutdown_event.is_set() and retries < RETRY_LIMIT:
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.bind(socket_path)
            sock.setblocking(False)
            sock.listen(5)
            logger.info(f"Socket listener started on {socket_path}")

            selector.register(sock, selectors.EVENT_READ, accept_socket_connection)

            while not shutdown_event.is_set():
                # logger.debug("Socket is waiting for events ...")
                events = selector.select(timeout=1)  # just one second timeout to prevent blocking
                for key, mask in events:
                    callback = key.data
                    callback(key.fileobj, mask)
        except OSError as e:
            retries += 1
            logger.error(f"Socket error ({retries}/{RETRY_LIMIT}): {e.errno} {e.strerror}")
            if retries >= RETRY_LIMIT:
                logger.error("Maximum retries reached. Exiting socket listener.")
                break

            time.sleep(2**retries)  # Exponential backoff
        finally:
            if Path(socket_path).exists():
                Path(socket_path).unlink(missing_ok=True)
                logger.info(f"Socket {socket_path} cleaned up")

    threading.main_thread().join()
    logger.info("Socket listener stopped")


def debug_mqtt_message(msg: mqtt.MQTTMessage) -> None:
    """
    Print the contents of an MQTT message to the log.

    :param msg: The MQTT message to print.
    :type msg: mqtt.MQTTMessage
    :return: None
    """
    logger.debug(
        f"Topic = {msg.topic};"
        # f"Properties = {msg.properties};"
        # f"Duplicate = {msg.dup};"
        # f"info = {msg.info};"
        # f"mid = {msg.mid};"
        f"payload = {msg.payload.decode()};"
        # f"qos = {msg.qos};"
        f"retain = {msg.retain};"
        f"state = {msg.state};"
        f"timestamp = {msg.timestamp}"
    )


def _evaluate_output_state(outputs: dict[int, dict[str, Any]], output_list: list) -> bool:
    """Helper function to evaluate output(s) state
    If any of the outputs is ON, return True
    else return False
    """
    return any(outputs[id]["state"] == 100 for id in output_list)


def ground_floor_state(outputs: dict[int, dict[str, Any]]):
    logger.info(
        f"Ground Floor Actuators: ACT_ATELIER={outputs[ACT_ATELIER]['state']}; "
        f"ACT_HOL_PARTER={outputs[ACT_HOL_PARTER]['state']}; "
        f"ACT_LIVING={outputs[ACT_LIVING]['state']}; "
        f"PMP_PARTER={outputs[PMP_PARTER]['state']}; "
        f"CMD_CT={outputs[CMD_CT]['state']}; "
    )

    return _evaluate_output_state(outputs, [ACT_ATELIER, ACT_HOL_PARTER, ACT_LIVING])


def upper_floor_state(outputs: dict[int, dict[str, Any]]):
    logger.info(
        f"Upper Floor Actuators: ACT_ETAJ_BABACI={outputs[ACT_ETAJ_BABACI]['state']}; "
        f"ACT_FLAVIA_HOL={outputs[ACT_FLAVIA_HOL]['state']}; "
        f"PMP_ETAJ={outputs[PMP_ETAJ]['state']}; "
        f"CMD_CT={outputs[CMD_CT]['state']}; "
    )

    return _evaluate_output_state(outputs, [ACT_ETAJ_BABACI, ACT_FLAVIA_HOL])


def pumps_state(outputs: dict[int, dict[str, Any]]):
    logger.info(
        f"Pumps State: PMP_PARTER={outputs[PMP_PARTER]['state']}; "
        f"PMP_ETAJ={outputs[PMP_ETAJ]['state']}; "
        f"CMD_CT={outputs[CMD_CT]['state']}; "
    )

    return _evaluate_output_state(outputs, [PMP_PARTER, PMP_ETAJ])


def monitor_outputs_state(client: mqtt.Client) -> None:
    pump_delay = config("PUMP_DELAY_SECONDS", cast=int)

    logger.info("Monitoring outputs state started ...")
    while not shutdown_event.is_set():
        try:
            id, outputs = output_state_queue.get()
            logger.info(f"State change received for output {id}")
        except Exception as ex:
            logger.exception(ex)

        logger.debug("Checking outputs state ...")
        for output in outputs:
            logger.debug(outputs[output])
            if outputs[output]["state"] == 100:
                logger.info(f"{outputs[output]['name']} is ON")

        if ground_floor_state(outputs):
            """
            Here we have one of the actuators on the ground floor turned on;
            This should turn on the pump on the ground floor (pump_delay)
            Should we do this in yet another thread? or just sleep for two minutes and then check one more time?
            """
            if outputs[PMP_PARTER]["state"] == 0:
                time.sleep(pump_delay)
                if ground_floor_state(outputs):
                    logger.info("Ground floor actuator ON, TURNING ON GROUND FLOOR PUMP")
                    client.publish(f"openmotics/output/{PMP_PARTER}/set", "100")
        else:
            if outputs[PMP_PARTER]["state"] != 0:
                # no actuator is running so the pump should be off
                logger.info("Ground floor actuators off - stopping the pump")
                client.publish(f"openmotics/output/{PMP_PARTER}/set", "0")

        if upper_floor_state(outputs):
            if outputs[PMP_ETAJ]["state"] == 0:
                time.sleep(pump_delay)
                if upper_floor_state(outputs):
                    logger.info("Upper floor ACTUATOR ON, TURNING ON UPPER FLOOR PUMP")
                    client.publish(f"openmotics/output/{PMP_ETAJ}/set", "100")
        else:
            if outputs[PMP_ETAJ]["state"] != 0:
                logger.info("Upper floor actuators off - stopping the pump")
                client.publish(f"openmotics/output/{PMP_ETAJ}/set", "0")

        if pumps_state(outputs):
            if outputs[CMD_CT]["state"] == 0:
                # turn on the heating
                logger.info("Turning on the heating ...")
                client.publish(f"openmotics/output/{CMD_CT}/set", "100")
        else:
            if outputs[CMD_CT]["state"] != 0:
                logger.info("Turning off the heating ...")
                client.publish(f"openmotics/output/{CMD_CT}/set", "0")

        output_state_queue.task_done()
        # no need to sleep since we wait on the queue to supply next message
        # time.sleep(10)

    threading.main_thread.join()
    logger.info("Monitoring outputs state thread stopped ...")


def on_log(client: mqtt.Client, userdata, client_log_level, messages):
    if client_log_level == mqtt.LogLevel.MQTT_LOG_DEBUG:
        logger.debug(messages)
    elif client_log_level == mqtt.LogLevel.MQTT_LOG_INFO:
        logger.info(messages)
    elif client_log_level == mqtt.LogLevel.MQTT_LOG_WARNING:
        logger.warning(messages)
    elif client_log_level == mqtt.LogLevel.MQTT_LOG_ERR:
        logger.error(messages)
    elif client_log_level == mqtt.LogLevel.MQTT_LOG_NOTICE:
        logger.info(messages)


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
    if reason_code == 0:
        logger.info("Connected successfully to the broker")
        logger.info("Starting the outputs monitoring thread")

        # we need this check because on reconnect of the MQTT client we don't want to start a new thread
        if not monitor_thread_started.is_set():
            threading.Thread(target=monitor_outputs_state, args=(client,)).start()
            monitor_thread_started.set()
    else:
        logger.error(f"Connection failed with code {reason_code}")


def on_message(client: mqtt.Client, userdata: any, msg: mqtt.MQTTMessage) -> None:
    logger.debug(f"{msg.topic}: {msg.payload.decode()}")

    topic = msg.topic
    debug_mqtt_message(msg)

    pattern = re.compile(r"openmotics/output/(\d+)/state")
    if not pattern.match(topic) or not topic:
        return

    payload: dict = ast.literal_eval(msg.payload.decode()) if msg.payload is not None else msg.payload.decode()
    id: int = payload["id"]

    if id in OUTPUTS:
        logger.debug(f"id = {id} found in OUTPUTS")
        old_state = OUTPUTS[id]["state"]
        new_state = payload["value"]
        OUTPUTS[id]["state"] = new_state
        OUTPUTS[id]["timestamp"] = (
            parser.parse(payload["timestamp"]) if payload["timestamp"] is not None else payload["timestamp"]
        )
        logger.debug(f"old_state={old_state}, new_state={new_state}")
        if old_state != new_state:
            logger.info(f"Output {OUTPUTS[id]['name']} state changed from {old_state} to {new_state}")

            try:
                output_state_queue.put((id, OUTPUTS))
            except Exception as ex:
                logger.exception(ex)


def run_mqtt_client(broker: str, port: int, topic: str, user: str, pwd: str):
    """Start the MQTT client."""

    client_id = "mqtt_client_" + socket.gethostname()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_log = on_log

    logger.info(f"Connecting to MQTT broker at {broker}:{port}")
    client.username_pw_set(username=user, password=pwd)
    client.connect(broker, port, 60)
    client.subscribe(topic)

    client.loop_start()
    # this is part of the main thread and we need to sleep some here, otherwise we just burn CPU
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("Received KeyboardInterrupt, stopping the MQTT client...")
    finally:
        client.loop_stop()
        client.disconnect()
        logger.info("MQTT client disconnected")


def clear_queue(queue: Queue):
    try:
        while not queue.empty():
            queue.get_nowait()
            queue.task_done()
    except Exception as e:
        logger.exception(f"Error while clearing queue: {e}")


def program_exit(signum, frame):
    logger.info(f"Stopping the MQTT Automation application... Received signal: {signum} {signal.strsignal(signum)}")
    logger.info("Stopping the threads ...")

    # this stops all threads by setting the shutdown_event
    shutdown_event.set()

    logger.info("Attempting to shut down the Queue...")
    # wait for the queue to finish, though the thread is already stopped
    clear_queue(output_state_queue)

    logger.info("Exiting...")
    sys.exit(0)


@app.command()
def stop():
    """Stop the MQTT Automation application."""
    program_exit(signal.SIGINT, None)


@app.command()
def status():
    """Get the status of the daemon."""
    try:
        socket_path = str(config("SOCKET_PATH", default="/tmp/mqtt_automation.sock"))
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5)  # Timeout for socket connection
        sock.connect(socket_path)
        sock.sendall(b"status")
        response = sock.recv(1024)
        print(response.decode("utf-8"))
    except OSError as e:
        logger.error(f"Error connecting to socket: {e}")


@app.command()
def start(
    broker: Annotated[str, typer.Option(help="MQTT broker address")] = str(config("MQTT_HOST")),
    port: Annotated[int, typer.Option(help="MQTT broker port")] = int(config("MQTT_PORT")),
    topic: Annotated[str, typer.Option(help="MQTT topic to subscribe to")] = "openmotics/output/#",
    user: Annotated[str, typer.Option(help="MQTT username")] = str(config("MQTT_USER")),
    pwd: Annotated[str, typer.Option(help="MQTT password")] = "",
):
    """Start the MQTT Automation application."""
    if pwd.strip() == "":
        pwd = str(config("MQTT_PASS"))

    MQTTLogConfig().setup_logging()
    signal.signal(signal.SIGTERM, program_exit)
    signal.signal(signal.SIGINT, program_exit)
    logger.info("Running MQTT Automation")
    threading.Thread(target=start_socket_listener).start()
    run_mqtt_client(broker, port, topic, user, pwd)


if __name__ == "__main__":
    app()
