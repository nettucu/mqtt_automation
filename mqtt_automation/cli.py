import ast
import re
import signal
import socket
import sys
import threading
import time
from queue import Queue
from pathlib import Path
from typing import Annotated, Any

import daemon
import daemon.pidfile
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
    ACT_HOL_PARTER: {"name": "ACT Hol Parter", "topic": "openmotics/output/33/state", "state": 0, "timestamp": 0},
    CMD_CT: {"name": "CMD CT", "topic": "openmotics/output/39/state", "state": 0, "timestamp": 0},
    ACT_LIVING: {"name": "ACT Living", "topic": "openmotics/output/40/state", "state": 0, "timestamp": 0},
    ACT_ATELIER: {"name": "ACT Atelier", "topic": "openmotics/output/42/state", "state": 0, "timestamp": 0},
    PMP_PARTER: {"name": "PMP Parter", "topic": "openmotics/output/43/state", "state": 0, "timestamp": 0},
    ACT_ETAJ_BABACI: {"name": "ACT Etaj Babaci", "topic": "openmotics/output/44/state", "state": 0, "timestamp": 0},
    ACT_FLAVIA_HOL: {"name": "ACT Flavia+Hol", "topic": "openmotics/output/45/state", "state": 0, "timestamp": 0},
    PMP_ETAJ: {"name": "PMP Etaj", "topic": "openmotics/output/47/state", "state": 0, "timestamp": 0},
}

BASE_DIR = Path(__file__).resolve().parent.parent
MONITOR_OUTPUTS_STATE = True
MONITOR_OUTPUTS_STATE_THREAD_STARTED = False
LOG_FILE_DESCRIPTOR = None

output_state_queue = Queue()
app = typer.Typer()


class MQTTLogConfig:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)

        return cls._instance

    def __init__(self, daemonize: bool = False) -> None:
        self.is_daemon = daemonize
        self.log_level_console = config("LOG_LEVEL_CONSOLE", default="INFO")
        self.log_level_file = config("LOG_LEVEL_FILE", default="INFO")
        self.log_file_rotation = config("LOG_ROTATION", default="10 MB")
        self.log_file = Path(f"{BASE_DIR}/log") / config("LOG_FILE")
        self.log_file.parent.mkdir(exist_ok=True, parents=True)

        self.std_err_logger = None
        self.file_logger = None

        self.format_console = config(
            "LOG_FORMAT_CONSOLE",
            default=(
                "<green>{time:YYYY.MM.DD HH:mm:ss}</green> | "
                "{level} {level.icon} | {function: >20} | "
                "<level>{message}</level>"
            ),
        )
        self.format_logfile = config(
            "LOG_FORMAT_FILE",
            default="{time:YYYY.MM.DD HH:mm:ss} | {level} | {module}:{thread}:{function}:{line} | <level>{message}</level>",
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
    logger.info("check_and_close_socket ...")
    if Path.exists(config("SOCKET_PATH")):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(config("SOCKET_PATH"))
            logger.info("Socket exists and is open. Closing it.")
            sock.close()
            logger.info("Socket closed")
        except ConnectionRefusedError:
            logger.info("Socket is already closed")
    else:
        logger.info("Socket does not exist")


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


def _evaluate_output_state(outputs: list) -> bool:
    """Helper function to evaluate output(s) state
    If any of the outputs is ON, return True
    else return False
    """
    return any(OUTPUTS[id]["state"] == 100 for id in outputs)


def ground_floor_state(outputs: dict[int, dict[str, Any]]):
    # global OUTPUTS

    logger.info(
        f"Ground Floor Actuators: ACT_ATELIER={outputs[ACT_ATELIER]['state']}; "
        f"ACT_HOL_PARTER={outputs[ACT_HOL_PARTER]['state']}; "
        f"ACT_LIVING={outputs[ACT_LIVING]['state']}; "
        f"PMP_PARTER={outputs[PMP_PARTER]['state']}; "
        f"CMD_CT={outputs[CMD_CT]['state']}; "
    )

    return _evaluate_output_state([ACT_ATELIER, ACT_HOL_PARTER, ACT_LIVING])


def upper_floor_state(outputs: dict[int, dict[str, Any]]):
    # global OUTPUTS

    logger.info(
        f"Upper Floor Actuators: ACT_ETAJ_BABACI={outputs[ACT_ETAJ_BABACI]['state']}; "
        f"ACT_FLAVIA_HOL={outputs[ACT_FLAVIA_HOL]['state']}; "
        f"PMP_ETAJ={outputs[PMP_ETAJ]['state']}; "
        f"CMD_CT={outputs[CMD_CT]['state']}; "
    )

    return _evaluate_output_state([ACT_ETAJ_BABACI, ACT_FLAVIA_HOL])


def pumps_state(outputs: dict[int, dict[str, Any]]):
    # global OUTPUTS

    logger.info(
        f"Pumps State: PMP_PARTER={outputs[PMP_PARTER]['state']}; "
        f"PMP_ETAJ={outputs[PMP_ETAJ]['state']}; "
        f"CMD_CT={outputs[CMD_CT]['state']}; "
    )

    return _evaluate_output_state([PMP_PARTER, PMP_ETAJ])


def monitor_outputs_state(client: mqtt.Client) -> None:
    pump_delay = config("PUMP_DELAY_SECONDS", cast=int)
    while MONITOR_OUTPUTS_STATE:
        id, outputs = output_state_queue.get()
        logger.debug("Checking outputs state ...")
        for output in outputs:
            logger.debug(outputs[output])
            if outputs[output]["state"] == 100:
                logger.info(f"{OUTPUTS[output]['name']} is ON")

        if ground_floor_state(outputs):
            """
            Here we have one of the actuators on the ground floor turned on;
            This should turn on the pump on the ground floor (2 minutes delay)
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
        time.sleep(10)


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
    global MONITOR_OUTPUTS_STATE_THREAD_STARTED

    if reason_code == 0:
        logger.info("Connected successfully to the broker")
        logger.info("Starting the check_outputs_state thread")

        # we need this check because on reconnect of the MQTT client we don't want to start a new thread
        if MONITOR_OUTPUTS_STATE_THREAD_STARTED is False:
            threading.Thread(target=monitor_outputs_state, args=(client,), daemon=True).start()
            MONITOR_OUTPUTS_STATE_THREAD_STARTED = True
    else:
        logger.error(f"Connection failed with code {reason_code}")


def on_message(client: mqtt.Client, userdata: any, msg: mqtt.MQTTMessage) -> None:
    # logger.info(f"{msg.topic}: {msg.payload.decode()}")

    topic = msg.topic
    if config("DEBUG", default=False, cast=bool):
        debug_mqtt_message(msg)

    pattern = re.compile(r"openmotics/output/(\d+)/state")
    if not pattern.match(topic) or not topic:
        return

    payload: dict = ast.literal_eval(msg.payload.decode()) if msg.payload is not None else msg.payload.decode()
    id: int = payload["id"]

    if id in OUTPUTS:
        old_state = OUTPUTS[id]["state"]
        new_state = payload["value"]
        OUTPUTS[id]["state"] = new_state
        OUTPUTS[id]["timestamp"] = (
            parser.parse(payload["timestamp"]) if payload["timestamp"] is not None else payload["timestamp"]
        )
        if old_state != new_state:
            logger.info(f"Output {OUTPUTS[id]['name']} state changed from {old_state} to {new_state}")
            output_state_queue.put((id, OUTPUTS))


def run_mqtt_client(broker: str, port: int, topic: str, user: str, pwd: str):
    """Start the MQTT client."""

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt_client_zeppelin")
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_log = on_log

    logger.info(f"Connecting to MQTT broker at {broker}:{port}")
    client.username_pw_set(username=user, password=pwd)
    client.connect(broker, port, 60)
    client.subscribe(topic)

    client.loop_start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()
        logger.info("MQTT client disconnected")


def program_exit(signal, frame):
    global MONITOR_OUTPUTS_STATE

    logger.info(f"Stopping the MQTT Automation application... Received signal: {signal}")
    logger.info("Stopping the monitor_outputs_state thread...")
    # this stops the monitor thread
    MONITOR_OUTPUTS_STATE = False

    logger.info("Closing the socket...")
    # close the socket
    check_and_close_socket()

    logger.info("Shutting down the Queue...")
    # wait for the queue to finish, though the thread is already stopped
    output_state_queue.join()

    # TODO: close down the MQTT client

    logger.info("Exiting...")
    sys.exit(0)


@app.command()
def stop():
    """Stop the MQTT Automation application."""
    check_and_close_socket()


@app.command()
def start(
    daemonize: Annotated[bool, typer.Option(help="Run as a daemon")] = False,
    broker: Annotated[str, typer.Option(help="MQTT broker address")] = config("MQTT_HOST"),
    port: Annotated[int, typer.Option(help="MQTT broker port")] = config("MQTT_PORT"),
    topic: Annotated[str, typer.Option(help="MQTT topic to subscribe to")] = "openmotics/output/#",
    user: Annotated[str, typer.Option(help="MQTT username")] = config("MQTT_USER"),
    pwd: Annotated[str, typer.Option(help="MQTT password")] = "",
):
    """Start the MQTT Automation application."""
    if pwd.strip() == "":
        pwd = config("MQTT_PASS")

    if daemonize:
        """Run the MQTT client as a daemon."""
        with daemon.DaemonContext(
            working_directory=Path.cwd(),
            pidfile=daemon.pidfile.PIDLockFile(config("PID_FILE")),
            # files_preserve=[config("LOG_FILE")],
            detach_process=True,
            stderr=sys.stderr,
            # stdout=sys.stdout,
            # stdout = config("LOG_FILE"),
        ) as context:
            context.signal_map = {
                signal.SIGTERM: context.terminate,
                signal.SIGINT: context.terminate,
                signal.SIGHUP: context.terminate,
            }
            # called again because it opens various files and we can't have those open before going into daemon mode
            MQTTLogConfig(daemonize).setup_logging()
            logger.info("Running MQTT Automation in the background.")
            run_mqtt_client(broker, port, topic, user, pwd)
    else:
        MQTTLogConfig().setup_logging()
        logger.info("Running MQTT Automation in the foreground.")
        run_mqtt_client(broker, port, topic, user, pwd)


if __name__ == "__main__":
    app()
