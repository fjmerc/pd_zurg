from base import *
from utils.logger import SubprocessLogger


# Global registry of all tracked processes for graceful shutdown
_process_registry = []
_registry_lock = threading.Lock()


def register_process(handler, process_name, key_type=None):
    with _registry_lock:
        _process_registry.append((handler, process_name, key_type))


def shutdown_all_processes(logger):
    with _registry_lock:
        for handler, process_name, key_type in reversed(_process_registry):
            try:
                if handler.process and handler.process.poll() is None:
                    desc = f"{process_name} w/ {key_type}" if key_type else process_name
                    logger.info(f"Terminating {desc} (pid {handler.process.pid})...")
                    handler.process.terminate()
                    try:
                        handler.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"{desc} did not exit gracefully, killing...")
                        handler.process.kill()
                        handler.process.wait(timeout=5)
            except Exception as e:
                logger.error(f"Error shutting down process: {e}")
        _process_registry.clear()


class ProcessHandler:
    def __init__(self, logger):
        self.logger = logger
        self.process = None
        self.subprocess_logger = None
        self.stdout = ""
        self.stderr = ""
        self.returncode = None

    def start_process(self, process_name, config_dir, command, key_type=None, suppress_logging=False):
        try:
            if key_type is not None:
                self.logger.info(f"Starting {process_name} w/ {key_type}")
                process_description = f"{process_name} w/ {key_type}"
            else:
                self.logger.info(f"Starting {process_name}")
                process_description = f"{process_name}"
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                cwd=config_dir,
                universal_newlines=True,
                bufsize=1
            )
            if not suppress_logging:
                self.subprocess_logger = SubprocessLogger(self.logger, f"{process_description}")
                self.subprocess_logger.start_logging_stdout(self.process)
                self.subprocess_logger.start_monitoring_stderr(self.process, key_type, process_name)
            register_process(self, process_name, key_type)
            return self.process
        except Exception as e:
            self.logger.error(f"Error running subprocess for {process_description}: {e}")
            return None

    def wait(self):
        if self.process:
            self.stdout, self.stderr = self.process.communicate()
            self.returncode = self.process.returncode
            self.stdout = self.stdout.strip() if self.stdout else ""
            self.stderr = self.stderr.strip() if self.stderr else ""           
            if self.subprocess_logger:
                self.subprocess_logger.stop_logging_stdout()
                self.subprocess_logger.stop_monitoring_stderr()

    def stop_process(self, process_name, key_type=None):
        try:
            if key_type:
                self.logger.info(f"Stopping {process_name} w/ {key_type}")
                process_description = f"{process_name} w/ {key_type}"
            else:
                self.logger.info(f"Stopping {process_name}")
                process_description = f"{process_name}"
            if self.process:
                self.process.kill()
                if self.subprocess_logger:
                    self.subprocess_logger.stop_logging_stdout()
                    self.subprocess_logger.stop_monitoring_stderr()
        except Exception as e:
            self.logger.error(f"Error stopping subprocess for {process_description}: {e}")