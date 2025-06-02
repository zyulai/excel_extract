import logging
import asyncio
import paramiko
from io import BytesIO
import os
import re
import time
from inspect import signature
from stat import S_ISDIR
from typing import Tuple, Optional

from paramiko import SSHClient, SFTPClient, SSHException


def connection_retry(func):
    """Decorator to retry on SSH disconnection only"""

    def wrapper(self, *args, **kwargs):
        sig = signature(func)
        bound_args = sig.bind(self, *args, **kwargs)
        bound_args.apply_defaults()
        retry_count = bound_args.arguments.get("retry_count", 0)

        attempts = 0
        while True:
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                attempts += 1
                # Reconnect only on specific network/connection errors
                if isinstance(e, (SSHException, EOFError)) and attempts <= retry_count:  # noqa
                    logging.warning(
                        f"Connection lost ({
                            attempts}/{retry_count}). Reconnecting..."
                    )
                    self._reconnect()
                else:
                    # Non-connection errors or max retries exceeded
                    if isinstance(e, SSHException):
                        logging.error(f"SSH error: {str(e)}")
                    elif not isinstance(e, (EOFError, SSHException)) and attempts > 1:  # noqa
                        logging.error(f"Operation failed after {
                                      attempts} attempts")
                    raise

    return wrapper


class AsyncParamikoWrapper:
    def __init__(self, client: paramiko.SSHClient):
        self._client = client
        self._transport = self._client.get_transport()

    async def exec_command(
        self, command: str, timeout: int = 3600
    ) -> Tuple[bytes, bytes, int]:
        channel = self._transport.open_session()

        try:
            channel.setblocking(False)

            channel.exec_command(command)

            stdout = BytesIO()
            stderr = BytesIO()

            while not channel.exit_status_ready():
                if timeout > 0 and channel.get_idle_time() > timeout:
                    raise TimeoutError(
                        f"Command {command} timed out after {timeout} seconds"
                    )

                await self._read_channel(channel, stdout, stderr)
                await asyncio.sleep(0.1)

            while channel.recv_ready() or channel.recv_stderr_ready():
                await self._read_channel(channel, stdout, stderr)

            exit_status = channel.recv_exit_status()

            return stdout.getvalue(), stderr.getvalue(), exit_status

        finally:
            channel.close()

    async def _read_channel(
        self,
        channel: paramiko.Channel,
        stdout_buf: BytesIO,
        stderr_buf: BytesIO,
    ) -> None:
        loop = asyncio.get_running_loop()

        while channel.recv_ready():
            recv_some = await loop.run_in_executor(None, lambda: channel.recv(4096))  # noqa
            stdout_buf.write(recv_some)

        while channel.recv_stderr_ready():
            recv_some = await loop.run_in_executor(
                None, lambda: channel.recv_stderr(4096)
            )
            stderr_buf.write(recv_some)


class Board:
    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = "root",
        password: str = "",
        retries: int = 3,
    ):
        self.ip = host
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._retries = retries

        self._client: Optional[SSHClient] = None
        self._sftp: Optional[SFTPClient] = None
        self._connect()

    def __enter__(self) -> "Board":
        if not self.is_connected():
            self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @property
    def is_connected(self) -> bool:
        """Check if SSH connection is active"""
        if not self._client:
            return False
        transport = self._client.get_transport() if self._client else None
        return transport and transport.is_active()

    def _connect(self, retry_count: Optional[int] = None) -> None:
        """Establish SSH connection with retry logic"""
        retries = retry_count or self._retries
        for attempt in range(retries + 1):
            try:
                self._client = paramiko.SSHClient()
                self._client.set_missing_host_key_policy(
                    paramiko.AutoAddPolicy())

                if self._password:
                    self._client.connect(
                        hostname=self._host,
                        port=self._port,
                        username=self._username,
                        password=self._password,
                        timeout=30,
                        banner_timeout=30,
                    )
                else:
                    # Attempt key-based auth
                    self._client.connect(
                        hostname=self._host,
                        port=self._port,
                        username=self._username,
                        timeout=30,
                        banner_timeout=30,
                    )

                self._sftp = self._client.open_sftp()
                logging.info(f"Connected to {self._host}:{
                             self._port} successfully")
                return
            except Exception as e:
                self.close()
                if attempt >= retries:
                    if isinstance(e, paramiko.AuthenticationException):
                        raise PermissionError(
                            f"Authentication failed for {
                                self._username}@{self._host}"
                        ) from e
                    elif isinstance(e, paramiko.SSHException):
                        raise ConnectionError(
                            f"SSH connection failed: {str(e)}") from e
                    else:
                        raise ConnectionError(
                            f"Connection failed: {str(e)}") from e
                logging.warning(
                    f"Connection attempt {
                        attempt + 1}/{retries} failed. Retrying..."
                )
                time.sleep(2**attempt)  # Exponential backoff

    def _reconnect(self) -> None:
        """Forcibly reestablish connection"""
        self.close()
        self._connect()

    def close(self) -> None:
        """Clean up all connections"""
        for conn in [self._sftp, self._client]:
            if conn:
                try:
                    conn.close()
                except OSError:
                    pass
        self._sftp = None
        self._client = None

    @connection_retry
    def download_directory(
        self,
        remote_dir: str,
        local_dir: str,
        pattern: str = "",
        retry_count: int = 3,
    ) -> None:
        """Download files from remote directory (non-recursive)"""
        os.makedirs(local_dir, exist_ok=True)

        for filename in self._sftp.listdir(remote_dir):
            # Apply regex filter
            if pattern and not re.match(pattern, filename):
                continue

            remote_path = os.path.join(remote_dir, filename)
            local_path = os.path.join(local_dir, filename)

            # Skip directories
            try:
                if S_ISDIR(self._sftp.stat(remote_path).st_mode):
                    logging.debug(f"Skipping directory: {remote_path}")
                    continue
            except OSError:
                continue  # Skip missing files

            self._sftp.get(remote_path, local_path)
            logging.info(f"Downloaded {remote_path} -> {local_path}")

    @connection_retry
    def download(self, remote_path: str, local_path: str, retry_count: int = 3) -> None:  # noqa
        try:
            self._sftp.get(remote_path, local_path)
            logging.info(f"Downloaded {remote_path} to {local_path}")
        except OSError as e:
            raise FileNotFoundError(f"Remote file not found: {
                                    remote_path}") from e

    @connection_retry
    def upload(self, local_path: str, remote_path: str, retry_count: int = 3) -> None:  # noqa
        try:
            self._sftp.put(local_path, remote_path)
            logging.info(f"Uploaded {local_path} to {remote_path}")
        except OSError as e:
            raise FileNotFoundError(
                f"Local file not found: {local_path}") from e

    @connection_retry
    def execute(
        self, command: str, timeout: int = 3600, retry_count: int = 3
    ) -> Tuple[str, str]:
        async_exec = AsyncParamikoWrapper(self._client)

        stdout_bytes, stderr_bytes, exit_code = await async_exec.exec_command(
            command, timeout=timeout
        )

        stdout_str = stdout_bytes.decode("utf-8", errors="ignore")
        stderr_str = stderr_bytes.decode("utf-8", errors="ignore")

        if exit_code != 0:
            raise paramiko.SSHException(
                f"Command {command} failed ({exit_code}):\n"
                f"STDOUT: {stdout_str}\nSTDERR: {stderr_str}"
            )

        return stdout_str, stderr_str, exit_code

    @connection_retry
    def file_exists(self, remote_path: str, retry_count: int = 0) -> bool:
        try:
            self._sftp.stat(remote_path)
            return True
        except OSError:
            return False

    @connection_retry
    def make_directory(self, remote_path: str, retry_count: int = 3) -> None:
        self.execute(f"mkdir -p '{remote_path}'")

    @connection_retry
    def remove_directory(self, remote_dir: str, retry_count: int = 3) -> None:
        self.execute(f"rm -rf '{remote_dir}'")

    @connection_retry
    def read_file(self, remote_path: str, retry_count: int = 3) -> str:
        with self._sftp.open(remote_path, "r") as f:
            return f.read().decode("utf-8")

    @connection_retry
    def write_file(self, remote_path: str, content: str, retry_count: int = 3) -> None:  # noqa
        with self._sftp.open(remote_path, "w") as f:
            f.write(content.encode("utf-8"))
