"""
client.py — Interactive CLI client for the KV store.

Usage:
  python client.py                   # interactive REPL
  python client.py GET mykey         # single command
  python client.py PUT name Alice    # single command

The client is intentionally simple: one persistent TCP connection,
readline for history, pretty-printed responses.
"""

import socket
import sys
try:
    import readline  # noqa: F401 — just importing enables arrow key history in input()
except ImportError:
    pass  # readline not available on Windows, that's fine

HOST = "127.0.0.1"
PORT = 6379
TIMEOUT = 5  # seconds


HELP = """
Available commands:
  PING
  PUT <key> <value> [TTL <seconds>]
  GET <key>
  DELETE <key>
  EXISTS <key>
  KEYS
  FLUSH
  STATS
  QUIT / EXIT / CTRL-C
"""


def connect() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    sock.connect((HOST, PORT))
    return sock


def send(sock: socket.socket, command: str) -> str:
    sock.sendall((command.strip() + "\n").encode("utf-8"))
    response = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
        if b"\n" in response:
            break
    return response.decode("utf-8").strip()


def pretty(response: str) -> str:
    if response.startswith("+"):
        return f"\033[32m{response[1:]}\033[0m"       # green
    elif response.startswith("-ERR"):
        return f"\033[31m{response[1:]}\033[0m"       # red
    elif response.startswith("$"):
        val = response[1:]
        if val == "NIL":
            return "\033[90m(nil)\033[0m"
        if val == "EMPTY":
            return "\033[90m(empty)\033[0m"
        if "," in val:
            keys = val.split(",")
            return "\n".join(f"  \033[36m{i+1}) {k}\033[0m" for i, k in enumerate(keys))
        if "|" in val:  # STATS
            return "\n".join(f"  \033[33m{part.strip()}\033[0m" for part in val.split("|"))
        return f"\033[36m\"{val}\"\033[0m"
    elif response.startswith(":"):
        return f"\033[33m(integer) {response[1:]}\033[0m"
    return response


def repl(sock: socket.socket):
    print("\033[1mKVStore client\033[0m — type HELP for commands")
    while True:
        try:
            line = input("\033[90mkvstore>\033[0m ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break
        if not line:
            continue
        if line.upper() in ("HELP", "?"):
            print(HELP)
            continue
        if line.upper() in ("EXIT", "QUIT"):
            send(sock, "QUIT")
            break
        try:
            resp = send(sock, line)
            print(pretty(resp))
        except socket.timeout:
            print("\033[31mTimeout: server did not respond\033[0m")
        except BrokenPipeError:
            print("\033[31mDisconnected from server\033[0m")
            break


def single_command(sock: socket.socket, args: list[str]):
    command = " ".join(args)
    resp = send(sock, command)
    print(pretty(resp))


def main():
    try:
        sock = connect()
    except ConnectionRefusedError:
        print(f"\033[31mCould not connect to {HOST}:{PORT} — is the server running?\033[0m")
        print("  Start it with:  python server.py")
        sys.exit(1)

    if len(sys.argv) > 1:
        single_command(sock, sys.argv[1:])
    else:
        repl(sock)

    sock.close()


if __name__ == "__main__":
    main()
