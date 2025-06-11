# -*- coding: utf-8 -*-
from paramiko.client import SSHClient
from paramiko.hostkeys import HostKeys
from pathlib import Path

HOSTKEYS = Path("~/.ssh/known_hosts").expanduser()

lcontrol = {
    "hostname": "94.16.104.167",
    "port": 22022,
    "username": "lcontrol"
}

python = {
    "hostname": "94.16.104.167",
    "port": 22022,
    "username": "python"
}



hostkeys = HostKeys(HOSTKEYS)
for k, v in hostkeys.items():
    print(f"{k}={v}")


client = SSHClient()
client.load_system_host_keys()

try:
    client.connect(**python)
except Exception as e:
    print(e)


stdin, stdout, stderr = client.exec_command("print('hi')")

print("err:", stderr.read())
print("stdout:", stdout.read())

client.close()
del client
