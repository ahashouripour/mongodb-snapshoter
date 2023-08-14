
import subprocess
from datetime import time
from pymongo import MongoClient
from urllib.parse import quote_plus
from urllib.parse import urlparse, parse_qs


class Snapshotter:
    def __init__(self, command):
        self.__command = command
        self.__scheme = None
        self.__hosts = []  # "your-mongodb-hostname"
        self.__port = 27017
        self.__username = None  # "your-username"
        self.__password = None  # "your-password"
        self.__auth_source = "admin"    '''It is recommended to use the "admin" database for administrative operations
                                        and user management.'''
        self.__auth_mechanism = "MONGODB-CR"    '''authmechanism must be in ('MONGODB-CR', 'PLAIN', 'MONGODB-OIDC',
                                                'SCRAM-SHA-1', 'MONGODB-X509', 'SCRAM-SHA-256', 'MONGODB-AWS',
                                                'DEFAULT', 'GSSAPI')'''
        self.__ssl_enabled = False
        self.__ssl_ca_certs = None  # "/path/to/ca.crt"
        self.__ssl_certfile = None  # "/path/to/client.crt"
        self.__ssl_keyfile = None  # "/path/to/client.key"

        self.__max_pool_size = 50
        self.__min_pool_size = 10
        self.__wait_queue_timeout_ms = 10000
        self.__connect_timeout_ms = 15000
        self.__socket_timeout_ms = 30000
        self.__heartbeat_frequency_ms = 10000

        self.__read_concern_level = "majority"  # "local", "majority", "available" or "linearizable" are acceptable.
        self.__write_concern = {"w": "majority"}  # {"w": 1}, {"w": "majority"} or {"w": 2} are acceptable

        self.__replica_sets = ""    '''options in MongoDB that relate to how you manage and control read operations in
                                    a replica set environment. 
                                    specifies the name of the replica set to which you want to connect.'''
        self.__read_preference = "primary"  '''
                                            read_preference defines the preferred mode for routing read operations 
                                            across the members of a replica set. It's important because it lets you
                                            balance between consistency and availability in your read operations. 
                                            "primary", "secondary", "secondaryPreferred" or "nearest" accepted. '''

        self.__database = "admin"  '''
                                    It is recommended to use the "admin" database for administrative operations and
                                    user management.'''
        self.__query_params = None
        self.__connected_db = ""

        self.__parsed_url = self.__command_parser()
        self.__extractor()

    def __command_parser(self):
        return urlparse(self.__command)

    def __extractor(self):
        self.__scheme = self.__parsed_url.scheme
        self.__username = quote_plus(self.__parsed_url.username)
        self.__password = quote_plus(self.__parsed_url.password)
        self.__hosts = self.__parsed_url.hostname.split(',')
        self.__database = self.__parsed_url.path.lstrip('/')
        self.__query_params = parse_qs(self.__parsed_url.query)
        # hostnames = self.hosts.split(',')

    def get_connection_string(self):
        # Construct the connection URI
        host_map = ', '.join(map(lambda host: host + ":" + str(self.__port), self.__hosts))
        print(host_map)
        uri = {
            "uri": f"{self.__scheme}://{quote_plus(self.__username)}:{quote_plus(self.__password)}@{host_map}/{self.__auth_source}?authMechanism={self.__auth_mechanism}",
            "ssl": {self.__ssl_enabled},
            "ssl_ca_certs": {self.__ssl_ca_certs},
            "ssl_certfile": {self.__ssl_certfile},
            "ssl_keyfile": {self.__ssl_keyfile},
            "maxPoolSize": {self.__max_pool_size},
            "minPoolSize": {self.__min_pool_size},
            "waitQueueTimeoutMS": {self.__wait_queue_timeout_ms},
            "connectTimeoutMS": {self.__connect_timeout_ms},
            "socketTimeoutMS": {self.__socket_timeout_ms},
            "heartbeatFrequencyMS": {self.__heartbeat_frequency_ms},
            "readConcernLevel": {self.__read_concern_level},
            "w": {self.__write_concern},
            "read_preference": {self.__read_preference},
            f"replicaSet": {self.__replica_sets} if self.__replica_sets else ""
        }
        return uri

    def get_connections(self):
        uris = []
        # Construct the connection URI\
        for host in self.__hosts:
            if not self.__replica_sets:
                uris.append([
                    host,
                    f"{self.__scheme}://{quote_plus(self.__username)}:{quote_plus(self.__password)}@{host}:{self.__port}/{self.__auth_source}?authMechanism={self.__auth_mechanism}",
                    [
                        f"ssl={self.__ssl_enabled}",
                        f"ssl_ca_certs={self.__ssl_ca_certs}",
                        f"ssl_certfile={self.__ssl_certfile}",
                        f"ssl_keyfile={self.__ssl_keyfile}",
                        f"maxPoolSize={self.__max_pool_size}",
                        f"minPoolSize={self.__min_pool_size}",
                        f"waitQueueTimeoutMS={self.__wait_queue_timeout_ms}",
                        f"connectTimeoutMS={self.__connect_timeout_ms}",
                        f"socketTimeoutMS={self.__socket_timeout_ms}",
                        f"heartbeatFrequencyMS={self.__heartbeat_frequency_ms}",
                        f"readConcernLevel={self.__read_concern_level}",
                        f"w={self.__write_concern}",
                        f"read_preference={self.__read_preference}"
                    ]
                ])

            for replica_set in self.__replica_sets:
                uris.append([
                    host,
                    f"{self.__scheme}://{self.__username}:{self.__password}@{host}:{self.__port}/{self.__auth_source}?authMechanism={self.__auth_mechanism}",
                    [
                        f"ssl={self.__ssl_enabled}",
                        f"ssl_ca_certs={self.__ssl_ca_certs}",
                        f"ssl_certfile={self.__ssl_certfile}",
                        f"ssl_keyfile={self.__ssl_keyfile}",
                        f"maxPoolSize={self.__max_pool_size}",
                        f"minPoolSize={self.__min_pool_size}",
                        f"waitQueueTimeoutMS={self.__wait_queue_timeout_ms}",
                        f"connectTimeoutMS={self.__connect_timeout_ms}",
                        f"socketTimeoutMS={self.__socket_timeout_ms}",
                        f"heartbeatFrequencyMS={self.__heartbeat_frequency_ms}",
                        f"readConcernLevel={self.__read_concern_level}",
                        f"w={self.__write_concern}",
                        f"replicaSet={replica_set}",
                        f"read_preference={self.__read_preference}"
                    ]
                ])
        return uris

    def get_uri(self):
        host_map = ', '.join(map(lambda host: host + ":" + str(self.__port), self.__hosts))
        return f"{self.__scheme}://{quote_plus(self.__username)}:{quote_plus(self.__password)}@{host_map}/{self.__auth_source}?authMechanism={self.__auth_mechanism}"

    def connect(self, uri, connection=[]):
        client = MongoClient(
            uri,
            self.__port,
            ssl=self.__ssl_enabled,
            maxPoolSize=self.__max_pool_size,
            minPoolSize=self.__min_pool_size,
            waitQueueTimeoutMS=self.__wait_queue_timeout_ms,
            connectTimeoutMS=self.__connect_timeout_ms,
            socketTimeoutMS=self.__socket_timeout_ms,
            heartbeatFrequencyMS=self.__heartbeat_frequency_ms,
            readConcernLevel=self.__read_concern_level,
        )
        self.__connected_db = client[self.__database]

    def lock_node(self):
        self.__connected_db.command("fsyncLock")
        print("Locked secondary node.")

    def lock_status(self):
        while True:
            op = self.__connected_db.current_op()["inprog"]
            fsync_ops = [op_item for op_item in op if "fsyncLock" in op_item.get("command", {})]
            if not fsync_ops:
                break
            time.sleep(5)
        print("Locking process completed.")
        return True

    def take_snapshot_by_host(self, vm):
        """ Here make-vm-snapsho is a function or module out of our code."""
        subprocess.run(["make-vm-snapsho", vm])
        print("Taking snapshot.")

    def take_snapshot(self, vm):
        subprocess.run(["make-vm-snapshot", self.__hosts[0]])
        print("Taking snapshot.")

    def unlock_secondary_node(self):
        self.__connected_db.command("fsyncUnlock")
        print("Unlocked secondary node.")


if __name__ == "__main__":
    print("Add your command please:")
    input_command = input()
    if input_command.strip() == '':
        print('User input is empty')
        exit()
    else:
        snapshoter = Snapshotter(input_command)
        snapshoter.connect(snapshoter.get_uri().strip())
        snapshoter.lock_node()
        if snapshoter.lock_status():
            snapshoter.take_snapshot()
            snapshoter.unlock_secondary_node()
