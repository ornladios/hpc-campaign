from os.path import expanduser

from hpc_campaign import connector


def _handler():
    return connector.MyTCPHandler.__new__(connector.MyTCPHandler)


def test_config_parsers_include_jump_identity_file(monkeypatch):
    monkeypatch.setattr(
        connector,
        "g_server_config_data",
        {
            "OLCF": {
                "dtn-ssh": {
                    "protocol": "ssh",
                    "host": "login.example.org",
                    "user": "remote_user",
                    "authentication": "passcode",
                    "serverpath": "~/bin/adios2_remote_server",
                    "jumphost": "jump.example.org",
                    "jumpuser": "jump_user",
                    "jumpidentity_file": "~/.ssh/jump_key",
                    "identity_file": "~/.ssh/remote_key",
                }
            }
        },
    )
    handler = _handler()
    req_qry = {"group": ["OLCF"], "service": ["dtn-ssh"]}

    service_req = handler.parse_service_request_version01(req_qry)
    connect_req = handler.get_jump_remote_from_config_v01(req_qry)

    assert service_req["jumphost"] == "jump.example.org"
    assert service_req["jumpuser"] == "jump_user"
    assert service_req["jumpidentity_file"] == "~/.ssh/jump_key"
    assert service_req["identity_file"] == "~/.ssh/remote_key"
    assert connect_req["jumphost"] == "jump.example.org"
    assert connect_req["jumpuser"] == "jump_user"
    assert connect_req["jumpidentity_file"] == "~/.ssh/jump_key"
    assert connect_req["identity_file"] == "~/.ssh/remote_key"


def test_login_connect_remote_uses_jump_identity_file_without_jump_password(monkeypatch):
    handler = _handler()
    connect_remote_calls = []
    via_jump_calls = []

    monkeypatch.setattr(connector, "g_remote_conn_list", [])
    monkeypatch.setattr(connector.MyTCPHandler, "check_connected_remote", lambda *args: None)
    monkeypatch.setattr(
        connector.MyTCPHandler,
        "login_window_remote",
        lambda *args: connector.SSHUserInfo("remote_user", "remote_pass"),
    )

    def fail_jump_popup(*args):
        raise AssertionError("jump password popup should not be used when jumpidentity_file is configured")

    def fake_connect_remote(
        self,
        host_name="",
        ssh_port=connector.SSH_PORT,
        user_name=None,
        user_pass=None,
        sock_channel=None,
    ):
        connect_remote_calls.append(
            {
                "host_name": host_name,
                "user_name": user_name,
                "user_pass": user_pass,
                "keyfile": self.options.keyfile,
            }
        )
        self.remote = connector.SSHConnectedServerInfo(
            connector.SSHServerInfo(host_name, ssh_port),
            connector.SSHUserInfo(user_name, user_pass),
            object(),
        )
        self.remote_connection_info = connector.SSHRemoteConnectionInfo(self.remote, None, None)
        return connector.SSH_NO_ERROR

    def fake_connect_remote_via_jump(
        self,
        connected_jump,
        remote_host_name,
        remote_ssh_port,
        remote_user_name,
        remote_user_pass,
    ):
        via_jump_calls.append(
            {
                "connected_jump": connected_jump.server.host_name,
                "remote_host_name": remote_host_name,
                "remote_user_name": remote_user_name,
                "remote_user_pass": remote_user_pass,
                "keyfile": self.options.keyfile,
            }
        )
        self.remote = connector.SSHConnectedServerInfo(
            connector.SSHServerInfo(remote_host_name, remote_ssh_port),
            connector.SSHUserInfo(remote_user_name, remote_user_pass),
            object(),
        )
        self.remote_connection_info = connector.SSHRemoteConnectionInfo(self.remote, connected_jump, None)
        return connector.SSH_NO_ERROR

    monkeypatch.setattr(connector.MyTCPHandler, "login_window_jump_remote", fail_jump_popup)
    monkeypatch.setattr(connector.SSHConnectRemote, "connect_remote", fake_connect_remote)
    monkeypatch.setattr(connector.SSHConnectRemote, "connect_remote_via_jump", fake_connect_remote_via_jump)

    ssh_connection = handler.login_connect_remote(
        {
            "remote_host": "login.example.org",
            "username": "remote_user",
            "identity_file": None,
            "jumphost": "jump.example.org",
            "jumpuser": "jump_user",
            "jumpidentity_file": "~/.ssh/jump_key",
            "auth": "passcode",
        }
    )

    assert ssh_connection is not None
    assert connect_remote_calls == [
        {
            "host_name": "jump.example.org",
            "user_name": "jump_user",
            "user_pass": None,
            "keyfile": expanduser("~/.ssh/jump_key"),
        }
    ]
    assert via_jump_calls == [
        {
            "connected_jump": "jump.example.org",
            "remote_host_name": "login.example.org",
            "remote_user_name": "remote_user",
            "remote_user_pass": "remote_pass",
            "keyfile": None,
        }
    ]
