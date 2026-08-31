from tests.integration import verify_campaigns_incontainer


def test_incontainer_verifier_checks_services_before_reads(monkeypatch):
    calls = []

    monkeypatch.setattr(
        verify_campaigns_incontainer,
        "check_port",
        lambda host, port: calls.append((host, port)),
    )
    monkeypatch.setattr(
        verify_campaigns_incontainer,
        "verify_campaigns",
        lambda: calls.append(("verify", 0)),
    )

    verify_campaigns_incontainer.main()

    assert calls == [
        ("s3-service.docker.hpc-campaign", 9000),
        ("https-service.docker.hpc-campaign", 443),
        ("ssh-service.docker.hpc-campaign", 22),
        ("xrootd-service.docker.hpc-campaign", 8080),
        ("localhost", 30000),
        ("verify", 0),
    ]
