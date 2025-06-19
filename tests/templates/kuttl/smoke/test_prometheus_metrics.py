# Fetch metrics from the built-in Prometheus endpoint of HDFS components.

import logging
import sys

import requests


def check_metrics(
    namespace: str, role: str, port: int, expected_metrics: list[str]
) -> None:
    response: requests.Response = requests.get(
        f"http://test-hbase-{role}-default-0.test-hbase-{role}-default.{namespace}.svc.cluster.local:{port}/prometheus",
        timeout=10,
    )
    assert response.ok, "Requesting metrics failed"

    # Split the response into lines to check for metric names at the beginning of each line.
    # This is a bit slower than using a regex but it allows to use special characters like "{}" in metric names
    # without needing to escape them.
    response_lines = response.text.splitlines()
    for metric in expected_metrics:
        # Use any() with a generator to stop early if the metric is found.
        assert any((line.startswith(metric) for line in response_lines)) is True, (
            f"Metric '{metric}' not found for {role}"
        )


def check_master_metrics(
    namespace: str,
) -> None:
    expected_metrics: list[str] = ["master_queue_size"]

    check_metrics(namespace, "master", 16010, expected_metrics)


def check_regionserver_metrics(
    namespace: str,
) -> None:
    expected_metrics: list[str] = ["region_server_queue_size"]

    check_metrics(namespace, "regionserver", 16030, expected_metrics)


def check_restserver_metrics(
    namespace: str,
) -> None:
    expected_metrics: list[str] = ["ugi_metrics_get_groups_num_ops"]

    check_metrics(namespace, "restserver", 8085, expected_metrics)


if __name__ == "__main__":
    namespace_arg: str = sys.argv[1]

    logging.basicConfig(
        level="DEBUG",
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    check_master_metrics(namespace_arg)
    check_regionserver_metrics(namespace_arg)
    check_restserver_metrics(namespace_arg)

    print("All expected metrics found")
