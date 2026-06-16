"""Static checks on docker-compose.yml: pinned images, documented env vars, resource limits."""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
SERVICES: dict = COMPOSE["services"]
ONE_SHOT = {"minio-init"}


def _env_example_keys() -> set[str]:
    keys = set()
    for line in (ROOT / ".env.example").read_text().splitlines():
        m = re.match(r"^([A-Z][A-Z0-9_]*)=", line)
        if m:
            keys.add(m.group(1))
    return keys


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_image_is_pinned(name: str) -> None:
    svc = SERVICES[name]
    image = svc.get("image")
    assert image, f"{name}: every service needs an explicit image name"
    if "build" in svc:
        return  # locally built; base images are pinned in the Dockerfiles
    assert ":" in image.rsplit("/", 1)[-1], f"{name}: image {image!r} has no tag"
    assert not image.endswith(":latest"), f"{name}: image {image!r} uses latest"


@pytest.mark.parametrize("dockerfile", sorted(ROOT.glob("**/Dockerfile")), ids=str)
def test_dockerfile_base_images_pinned(dockerfile: Path) -> None:
    for line in dockerfile.read_text().splitlines():
        for ref in re.findall(r"^FROM\s+(\S+)|--from=(\S+)", line):
            image = next(r for r in ref if r)
            assert ":" in image and not image.endswith(":latest"), f"{dockerfile}: {image}"


def test_every_compose_variable_is_documented() -> None:
    text = (ROOT / "docker-compose.yml").read_text()
    used = {m.group(1) for m in re.finditer(r"(?<!\$)\$\{([A-Z][A-Z0-9_]*)", text)}
    missing = used - _env_example_keys()
    assert not missing, f"variables used in compose but missing from .env.example: {sorted(missing)}"


def test_kafka_cluster_id_is_kraft_uuid() -> None:
    text = (ROOT / ".env.example").read_text()
    m = re.search(r"^KAFKA_CLUSTER_ID=(\S+)", text, re.M)
    assert m and re.fullmatch(r"[A-Za-z0-9_-]{22}", m.group(1)), "generate with kafka-storage.sh random-uuid"


@pytest.mark.parametrize("name", sorted(set(SERVICES) - ONE_SHOT))
def test_long_running_services_restart_and_are_bounded(name: str) -> None:
    svc = SERVICES[name]
    assert svc.get("restart") == "unless-stopped", f"{name}: restart policy"
    limit = svc.get("deploy", {}).get("resources", {}).get("limits", {}).get("memory")
    assert limit, f"{name}: missing memory limit"


def test_no_docker_socket_mounted() -> None:
    for name, svc in SERVICES.items():
        for vol in svc.get("volumes", []):
            assert "docker.sock" not in str(vol), f"{name} mounts the Docker socket"
