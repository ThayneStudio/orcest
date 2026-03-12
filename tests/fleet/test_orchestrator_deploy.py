"""Tests for orcest.fleet.orchestrator_deploy."""

import pytest
import yaml

from orcest.fleet.orchestrator_deploy import render_orchestrator_config, render_project_compose

pytestmark = pytest.mark.unit


class TestRenderProjectCompose:
    def test_valid_yaml(self):
        content = render_project_compose(redis_port=6380)
        data = yaml.safe_load(content)
        assert isinstance(data, dict)
        assert "services" in data

    def test_redis_port_mapping(self):
        content = render_project_compose(redis_port=6385)
        data = yaml.safe_load(content)
        assert "6385:6379" in data["services"]["redis"]["ports"][0]

    def test_uses_image_not_build(self):
        content = render_project_compose(redis_port=6379)
        data = yaml.safe_load(content)
        orch = data["services"]["orchestrator"]
        assert orch["image"] == "orcest-orchestrator:latest"
        assert "build" not in orch

    def test_orchestrator_depends_on_redis(self):
        content = render_project_compose(redis_port=6379)
        data = yaml.safe_load(content)
        deps = data["services"]["orchestrator"]["depends_on"]
        assert "redis" in deps

    def test_redis_healthcheck(self):
        content = render_project_compose(redis_port=6379)
        data = yaml.safe_load(content)
        assert "healthcheck" in data["services"]["redis"]

    def test_env_file_reference(self):
        content = render_project_compose(redis_port=6379)
        data = yaml.safe_load(content)
        assert ".env" in data["services"]["orchestrator"]["env_file"]

    def test_config_volume_mount(self):
        content = render_project_compose(redis_port=6379)
        data = yaml.safe_load(content)
        volumes = data["services"]["orchestrator"]["volumes"]
        assert any("config" in v for v in volumes)

    def test_mem_limit(self):
        content = render_project_compose(redis_port=6379)
        data = yaml.safe_load(content)
        assert data["services"]["orchestrator"]["mem_limit"] == "1g"


class TestRenderOrchestratorConfig:
    def test_valid_yaml(self):
        content = render_orchestrator_config(repo="MyOrg/my-repo")
        data = yaml.safe_load(content)
        assert isinstance(data, dict)

    def test_repo_set(self):
        content = render_orchestrator_config(repo="MyOrg/my-repo")
        data = yaml.safe_load(content)
        assert data["github"]["repo"] == "MyOrg/my-repo"

    def test_redis_host_is_docker_internal(self):
        content = render_orchestrator_config(repo="owner/repo")
        data = yaml.safe_load(content)
        assert data["redis"]["host"] == "redis"
        assert data["redis"]["port"] == 6379

    def test_default_labels(self):
        content = render_orchestrator_config(repo="owner/repo")
        data = yaml.safe_load(content)
        assert data["labels"]["ready"] == "orcest:ready"
        assert data["labels"]["needs_human"] == "orcest:needs-human"
        assert data["labels"]["blocked"] == "orcest:blocked"

    def test_default_runner(self):
        content = render_orchestrator_config(repo="owner/repo")
        data = yaml.safe_load(content)
        assert data["default_runner"] == "claude"
