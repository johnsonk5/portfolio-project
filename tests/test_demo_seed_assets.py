from pathlib import Path
from zipfile import ZipFile

from dagster import materialize

import portfolio_project.defs.demo_seed_assets as demo_seed_module
from portfolio_project.defs.demo_seed_assets import seed_demo_data


class _FakeResponse:
    def __init__(self, content: bytes = b"", status_code: int = 200, json_payload=None):
        self.content = content
        self.status_code = status_code
        self._json_payload = json_payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json_payload


class _FakeSession:
    def __init__(self, zip_bytes: bytes):
        self.headers = {}
        self.zip_bytes = zip_bytes
        self.release_calls = 0
        self.download_calls = 0

    def get(self, url: str, timeout: int):
        if url.endswith("/releases/latest") or "/releases/tags/" in url:
            self.release_calls += 1
            return _FakeResponse(
                json_payload={
                    "tag_name": "v1.0.0-demo",
                    "assets": [
                        {
                            "id": 12345,
                            "name": "portfolio-demo-data-v1.zip",
                            "browser_download_url": "https://example.test/portfolio-demo-data-v1.zip",
                        }
                    ],
                }
            )
        if url == "https://example.test/portfolio-demo-data-v1.zip":
            self.download_calls += 1
            return _FakeResponse(content=self.zip_bytes)
        return _FakeResponse(status_code=404)


def _build_demo_zip(zip_path: Path) -> bytes:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr("bronze/reference/demo_source.txt", "demo-seed")
        zip_file.writestr("silver/demo/demo_table.csv", "id,value\n1,seeded\n")
    return zip_path.read_bytes()


def test_seed_demo_data_from_local_zip(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path
    source_zip = project_root / "demo" / "demo_data.zip"
    _build_demo_zip(source_zip)

    data_root = project_root / "data"
    monkeypatch.setattr(demo_seed_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_ZIP_LOCAL_PATH", str(source_zip))
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_ZIP_URL", "")

    result = materialize(assets=[seed_demo_data])
    assert result.success

    stored_zip = data_root / "demo" / "demo_data.zip"
    stored_sha = data_root / "demo" / "demo_data.zip.sha256"
    extracted_file = data_root / "bronze" / "reference" / "demo_source.txt"

    assert stored_zip.exists()
    assert stored_sha.exists()
    assert len(stored_sha.read_text(encoding="ascii").strip()) == 64
    assert extracted_file.exists()
    assert extracted_file.read_text(encoding="utf-8") == "demo-seed"


def test_seed_demo_data_from_url(tmp_path: Path, monkeypatch) -> None:
    source_zip = tmp_path / "source.zip"
    zip_bytes = _build_demo_zip(source_zip)
    data_root = tmp_path / "data"

    monkeypatch.setattr(demo_seed_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_ZIP_LOCAL_PATH", "")
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_ZIP_URL", "https://example.test/demo.zip")
    monkeypatch.setattr(demo_seed_module.requests, "get", lambda url, timeout: _FakeResponse(zip_bytes))

    result = materialize(assets=[seed_demo_data])
    assert result.success
    assert (data_root / "demo" / "demo_data.zip").exists()
    assert (data_root / "silver" / "demo" / "demo_table.csv").exists()


def test_seed_demo_data_from_github_release_with_marker_skips_redownload(tmp_path: Path, monkeypatch) -> None:
    source_zip = tmp_path / "source.zip"
    zip_bytes = _build_demo_zip(source_zip)
    session = _FakeSession(zip_bytes=zip_bytes)
    data_root = tmp_path / "data"

    monkeypatch.setattr(demo_seed_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_ZIP_LOCAL_PATH", "")
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_ZIP_URL", "")
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_DATA_REPO", "yourorg/yourrepo")
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_DATA_ASSET", "portfolio-demo-data-v1.zip")
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_DATA_RELEASE", "latest")
    monkeypatch.setattr(demo_seed_module, "DEFAULT_DEMO_DATA_TAG", "")
    monkeypatch.setattr(demo_seed_module, "_create_session", lambda github_token: session)

    first = materialize(assets=[seed_demo_data])
    assert first.success
    second = materialize(assets=[seed_demo_data])
    assert second.success

    assert session.release_calls == 2
    assert session.download_calls == 1
    assert (data_root / "demo" / ".demo_data_source.json").exists()
