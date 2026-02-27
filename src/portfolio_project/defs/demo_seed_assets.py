import hashlib
import json
import os
from pathlib import Path
from zipfile import ZipFile

import requests
from dagster import AssetExecutionContext, asset


DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))
GITHUB_API_ROOT = "https://api.github.com"
DEFAULT_DEMO_DATA_REPO = os.getenv("DEMO_DATA_REPO", "").strip()
DEFAULT_DEMO_DATA_ASSET = os.getenv("DEMO_DATA_ASSET", "").strip()
DEFAULT_DEMO_DATA_RELEASE = os.getenv("DEMO_DATA_RELEASE", "latest").strip().lower()
DEFAULT_DEMO_DATA_TAG = os.getenv("DEMO_DATA_TAG", "").strip()
DEFAULT_GITHUB_TOKEN_ENV = os.getenv("DEMO_DATA_GITHUB_TOKEN_ENV", "GITHUB_TOKEN").strip()
DEFAULT_DEMO_ZIP_URL = os.getenv("DEMO_DATA_ZIP_URL", "").strip()
DEFAULT_DEMO_ZIP_LOCAL_PATH = os.getenv("DEMO_DATA_ZIP_LOCAL_PATH", "demo/demo_data.zip").strip()
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("DEMO_DATA_TIMEOUT_SECONDS", "30"))


def _get_op_config(context: AssetExecutionContext) -> dict:
    op_execution_context = getattr(context, "op_execution_context", None)
    if op_execution_context is None:
        return {}
    return getattr(op_execution_context, "op_config", {}) or {}


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _marker_matches(marker_path: Path, expected: dict[str, str]) -> bool:
    if not marker_path.exists():
        return False
    try:
        marker_data = json.loads(marker_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    for key, value in expected.items():
        if str(marker_data.get(key, "")) != str(value):
            return False
    return True


def _write_marker(marker_path: Path, marker: dict[str, str]) -> None:
    marker_path.write_text(json.dumps(marker, indent=2, sort_keys=True), encoding="utf-8")


def _safe_extract_zip(zip_path: Path, extract_to: Path) -> int:
    extract_to_resolved = extract_to.resolve()
    extracted_files = 0
    with ZipFile(zip_path, "r") as zip_file:
        for member in zip_file.infolist():
            target_path = (extract_to / member.filename).resolve()
            if extract_to_resolved not in target_path.parents and target_path != extract_to_resolved:
                raise ValueError(f"Unsafe zip member path: {member.filename}")
            zip_file.extract(member, path=extract_to)
            if not member.is_dir():
                extracted_files += 1
    return extracted_files


def _create_session(github_token: str | None) -> requests.Session:
    session = requests.Session()
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "portfolio-project/0.1",
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    session.headers.update(headers)
    return session


def _resolve_github_release(session: requests.Session, repo: str, release: str, tag: str, timeout_seconds: int) -> dict:
    if "/" not in repo:
        raise ValueError("DEMO_DATA_REPO must be in owner/repo format.")
    owner, repository = repo.split("/", 1)
    release_mode = (release or "latest").lower()
    if tag:
        url = f"{GITHUB_API_ROOT}/repos/{owner}/{repository}/releases/tags/{tag}"
    elif release_mode == "latest":
        url = f"{GITHUB_API_ROOT}/repos/{owner}/{repository}/releases/latest"
    else:
        raise ValueError("DEMO_DATA_RELEASE must be 'latest' unless DEMO_DATA_TAG is provided.")
    response = session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.json()


def _select_release_asset(release_payload: dict, asset_name: str) -> dict:
    assets = release_payload.get("assets", []) or []
    for asset in assets:
        if str(asset.get("name", "")) == asset_name:
            return asset
    raise ValueError(f"Release asset not found: {asset_name}")


def _load_source_bytes(
    local_path: Path | None,
    url: str,
    timeout_seconds: int,
    demo_repo: str,
    demo_asset: str,
    demo_release: str,
    demo_tag: str,
    token_env: str,
    zip_path: Path,
    marker_path: Path,
) -> tuple[bytes, str, dict[str, str], bool]:
    github_token = os.getenv(token_env) if token_env else None
    if demo_repo and demo_asset:
        session = _create_session(github_token=github_token)
        release_payload = _resolve_github_release(
            session=session,
            repo=demo_repo,
            release=demo_release,
            tag=demo_tag,
            timeout_seconds=timeout_seconds,
        )
        release_tag = str(release_payload.get("tag_name", "unknown"))
        asset = _select_release_asset(release_payload, demo_asset)
        asset_id = str(asset.get("id", ""))
        download_url = str(asset.get("browser_download_url", ""))
        if not download_url:
            raise ValueError("Release asset missing browser_download_url.")

        marker_data = {
            "mode": "github_release",
            "repo": demo_repo,
            "asset_name": demo_asset,
            "release_tag": release_tag,
            "asset_id": asset_id,
        }
        if zip_path.exists() and _marker_matches(marker_path, marker_data):
            return zip_path.read_bytes(), f"github:{demo_repo}@{release_tag}/{demo_asset}", marker_data, False

        download_response = session.get(download_url, timeout=timeout_seconds)
        download_response.raise_for_status()
        return download_response.content, f"github:{demo_repo}@{release_tag}/{demo_asset}", marker_data, True

    if local_path is not None and local_path.exists():
        return local_path.read_bytes(), f"local:{local_path}", {"mode": "local", "path": str(local_path)}, True
    if url:
        response = requests.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        return response.content, f"url:{url}", {"mode": "url", "url": url}, True
    raise ValueError(
        "No demo zip source configured. Set DEMO_DATA_REPO+DEMO_DATA_ASSET, DEMO_DATA_ZIP_LOCAL_PATH, or DEMO_DATA_ZIP_URL."
    )


@asset(
    name="seed_demo_data",
)
def seed_demo_data(context: AssetExecutionContext) -> None:
    """
    Ensure demo_data.zip is present, write its SHA256 file, and extract it into the data root.
    """
    op_config = _get_op_config(context)
    demo_repo = str(op_config.get("demo_repo", DEFAULT_DEMO_DATA_REPO)).strip()
    demo_asset = str(op_config.get("demo_asset", DEFAULT_DEMO_DATA_ASSET)).strip()
    demo_release = str(op_config.get("demo_release", DEFAULT_DEMO_DATA_RELEASE)).strip().lower()
    demo_tag = str(op_config.get("demo_tag", DEFAULT_DEMO_DATA_TAG)).strip()
    token_env = str(op_config.get("token_env", DEFAULT_GITHUB_TOKEN_ENV)).strip()
    configured_local_path = str(op_config.get("local_path", DEFAULT_DEMO_ZIP_LOCAL_PATH)).strip()
    configured_url = str(op_config.get("url", DEFAULT_DEMO_ZIP_URL)).strip()
    timeout_seconds = int(op_config.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))

    local_path = Path(configured_local_path) if configured_local_path else None
    data_root = DATA_ROOT
    demo_dir = data_root / "demo"
    zip_path = demo_dir / "demo_data.zip"
    sha_path = demo_dir / "demo_data.zip.sha256"
    marker_path = demo_dir / ".demo_data_source.json"
    demo_dir.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)

    content, source, source_marker, source_downloaded = _load_source_bytes(
        local_path=local_path,
        url=configured_url,
        timeout_seconds=timeout_seconds,
        demo_repo=demo_repo,
        demo_asset=demo_asset,
        demo_release=demo_release,
        demo_tag=demo_tag,
        token_env=token_env,
        zip_path=zip_path,
        marker_path=marker_path,
    )
    content_sha = _sha256_bytes(content)

    existing_sha = sha_path.read_text(encoding="ascii").strip() if sha_path.exists() else ""
    updated_zip = True
    if zip_path.exists() and existing_sha == content_sha:
        updated_zip = False
    else:
        zip_path.write_bytes(content)
        sha_path.write_text(content_sha, encoding="ascii")
    _write_marker(marker_path, source_marker)

    extracted_files = _safe_extract_zip(zip_path=zip_path, extract_to=data_root)

    context.add_output_metadata(
        {
            "source": source,
            "zip_path": str(zip_path),
            "sha_path": str(sha_path),
            "marker_path": str(marker_path),
            "sha256": content_sha,
            "updated_zip": updated_zip,
            "source_downloaded": source_downloaded,
            "extracted_files": extracted_files,
        }
    )
