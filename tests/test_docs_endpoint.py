#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The docs view's two read-only endpoints.

Three things are worth testing here and they are not the happy path.

*Containment.* ``name`` comes from a URL segment. The judge-config endpoint had
exactly this bug once -- a URL segment interpolated straight into a path -- so
the same check is asserted rather than assumed.

*The empty state.* ``docs/`` is deliberately not packaged, so a pip install has
no documents. That must be a listable "nothing here" and not a 500, because it
is the *normal* state for most installs rather than an edge case.

*Registration-free listing.* A new ``.md`` file has to appear without a code
change, which is only true if the title comes out of the file itself.
"""

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from text2sql_eval_toolkit.ui import routers_docs, server  # noqa: E402


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A checkout-shaped directory: pyproject.toml at the root, docs/notes under it."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    notes = tmp_path / "docs" / "notes"
    notes.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return notes


@pytest.fixture
def client():
    return TestClient(server.app)


def _write(notes, name, text):
    (notes / f"{name}.md").write_text(text, encoding="utf-8")


# --- listing ---------------------------------------------------------------


def test_a_new_file_is_listed_with_no_code_change(repo, client):
    _write(repo, "state-of-the-art", "# The state of the art\n\nA survey.\n")
    body = client.get("/api/docs").json()
    assert body["available"] is True
    assert body["items"] == [
        {
            "name": "state-of-the-art",
            "title": "The state of the art",
            "summary": "A survey.",
        }
    ]


def test_the_title_falls_back_to_the_filename(repo, client):
    _write(repo, "no-heading", "Just prose, no heading.\n")
    (item,) = client.get("/api/docs").json()["items"]
    assert item["title"] == "No heading"


def test_the_summary_skips_headings_and_code(repo, client):
    _write(
        repo,
        "d",
        "# Title\n\n## Subtitle\n\n```sql\nSELECT 1;\n```\n\n- a bullet\n\nThe paragraph.\n",
    )
    (item,) = client.get("/api/docs").json()["items"]
    assert item["summary"] == "The paragraph."


def test_unranked_documents_are_sorted_by_title_not_filename(repo, client):
    _write(repo, "zebra", "# Alpha\n\nx\n")
    _write(repo, "alpha", "# Zebra\n\nx\n")
    titles = [i["title"] for i in client.get("/api/docs").json()["items"]]
    assert titles == ["Alpha", "Zebra"]


def test_a_ranked_document_takes_its_place_regardless_of_title(repo, client):
    # The survey sorts last despite an "S" title, and the tour first despite
    # an "A" one -- reading order, not the alphabet.
    _write(repo, "text-to-sql-evaluation-survey", "# State of the art\n\nx\n")
    _write(repo, "dashboard-tour", "# A tour\n\nx\n")
    _write(repo, "worked-examples", "# Worked examples\n\nx\n")
    names = [i["name"] for i in client.get("/api/docs").json()["items"]]
    assert names == [
        "dashboard-tour",
        "worked-examples",
        "text-to-sql-evaluation-survey",
    ]


def test_an_unranked_document_lands_between_the_ranked_ones(repo, client):
    # Adding a note needs no code change; it sorts into the middle, which is a
    # reasonable place for something nobody has ranked.
    _write(repo, "text-to-sql-evaluation-survey", "# State of the art\n\nx\n")
    _write(repo, "dashboard-tour", "# A tour\n\nx\n")
    _write(repo, "brand-new", "# Something new\n\nx\n")
    names = [i["name"] for i in client.get("/api/docs").json()["items"]]
    assert names == ["dashboard-tour", "brand-new", "text-to-sql-evaluation-survey"]


def test_a_file_whose_name_is_not_addressable_is_not_offered(repo, client):
    # Listing it would offer a link that 404s on the way back in.
    _write(repo, ".hidden", "# Hidden\n\nx\n")
    assert client.get("/api/docs").json()["items"] == []


def test_the_directory_readme_is_not_listed_as_a_document(repo, client):
    # It describes the other files rather than being one of them; in the view it
    # would read as a document about how to add documents.
    _write(repo, "README", "# Notes\n\nHow to add one.\n")
    _write(repo, "real", "# Real\n\nA document.\n")
    titles = [i["title"] for i in client.get("/api/docs").json()["items"]]
    assert titles == ["Real"]


def test_the_readme_is_still_fetchable_by_name(repo, client):
    # Unlisted, not unreachable -- a link to it should not be broken.
    _write(repo, "README", "# Notes\n\nHow to add one.\n")
    assert client.get("/api/docs/README").json()["title"] == "Notes"


def test_only_markdown_is_listed(repo, client):
    (repo / "notes.txt").write_text("# Not markdown\n", encoding="utf-8")
    assert client.get("/api/docs").json()["items"] == []


# --- the empty state -------------------------------------------------------


def test_no_documents_directory_lists_empty_rather_than_failing(
    tmp_path, monkeypatch, client
):
    """What a pip install sees: docs/ ships in neither the wheel nor the sdist."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    resp = client.get("/api/docs")
    assert resp.status_code == 200
    assert resp.json() == {"items": [], "available": False}


def test_available_distinguishes_absent_from_empty(repo, client):
    # The directory exists and holds nothing: a different message from "not
    # installed", so the view must be able to tell them apart.
    body = client.get("/api/docs").json()
    assert body == {"items": [], "available": True}


def test_fetching_a_document_with_no_directory_is_404_not_500(
    tmp_path, monkeypatch, client
):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert client.get("/api/docs/anything").status_code == 404


# --- one document ----------------------------------------------------------


def test_the_markdown_is_returned_unrendered(repo, client):
    source = "# Title\n\nSome *emphasis* and `code`.\n"
    _write(repo, "doc", source)
    body = client.get("/api/docs/doc").json()
    assert body == {"name": "doc", "title": "Title", "markdown": source}


def test_a_missing_document_is_404(repo, client):
    assert client.get("/api/docs/absent").status_code == 404


# --- images ----------------------------------------------------------------


PNG = bytes.fromhex("89504e470d0a1a0a") + b"stub"


def _asset(repo, filename, data=PNG):
    assets = repo / "assets"
    assets.mkdir(exist_ok=True)
    (assets / filename).write_bytes(data)


def test_a_screenshot_is_served_with_its_media_type(repo, client):
    _asset(repo, "home.png")
    resp = client.get("/api/docs/assets/home.png")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "image/png"
    assert resp.content == PNG


def test_a_missing_image_is_404(repo, client):
    assert client.get("/api/docs/assets/absent.png").status_code == 404


def test_an_unsupported_type_is_refused(repo, client):
    # Notes reference screenshots. Anything else in that directory is not a
    # reason to turn the endpoint into a general file server.
    for name in ("notes.md", "script.js", "diagram.svg", "archive.zip"):
        _asset(repo, name, b"x")
        assert client.get(f"/api/docs/assets/{name}").status_code == 404, name


def test_an_image_cannot_escape_the_assets_directory(repo, client):
    outside = repo.parent.parent / "secret.png"
    outside.write_bytes(b"not for the API")
    for attempt in ("../../secret.png", "..%2F..%2Fsecret.png", "....//secret.png"):
        resp = client.get(f"/api/docs/assets/{attempt}")
        assert resp.status_code != 200, attempt
        assert b"not for the API" not in resp.content


def test_assets_do_not_shadow_a_document_named_assets(repo, client):
    # `/api/docs/assets` is one segment and `/api/docs/assets/x.png` is two, so
    # the two routes cannot collide -- asserted because it looks like they
    # should.
    _write(repo, "assets", "# Assets\n\nA document that happens to be called that.\n")
    assert client.get("/api/docs/assets").json()["title"] == "Assets"


def test_no_documents_directory_means_no_assets(tmp_path, monkeypatch, client):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    assert client.get("/api/docs/assets/home.png").status_code == 404


# --- containment -----------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "..%2F..%2Fpyproject",
        "%2Fetc%2Fpasswd",
        "..",
        ".hidden",
        "with%20space",
    ],
)
def test_a_name_that_is_not_a_plain_stem_is_refused(repo, client, name):
    resp = client.get(f"/api/docs/{name}")
    assert resp.status_code in (404, 405), f"{name!r} returned {resp.status_code}"


def test_an_empty_name_falls_back_to_the_listing(repo, client):
    # `/api/docs/` is not a document request with an empty name -- Starlette
    # redirects it to the list endpoint. Recorded so the parametrize above is
    # not later "fixed" by adding "" to it.
    assert client.get("/api/docs/").json()["items"] == []


def test_traversal_cannot_reach_a_file_outside_the_docs_directory(repo, client):
    # The target exists and is readable, so a successful read would be a real
    # disclosure rather than a 404 by accident.
    outside = repo.parent.parent / "secret.md"
    outside.write_text("# Secret\n\nnot for the API.\n", encoding="utf-8")
    for attempt in ("../../secret", "..%2F..%2Fsecret", "....//secret"):
        resp = client.get(f"/api/docs/{attempt}")
        assert resp.status_code != 200
        assert "not for the API" not in resp.text


def test_the_resolver_refuses_an_escaping_name_directly(repo):
    with pytest.raises(FileNotFoundError):
        routers_docs._resolve_doc_path("../secret")


def test_a_symlink_out_of_the_directory_is_refused(repo, client):
    outside = repo.parent.parent / "elsewhere.md"
    outside.write_text("# Elsewhere\n\nreached by symlink.\n", encoding="utf-8")
    (repo / "link.md").symlink_to(outside)
    resp = client.get("/api/docs/link")
    # Resolution follows the link, so containment is asserted on the real path.
    assert resp.status_code == 404
    assert "reached by symlink" not in resp.text
