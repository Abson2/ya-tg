import base64
import hashlib

from app import tmdb
from app.config import settings
from app.publisher import human_size
from app.security import encrypt_secret, decrypt_secret, mask_secret
from app.utils import (backoff_seconds, device_sign, generate_pkce,
                       parse_media_name, render_template)


def test_device_sign_matches_openapi_spec():
    client_id, ts, secret = "abc123", "1770298425", "s3cr3t"
    expected = hashlib.md5(
        f"client_id={client_id}&timestamp={ts}&secret={secret}".encode("utf-8")
    ).hexdigest()
    assert device_sign(client_id, ts, secret) == expected
    assert device_sign(client_id, ts, "wrong") != expected


def test_pkce_challenge_is_s256_without_padding():
    verifier, challenge = generate_pkce()
    assert len(verifier) >= 43
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()
    ).rstrip(b"=").decode("ascii")
    assert challenge == expected
    assert "=" not in challenge


def test_parse_movie_name():
    r = parse_media_name("Movie.Name.2024.1080p.WEB-DL.x265.mkv")
    assert r["year"] == "2024"
    assert r["resolution"] == "1080P"
    assert r["is_tv_hint"] is False
    assert "movie" in r["title"].lower()


def test_parse_tv_name():
    r = parse_media_name("Show.Name.S02E05.720p.mkv")
    assert (r["season"], r["episode"]) == (2, 5)
    assert r["is_tv_hint"] is True


def test_parse_cn_episode_name():
    r = parse_media_name("某剧.第12集.mp4")
    assert r["episode"] == 12
    assert r["is_tv_hint"] is True


def test_template_variables_and_condition():
    tpl = "<b>{{title}}</b>{{#if year}}({{year}}){{/if}}|{{share_url}}"
    out = render_template(tpl, {"title": "Test", "year": "", "share_url": "https://x"})
    assert out == "<b>Test</b>|https://x"
    out2 = render_template(tpl, {"title": "Test", "year": "2024", "share_url": "https://x"})
    assert out2 == "<b>Test</b>(2024)|https://x"


def test_backoff_is_bounded_and_grows():
    settings.queue_backoff_base, settings.queue_backoff_max = 60, 3600
    first = backoff_seconds(1)
    third = backoff_seconds(3)
    assert 1 <= first <= 100
    assert third >= first
    assert backoff_seconds(20) <= 3600


def test_encryption_roundtrip_and_mask(monkeypatch):
    monkeypatch.setattr(settings, "app_secret", "a" * 40)
    token = "1234567890:AAExampleBotTokenabcdefgh"
    enc = encrypt_secret(token)
    assert enc != token
    assert decrypt_secret(enc) == token
    masked = mask_secret(token)
    assert token[:4] in masked and token[-4:] in masked
    assert "AAExample" not in masked


def test_poster_and_tmdb_url():
    assert tmdb.poster_url("/abc.jpg") == "https://image.tmdb.org/t/p/w500/abc.jpg"
    assert tmdb.poster_url("") == ""
    assert tmdb.tmdb_url("tv", 42) == "https://www.themoviedb.org/tv/42"


def test_human_size():
    assert human_size(512) == "512B"
    assert human_size(2 * 1024 * 1024) == "2.0MB"
