"""Sanity tests — these should pass before any real logic exists."""


def test_imports():
    import anomalies
    import api_client
    import db
    import llm_framing
    import parse
    import sheets_export

    assert all([anomalies, api_client, db, llm_framing, parse, sheets_export])
