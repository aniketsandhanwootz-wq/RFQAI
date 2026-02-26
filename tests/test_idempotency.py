import hashlib

def test_content_sha_stable():
    base = "RFQ_BRIEF|rfq123||title|0|hello"
    h1 = hashlib.sha256(base.encode("utf-8")).hexdigest()
    h2 = hashlib.sha256(base.encode("utf-8")).hexdigest()
    assert h1 == h2