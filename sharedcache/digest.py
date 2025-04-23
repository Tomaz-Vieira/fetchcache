from typing import Final, Protocol
from hashlib import sha256


class HashlibHash(Protocol):
    def hexdigest(self) -> str: ...
    def digest(self) -> bytes: ...

class Digest:
    def __init__(self, digest: bytes) -> None:
        super().__init__()
        assert len(digest) == 32
        self.digest: Final[bytes] = digest

    def __hash__(self) -> int:
        return hash(self.digest)

    def __eq__(self, value: object, /) -> bool:
        return isinstance(value, self.__class__) and self.digest == value.digest

    def __str__(self) -> str:
        return "".join(f'{byte:02x}' for byte in self.digest)

    @classmethod
    def parse(cls, *, hexdigest: str) -> "Digest":
        if len(hexdigest) != 64:
            raise ValueError(f"value should have 64 characters")
        digest = bytearray(32)
        for hex_idx in range(0, 64, 2):
            digest[hex_idx // 2] = int(hexdigest[hex_idx:hex_idx + 2], 16)
        return Digest(bytes(digest))

class ContentDigest(Digest):
    @classmethod
    def parse(cls, *, hexdigest: str) -> "ContentDigest":
        digest = Digest.parse(hexdigest=hexdigest).digest
        return ContentDigest(digest)

class UrlDigest(Digest):
    @classmethod
    def from_str(cls, url: str) -> "UrlDigest":
        return UrlDigest(digest=sha256(url.encode("utf8")).digest())

    @classmethod
    def parse(cls, *, hexdigest: str) -> "UrlDigest":
        digest = Digest.parse(hexdigest=hexdigest).digest
        return UrlDigest(digest)

