"""Public exceptions shared across scheduler and storage boundaries."""


class OutputOwnershipError(RuntimeError):
    """A materialized project path was claimed by more than one producer."""


class CacheIntegrityError(RuntimeError):
    """A cache entry exists but is not safe to replay."""

    def __init__(self, path: object, lookup: object) -> None:
        self.path = path
        self.lookup = lookup
        reason = getattr(lookup, "reason", "unknown cache integrity failure")
        super().__init__(f"Corrupt cache at {path}: {reason}")
