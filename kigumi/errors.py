"""Public exceptions shared across scheduler and storage boundaries."""


class OutputOwnershipError(RuntimeError):
    """A materialized project path was claimed by more than one producer."""
