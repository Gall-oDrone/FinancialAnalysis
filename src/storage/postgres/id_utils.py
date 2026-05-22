"""Helpers for primary keys compatible with PostgreSQL BIGINT."""
import uuid

# Max positive value for signed BIGINT (2^63 - 1)
_BIGINT_MAX = 0x7FFFFFFFFFFFFFFF


def random_bigint_id() -> int:
    """Return a random positive integer that fits in PostgreSQL BIGINT."""
    return uuid.uuid4().int & _BIGINT_MAX
