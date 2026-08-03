"""Lazy Daft source for PostgreSQL-backed encoded image rows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ImageSourceConfig:
    workload_name: str
    limit: int
    offset: int = 0
    dataset_passes: int = 1

    def __post_init__(self) -> None:
        if not self.workload_name:
            raise ValueError("workload_name must be non-empty")
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if self.offset < 0:
            raise ValueError("offset must be non-negative")
        if self.dataset_passes <= 0:
            raise ValueError("dataset_passes must be positive")


def image_documents_query(config: ImageSourceConfig) -> str:
    """Build the fixed-schema Daft SQL query for encoded image rows."""
    workload = config.workload_name.replace("'", "''")
    selected = (
        "SELECT doc_id, workload_name, image, image_bytes "
        "FROM image_documents "
        f"WHERE workload_name = '{workload}' "
        "ORDER BY doc_id "
        f"LIMIT {config.limit} OFFSET {config.offset}"
    )
    if config.dataset_passes == 1:
        return selected
    return (
        "WITH selected AS (" + selected + ") "
        "SELECT selected.doc_id::text || '#pass=' || pass_index::text AS doc_id, "
        "selected.workload_name, selected.image, selected.image_bytes "
        "FROM selected CROSS JOIN "
        f"generate_series(1, {config.dataset_passes}) AS generated(pass_index) "
        "ORDER BY pass_index, selected.doc_id"
    )


def split_image_source_config(
    config: ImageSourceConfig,
    shards: int,
) -> tuple[ImageSourceConfig, ...]:
    """Split one ordered source range into non-overlapping balanced shards."""
    if shards <= 0:
        raise ValueError("shards must be positive")
    shard_count = min(shards, config.limit)
    base, remainder = divmod(config.limit, shard_count)
    result: list[ImageSourceConfig] = []
    next_offset = config.offset
    for index in range(shard_count):
        shard_limit = base + (1 if index < remainder else 0)
        result.append(
            ImageSourceConfig(
                workload_name=config.workload_name,
                limit=shard_limit,
                offset=next_offset,
                dataset_passes=config.dataset_passes,
            )
        )
        next_offset += shard_limit
    return tuple(result)


class DaftImageSource:
    """Return a lazy Daft DataFrame; never collect image payloads on the driver."""

    def read(self, database_url: str, config: ImageSourceConfig):
        if not database_url:
            raise ValueError("database_url must be non-empty")
        import daft

        return daft.read_sql(image_documents_query(config), database_url)

    def read_sharded(
        self,
        database_url: str,
        config: ImageSourceConfig,
        *,
        shards: int,
    ):
        """Return one lazy Daft frame with explicit PostgreSQL source shards."""
        if not database_url:
            raise ValueError("database_url must be non-empty")
        import daft

        frames = [
            daft.read_sql(image_documents_query(shard), database_url)
            for shard in split_image_source_config(config, shards)
        ]
        return frames[0] if len(frames) == 1 else daft.concat(frames)
