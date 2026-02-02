from dataclasses import dataclass
from typing import Iterator, List, Dict, Any
import csv
import json
import os


@dataclass
class Chunk:
    chunk_id: int
    records: List[Dict[str, Any]]
    row_start: int
    row_end: int


class BaseReader:
    def __iter__(self) -> Iterator[Chunk]:
        raise NotImplementedError


class CSVChunkReader(BaseReader):
    def __init__(self, file_path: str, chunk_size: int = 10_000):
        self.file_path = file_path
        self.chunk_size = chunk_size

        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

    def __iter__(self) -> Iterator[Chunk]:
        with open(self.file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            chunk_id = 0
            buffer: List[Dict[str, Any]] = []
            row_start = 0

            for row_index, row in enumerate(reader):
                buffer.append(row)

                if len(buffer) >= self.chunk_size:
                    yield Chunk(
                        chunk_id=chunk_id,
                        records=buffer,
                        row_start=row_start,
                        row_end=row_index,
                    )
                    chunk_id += 1
                    row_start = row_index + 1
                    buffer = []

            # flush last chunk
            if buffer:
                yield Chunk(
                    chunk_id=chunk_id,
                    records=buffer,
                    row_start=row_start,
                    row_end=row_start + len(buffer) - 1,
                )


class JSONLinesChunkReader(BaseReader):
    """
    Expect JSON Lines format:
    {"a": 1}
    {"a": 2}
    """

    def __init__(self, file_path: str, chunk_size: int = 10_000):
        self.file_path = file_path
        self.chunk_size = chunk_size

        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

    def __iter__(self) -> Iterator[Chunk]:
        with open(self.file_path, encoding="utf-8") as f:
            chunk_id = 0
            buffer: List[Dict[str, Any]] = []
            row_start = 0

            for row_index, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue

                buffer.append(json.loads(line))

                if len(buffer) >= self.chunk_size:
                    yield Chunk(
                        chunk_id=chunk_id,
                        records=buffer,
                        row_start=row_start,
                        row_end=row_index,
                    )
                    chunk_id += 1
                    row_start = row_index + 1
                    buffer = []

            if buffer:
                yield Chunk(
                    chunk_id=chunk_id,
                    records=buffer,
                    row_start=row_start,
                    row_end=row_start + len(buffer) - 1,
                )