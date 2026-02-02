from dataclasses import dataclass
from typing import List, Dict, Any, Callable
from datetime import datetime

from pipeline.reader import Chunk


@dataclass
class TransformError:
    row_index: int
    reason: str
    raw_record: Dict[str, Any]


@dataclass
class TransformedChunk:
    chunk_id: int
    records: List[Dict[str, Any]]
    errors: List[TransformError]


class TransformStep:
    """
    Base class for all transform steps
    """
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class TransformerPipeline:
    def __init__(self, steps: List[TransformStep]):
        self.steps = steps

    def process_chunk(self, chunk: Chunk) -> TransformedChunk:
        transformed_records: List[Dict[str, Any]] = []
        errors: List[TransformError] = []

        for i, record in enumerate(chunk.records):
            try:
                current = record
                for step in self.steps:
                    current = step.process(current)

                transformed_records.append(current)

            except Exception as e:
                errors.append(
                    TransformError(
                        row_index=chunk.row_start + i,
                        reason=str(e),
                        raw_record=record,
                    )
                )

        return TransformedChunk(
            chunk_id=chunk.chunk_id,
            records=transformed_records,
            errors=errors,
        )


class CleanStep(TransformStep):
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = {}

        for k, v in record.items():
            if v is None:
                continue

            if isinstance(v, str):
                v = v.strip()
                if v == "":
                    continue

            cleaned[k] = v

        if not cleaned:
            raise ValueError("Empty record after cleaning")

        return cleaned


class NormalizeStep(TransformStep):
    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(record)

        # example: normalize date field
        if "created_at" in normalized:
            value = normalized["created_at"]
            if isinstance(value, str):
                try:
                    normalized["created_at"] = datetime.fromisoformat(value)
                except ValueError:
                    raise ValueError(f"Invalid datetime: {value}")

        # example: normalize numeric
        if "amount" in normalized:
            normalized["amount"] = float(normalized["amount"])

        return normalized


class EnrichStep(TransformStep):
    def __init__(self, country_map: Dict[str, str]):
        self.country_map = country_map

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(record)

        country_code = enriched.get("country_code")
        if country_code:
            country_name = self.country_map.get(country_code)
            if not country_name:
                raise ValueError(f"Unknown country code: {country_code}")
            enriched["country_name"] = country_name

        return enriched
