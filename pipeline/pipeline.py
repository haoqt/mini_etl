import logging
from typing import Set

from pipeline.reader import BaseReader
from pipeline.transformer import TransformerPipeline
from pipeline.loader import PostgresLoader


class ETLPipeline:
    def __init__(
        self,
        reader: BaseReader,
        transformer: TransformerPipeline,
        loader: PostgresLoader,
        run_id: str,
        max_retries: int = 3,
    ):
        self.reader = reader
        self.transformer = transformer
        self.loader = loader
        self.run_id = run_id
        self.max_retries = max_retries

        self.logger = logging.getLogger("etl.pipeline")

    def run(self):
        self.logger.info("ETL run started", extra={"run_id": self.run_id})

        processed_chunks = self._load_processed_chunks()

        for chunk in self.reader:
            if chunk.chunk_id in processed_chunks:
                self.logger.info(
                    "Skipping processed chunk",
                    extra={"chunk_id": chunk.chunk_id},
                )
                continue

            self._process_chunk_with_retry(chunk)

        self.logger.info("ETL run finished", extra={"run_id": self.run_id})

    # -------------------------
    # Internal helpers
    # -------------------------

    def _process_chunk_with_retry(self, chunk):
        attempts = 0

        while attempts < self.max_retries:
            try:
                self.logger.info(
                    "Processing chunk",
                    extra={
                        "chunk_id": chunk.chunk_id,
                        "attempt": attempts + 1,
                    },
                )

                transformed = self.transformer.process_chunk(chunk)

                self.loader.load_chunk(self.run_id, transformed)

                self.logger.info(
                    "Chunk processed successfully",
                    extra={"chunk_id": chunk.chunk_id},
                )
                return

            except Exception as e:
                attempts += 1
                self.logger.error(
                    "Chunk processing failed",
                    extra={
                        "chunk_id": chunk.chunk_id,
                        "attempt": attempts,
                        "error": str(e),
                    },
                )

        self.logger.critical(
            "Chunk permanently failed",
            extra={"chunk_id": chunk.chunk_id},
        )

    def _load_processed_chunks(self) -> Set[int]:
        """
        Load successful chunks for this run from DB
        """
        cursor = self.loader.conn.cursor()
        try:
            cursor.execute(
                """
                SELECT chunk_id
                FROM etl_chunks
                WHERE run_id = %s AND status = 'success'
                """,
                (self.run_id,),
            )
            return {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()
