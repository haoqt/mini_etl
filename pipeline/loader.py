from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_batch

from pipeline.transformer import TransformedChunk


class PostgresLoader:
    def __init__(self, conn: psycopg2.extensions.connection):
        self.conn = conn

    def load_chunk(
        self,
        run_id: str,
        chunk: TransformedChunk,
    ):
        """
        Load 1 chunk in a single transaction
        """
        if not chunk.records:
            return

        cursor = self.conn.cursor()

        try:
            self._mark_chunk_start(cursor, run_id, chunk.chunk_id)

            self._upsert_records(cursor, chunk.records)

            self._mark_chunk_success(cursor, run_id, chunk.chunk_id)

            self.conn.commit()

        except Exception:
            self.conn.rollback()
            self._mark_chunk_failed(run_id, chunk.chunk_id)
            raise

        finally:
            cursor.close()

    # -------------------------
    # Internal helpers
    # -------------------------

    def _upsert_records(
        self,
        cursor,
        records: List[Dict[str, Any]],
    ):
        sql = """
        INSERT INTO orders (
            external_id,
            amount,
            country_code,
            country_name,
            created_at
        )
        VALUES (
            %(external_id)s,
            %(amount)s,
            %(country_code)s,
            %(country_name)s,
            %(created_at)s
        )
        ON CONFLICT (external_id)
        DO UPDATE SET
            amount = EXCLUDED.amount,
            country_name = EXCLUDED.country_name,
            created_at = EXCLUDED.created_at
        """

        execute_batch(cursor, sql, records, page_size=1000)

    def _mark_chunk_start(self, cursor, run_id: str, chunk_id: int):
        cursor.execute(
            """
            INSERT INTO etl_chunks (run_id, chunk_id, status)
            VALUES (%s, %s, 'processing')
            ON CONFLICT (run_id, chunk_id)
            DO UPDATE SET status = 'processing'
            """,
            (run_id, chunk_id),
        )

    def _mark_chunk_success(self, cursor, run_id: str, chunk_id: int):
        cursor.execute(
            """
            UPDATE etl_chunks
            SET status = 'success', updated_at = now()
            WHERE run_id = %s AND chunk_id = %s
            """,
            (run_id, chunk_id),
        )

    def _mark_chunk_failed(self, run_id: str, chunk_id: int):
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE etl_chunks
                SET status = 'failed', updated_at = now()
                WHERE run_id = %s AND chunk_id = %s
                """,
                (run_id, chunk_id),
            )
            self.conn.commit()