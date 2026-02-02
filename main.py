import logging
from datetime import datetime

from pipeline.reader import CSVChunkReader
from pipeline.transformer import (
    TransformerPipeline,
    CleanStep,
    NormalizeStep,
    EnrichStep,
)
from pipeline.pipeline import ETLPipeline
from pipeline.loader import PostgresLoader
from db.connection import get_connection


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main():
    setup_logging()

    run_id = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")

    reader = CSVChunkReader("data/input.csv", chunk_size=10_000)

    transformer = TransformerPipeline(
        steps=[
            CleanStep(),
            NormalizeStep(),
            EnrichStep(country_map={"VN": "Vietnam"}),
        ]
    )

    conn = get_connection("dbname=etl user=etl password=etl host=localhost")
    loader = PostgresLoader(conn)

    pipeline = ETLPipeline(
        reader=reader,
        transformer=transformer,
        loader=loader,
        run_id=run_id,
        max_retries=3,
    )

    pipeline.run()


if __name__ == "__main__":
    main()