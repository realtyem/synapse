#!/usr/bin/env python
from __future__ import annotations

import asyncio
import itertools
import logging
import os
import random
import sys
import time
from argparse import ArgumentParser, Namespace
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from enum import Enum
from types import ModuleType
from typing import Any, Generator, Mapping, Sequence, Union

import psycopg
import psycopg2
import psycopg2.extras
from typing_extensions import LiteralString

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


class Driver(str, Enum):
    psycopg2 = "psycopg2"
    psycopg = "psycopg"
    psycopg_async = "psycopg_async"


ids: list[int] = []
data: list[dict[str, Any]] = []


def main() -> None:
    args = parse_cmdline()

    ids[:] = range(args.ntests)
    data[:] = [
        {
            "id": i,
            "name": "c%d" % i,
            "description": "c%d" % i,
            "q": i * 10,
            "p": i * 20,
            "x": i * 30,
            "y": i * 40,
        }
        for i in ids
    ]

    drop_test_records(psycopg, args)
    for _, name in enumerate(args.drivers):
        if name == Driver.psycopg2:
            run_psycopg2(args)

        elif name == Driver.psycopg:
            run_psycopg(args)

        elif name == Driver.psycopg_async:
            if sys.platform == "win32":
                if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
                    asyncio.set_event_loop_policy(
                        asyncio.WindowsSelectorEventLoopPolicy()
                    )

            asyncio.run(run_psycopg_async(args))

        else:
            raise AssertionError(f"unknown driver: {name!r}")


table = """
CREATE TABLE customer (
        id SERIAL NOT NULL,
        name VARCHAR(255),
        description VARCHAR(255),
        q INTEGER,
        p INTEGER,
        x INTEGER,
        y INTEGER,
        z INTEGER,
        PRIMARY KEY (id)
)
"""
drop = "DROP TABLE IF EXISTS customer"

insert = """
INSERT INTO customer (id, name, description, q, p, x, y) VALUES
(%(id)s, %(name)s, %(description)s, %(q)s, %(p)s, %(x)s, %(y)s)
"""

select: LiteralString = """
SELECT customer.id, customer.name, customer.description, customer.q,
    customer.p, customer.x, customer.y, customer.z
FROM customer
WHERE customer.id = %(id)s
"""

select_list: LiteralString = """
SELECT customer.id, customer.name, customer.description, customer.q,
    customer.p, customer.x, customer.y, customer.z
FROM customer
WHERE customer.id = ANY(%s)
"""

select_values = """
SELECT customer.id, customer.name, customer.description, customer.q,
    customer.p, customer.x, customer.y, customer.z
FROM customer, (VALUES (%s)) as ld(id)
WHERE customer.id = ld.id
"""

# Copied directly from synapse.storage.types
SQLQueryParameters = Union[Sequence[Any], Mapping[str, Any]]


@contextmanager
def time_log(message: str) -> Generator[None, None, None]:
    start = time.monotonic()
    yield
    end = time.monotonic()
    logger.info(f"Run {message} in {end-start} s")


def insert_with_executemany_sync(module: ModuleType, args: Namespace) -> None:
    with time_log(
        f"{module.__name__}: executemany inserting {args.ntests} test records"
    ):
        with module.connect(args.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(table)
                cursor.executemany(insert, data)
            conn.commit()


def insert_with_execute_sync(module: ModuleType, args: Namespace) -> None:
    with time_log(f"{module.__name__}: execute inserting {args.ntests} test records"):
        with module.connect(args.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(table)
                for row in data:
                    cursor.execute(insert, row)
            conn.commit()


def drop_test_records(module: ModuleType, args: Namespace) -> None:
    with time_log(f"{module.__name__}: execute dropping test records"):
        with module.connect(args.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(drop)
            conn.commit()


def run_iter_execute_sync(
    connection: Any, sql_string: LiteralString, query_args: SQLQueryParameters
) -> None:
    """
    Take a Connection, make a Cursor, then run execute() for each parameter

    Args:
        connection:
        sql_string: the SQL to run
        query_args: usually a list of int
    """
    with connection.cursor() as cursor:
        for t in query_args:
            cursor.execute(sql_string, t)
        cursor.fetchall()


def run_executemany_sync(
    connection: Any,
    sql_string: LiteralString,
    query_args: SQLQueryParameters,
    create_returning: bool = False,
) -> None:
    # The major difference between psycopg2 and psycopg's versions of executemany() is
    # that psycopg allows for returning results while psycopg2 does not. This is
    # described better in the docs for psycopg, but manifests as an added kwarg and an
    # awkward iterative for gathering such results.
    # https://www.psycopg.org/psycopg3/docs/api/cursors.html#psycopg.Cursor.executemany
    with connection.cursor() as cursor:
        if isinstance(connection, psycopg.Connection):
            cursor.executemany(sql_string, query_args, returning=create_returning)
        else:
            cursor.executemany(sql_string, query_args)

        if create_returning:
            result = []
            assert isinstance(connection, psycopg.Connection)
            while True:
                result.extend(cursor.fetchall())
                if not cursor.nextset():
                    break


def run_psycopg2(args: Namespace) -> None:
    logger.info("Running psycopg2")

    insert_with_execute_sync(psycopg2, args)
    drop_test_records(psycopg2, args)
    insert_with_executemany_sync(psycopg2, args)
    # Make sure to keep the last one so the read tests have something to work with

    def run(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]

        with psycopg2.connect(args.dsn) as conn:
            with time_log(
                f"psycopg2: thread {i} running {args.ntests} execute() queries"
            ):
                run_iter_execute_sync(conn, select, args_list)

    def run_many(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]

        with psycopg2.connect(args.dsn) as conn:
            with time_log(
                f"psycopg2: thread {i} running {args.ntests} executemany() queries"
            ):
                run_executemany_sync(conn, select, args_list)

    def run_list(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        with psycopg2.connect(args.dsn) as conn:
            with time_log(
                f"psycopg2: thread {i} running {args.ntests} ANY LIST execute() query"
            ):
                with conn.cursor() as cursor:
                    cursor.execute(select_list, [to_query])
                    cursor.fetchall()

    def run_execute_values(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]
        with psycopg2.connect(args.dsn) as conn:
            with time_log(
                f"psycopg2: thread {i} running {args.ntests} execute_values() queries"
            ):
                with conn.cursor() as cursor:
                    # Recall when using execute_values() with a Sequence of Mappings
                    # that a template needs to be provided as the normal parameter
                    # discovery only works correctly for Sequences of Sequences
                    psycopg2.extras.execute_values(
                        cursor,
                        select_values,
                        args_list,
                        template="(%(id)s)",
                        fetch=True,
                    )

    if args.concurrency <= 1:
        run(0)
        run_many(0)
        run_list(0)
        run_execute_values(0)
    else:
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            list(executor.map(run, range(args.concurrency)))
            list(executor.map(run_many, range(args.concurrency)))
            list(executor.map(run_list, range(args.concurrency)))
            list(executor.map(run_execute_values, range(args.concurrency)))

    drop_test_records(psycopg2, args)


def run_psycopg(args: Namespace) -> None:
    logger.info("Running psycopg sync")

    insert_with_execute_sync(psycopg, args)
    drop_test_records(psycopg, args)
    insert_with_executemany_sync(psycopg, args)
    # Make sure to keep the last one so the read tests have something to work with

    def run(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]

        with psycopg.connect(args.dsn) as conn:
            with time_log(
                f"psycopg: thread {i} running {args.ntests} execute() queries"
            ):
                run_iter_execute_sync(conn, select, args_list)

    def run_pipeline(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]

        with psycopg.connect(args.dsn) as conn, conn.pipeline():
            with time_log(
                f"psycopg: thread {i} running {args.ntests} PIPELINED execute() queries"
            ):
                run_iter_execute_sync(conn, select, args_list)

    def run_many(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]

        with psycopg.connect(args.dsn) as conn:
            with time_log(f"psycopg: thread {i} running {args.ntests} executemany()"):
                run_executemany_sync(conn, select, args_list, True)

    def run_list(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        with psycopg.connect(args.dsn) as conn:
            with time_log(
                f"psycopg: thread {i} running {args.ntests} ANY LIST execute() query"
            ):
                with conn.cursor() as cursor:
                    cursor.execute(select_list, [to_query])
                    cursor.fetchall()

    def run_with_copy(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        with psycopg.connect(args.dsn) as conn:
            with time_log(f"psycopg: thread {i} running {args.ntests} ITERATIVE copy() queries"):
                results = []
                sql = f"COPY ({select_values}) TO STDOUT"
                for id_ in to_query:
                    with conn.cursor() as cursor:
                        with cursor.copy(sql, (id_,)) as copy:
                            for row in copy.rows():
                                results.append(row)

    def run_copy_bulk(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [(id_,) for id_ in to_query]
        # Significant portions of this code were lifted right out of the Synapse code.
        # Credit to @clokep for them
        # In order to simulate the client-side behavior of execute_values() from
        # psycopg2, prepare the entire sql statement with values already inserted. These
        # must be flattened in advance instead of using the args_list directly
        value_str = "(" + ", ".join("%s" for _ in next(iter(args_list))) + ")"
        sql = select_values.replace("(%s)", ", ".join(value_str for _ in args_list))
        sql = f"COPY ({sql}) TO STDOUT"

        new_args_list = list(itertools.chain.from_iterable(args_list))
        with psycopg.connect(args.dsn) as conn:
            with time_log(
                f"psycopg: thread {i} running {args.ntests} BATCHING copy() queries"
            ):
                results = []
                with conn.cursor() as cursor:
                    with cursor.copy(sql, new_args_list) as copy:
                        for row in copy.rows():
                            results.append(row)

    if args.concurrency <= 1:
        run(0)
        run_pipeline(0)
        run_many(0)
        run_list(0)
        run_with_copy(0)
        run_copy_bulk(0)
    else:
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            list(executor.map(run, range(args.concurrency)))
            list(executor.map(run_pipeline, range(args.concurrency)))
            list(executor.map(run_many, range(args.concurrency)))
            list(executor.map(run_list, range(args.concurrency)))
            list(executor.map(run_with_copy, range(args.concurrency)))
            list(executor.map(run_copy_bulk, range(args.concurrency)))

    drop_test_records(psycopg, args)


async def run_psycopg_async(args: Namespace) -> None:
    logger.info("Running psycopg async")

    with time_log(f"psycopg_async: execute inserting {args.ntests} test records"):
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(table)
                for row in data:
                    await cursor.execute(insert, row)
            await conn.commit()

    with time_log("psycopg_async: execute dropping test records"):
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(drop)
            await conn.commit()

    with time_log(f"psycopg_async: executemany inserting {args.ntests} test records"):
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(drop)
                await cursor.execute(table)
                await cursor.executemany(insert, data)
            await conn.commit()

    async def run(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]

        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            async with conn.cursor() as cursor:
                with time_log(
                    f"psycopg_async: task {i} running {args.ntests} execute() queries"
                ):
                    for t in args_list:
                        await cursor.execute(select, t)
                    await cursor.fetchall()
                    # await cursor.close()

    async def run_pipeline(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]

        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            # Closing this context manager does a commit() which also runs a sync() on
            # the pipeline
            async with conn.pipeline():
                async with conn.cursor() as cursor:
                    with time_log(
                        f"psycopg_async: task {i} running {args.ntests} PIPELINED execute() queries"
                    ):
                        for t in args_list:
                            await cursor.execute(select, t)
                        # await cursor.fetchall()

    async def run_many(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [{"id": id_} for id_ in to_query]
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            with time_log(
                f"psycopg_async: task {i} running {args.ntests} executemany() queries"
            ):
                async with conn.cursor() as cursor:
                    await cursor.executemany(select, args_list, returning=True)

                    result = []
                    while True:
                        result.extend(await cursor.fetchall())
                        if cursor.nextset():
                            break

    async def run_list(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            with time_log(
                f"psycopg_async: task {i} running {args.ntests} ANY LIST execute() query"
            ):
                async with conn.cursor() as cursor:
                    await cursor.execute(select_list, [to_query])
                    await cursor.fetchall()

    async def run_with_copy(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            with time_log(
                f"psycopg_async: task {i} running {args.ntests} ITERATIVE copy() queries"
            ):
                results = []
                sql = f"COPY ({select_values}) TO STDOUT"
                for id_ in to_query:
                    async with conn.cursor() as cursor:
                        async with cursor.copy(sql, (id_,)) as copy:
                            async for row in copy.rows():
                                results.append(row)

    async def run_copy_bulk(i: int) -> None:
        to_query = random.choices(ids, k=args.ntests)
        args_list = [(id_,) for id_ in to_query]
        # Significant portions of this code were lifted right out of the Synapse code.
        # Credit to @clokep for them
        # In order to simulate the client-side behavior of execute_values() from
        # psycopg2, prepare the entire sql statement with values already inserted. These
        # must be flattened in advance instead of using the args_list directly
        value_str = "(" + ", ".join("%s" for _ in next(iter(args_list))) + ")"
        sql = select_values.replace("(%s)", ", ".join(value_str for _ in args_list))
        sql = f"COPY ({sql}) TO STDOUT"

        new_args_list = list(itertools.chain.from_iterable(args_list))
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            with time_log(
                f"psycopg_async: task {i} running {args.ntests} BATCHING copy() queries"
            ):
                results = []
                async with conn.cursor() as cursor:
                    async with cursor.copy(sql, new_args_list) as copy:
                        async for row in copy.rows():
                            results.append(row)

    if args.concurrency <= 1:
        await run(0)
        await run_pipeline(0)
        await run_many(0)
        await run_list(0)
        await run_with_copy(0)
        await run_copy_bulk(0)
    else:
        tasks = [run(i) for i in range(args.concurrency)]
        await asyncio.gather(*tasks)
        tasks_pipeline = [run_pipeline(i) for i in range(args.concurrency)]
        await asyncio.gather(*tasks_pipeline)
        tasks_many = [run_many(i) for i in range(args.concurrency)]
        await asyncio.gather(*tasks_many)
        tasks_list = [run_list(i) for i in range(args.concurrency)]
        await asyncio.gather(*tasks_list)
        tasks_copy = [run_with_copy(i) for i in range(args.concurrency)]
        await asyncio.gather(*tasks_copy)
        tasks_bulk_copy = [run_copy_bulk(i) for i in range(args.concurrency)]
        await asyncio.gather(*tasks_bulk_copy)

    with time_log("psycopg_async: dropping test records"):
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(drop)
            await conn.commit()


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "drivers",
        nargs="+",
        metavar="DRIVER",
        type=Driver,
        help=f"the drivers to test [choices: {', '.join(d.value for d in Driver)}]",
    )

    parser.add_argument(
        "--ntests",
        "-n",
        type=int,
        default=10_000,
        help="number of tests to perform [default: %(default)s]",
    )

    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=1,
        help="number of parallel tasks [default: %(default)s]",
    )

    parser.add_argument(
        "--dsn",
        default=os.environ.get("PSYCOPG_TEST_DSN", ""),
        help="database connection string"
        " [default: %(default)r (from PSYCOPG_TEST_DSN env var)]",
    )

    opt = parser.parse_args()

    return opt


if __name__ == "__main__":
    main()
