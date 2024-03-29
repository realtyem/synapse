#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2020 The Matrix.org Foundation C.I.C.
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#
import abc
import logging
import threading
from typing import TYPE_CHECKING, Callable, List, Optional

from synapse.storage.engines import (
    BaseDatabaseEngine,
    IncorrectDatabaseSetup,
    PostgresEngine,
)
from synapse.storage.types import Connection, Cursor

if TYPE_CHECKING:
    from synapse.storage.database import LoggingDatabaseConnection

logger = logging.getLogger(__name__)


_INCONSISTENT_SEQUENCE_ERROR = """
Postgres sequence '%(seq)s' is inconsistent with associated
table '%(table)s'. This can happen if Synapse has been downgraded and
then upgraded again, or due to a bad migration.

To fix this error, shut down Synapse (including any and all workers)
and run the following SQL:

    SELECT setval('%(seq)s', (
        %(max_id_sql)s
    ));

See docs/postgres.md for more information.
"""


class SequenceGenerator(metaclass=abc.ABCMeta):
    """A class which generates a unique sequence of integers"""

    @abc.abstractmethod
    def get_next_id_txn(self, txn: Cursor) -> int:
        """Gets the next ID in the sequence"""
        ...

    @abc.abstractmethod
    def get_next_mult_txn(self, txn: Cursor, n: int) -> List[int]:
        """Get the next `n` IDs in the sequence"""
        ...

    @abc.abstractmethod
    def check_consistency(
        self,
        db_conn: "LoggingDatabaseConnection",
        table: str,
        id_column: str,
        positive: bool = True,
    ) -> None:
        """Should be called during start up to test that the current value of
        the sequence is greater than or equal to the maximum ID in the table.

        This is to handle various cases where the sequence value can get out of
        sync with the table, e.g. if Synapse gets rolled back to a previous
        version and the rolled forwards again.
        """
        ...


class PostgresSequenceGenerator(SequenceGenerator):
    """An implementation of SequenceGenerator which uses a postgres sequence"""

    def __init__(self, sequence_name: str):
        self._sequence_name = sequence_name

    def get_next_id_txn(self, txn: Cursor) -> int:
        txn.execute("SELECT nextval(?)", (self._sequence_name,))
        fetch_res = txn.fetchone()
        assert fetch_res is not None
        return fetch_res[0]

    def get_next_mult_txn(self, txn: Cursor, n: int) -> List[int]:
        txn.execute(
            "SELECT nextval(?) FROM generate_series(1, ?)", (self._sequence_name, n)
        )
        return [i for (i,) in txn]

    def check_consistency(
        self,
        db_conn: "LoggingDatabaseConnection",
        table: str,
        id_column: str,
        positive: bool = True,
    ) -> None:
        """See SequenceGenerator.check_consistency for docstring."""

        txn = db_conn.cursor(txn_name="sequence.check_consistency")

        # First we get the current max ID from the table.
        table_sql = "SELECT GREATEST(%(agg)s(%(id)s), 0) FROM %(table)s" % {
            "id": id_column,
            "table": table,
            "agg": "MAX" if positive else "-MIN",
        }

        txn.execute(table_sql)
        row = txn.fetchone()
        if not row:
            # Table is empty, so nothing to do.
            txn.close()
            return

        # Now we fetch the current value from the sequence and compare with the
        # above.
        max_stream_id = row[0]
        txn.execute(
            "SELECT last_value, is_called FROM %(seq)s" % {"seq": self._sequence_name}
        )
        fetch_res = txn.fetchone()
        assert fetch_res is not None
        last_value, is_called = fetch_res

        txn.close()

        # If `is_called` is False then `last_value` is actually the value that
        # will be generated next, so we decrement to get the true "last value".
        if not is_called:
            last_value -= 1

        if max_stream_id > last_value:
            logger.warning(
                "Postgres sequence %s is behind table %s: %d < %d",
                self._sequence_name,
                table,
                last_value,
                max_stream_id,
            )
            raise IncorrectDatabaseSetup(
                _INCONSISTENT_SEQUENCE_ERROR
                % {"seq": self._sequence_name, "table": table, "max_id_sql": table_sql}
            )


GetFirstCallbackType = Callable[[Cursor], int]


class LocalSequenceGenerator(SequenceGenerator):
    """An implementation of SequenceGenerator which uses local locking

    This only works reliably if there are no other worker processes generating IDs at
    the same time.
    """

    def __init__(self, get_first_callback: GetFirstCallbackType):
        """
        Args:
            get_first_callback: a callback which is called on the first call to
                 get_next_id_txn; should return the current maximum id
        """
        # the callback. this is cleared after it is called, so that it can be GCed.
        self._callback: Optional[GetFirstCallbackType] = get_first_callback

        # The current max value, or None if we haven't looked in the DB yet.
        self._current_max_id: Optional[int] = None
        self._lock = threading.Lock()

    def get_next_id_txn(self, txn: Cursor) -> int:
        # We do application locking here since if we're using sqlite then
        # we are a single process synapse.
        with self._lock:
            if self._current_max_id is None:
                assert self._callback is not None
                self._current_max_id = self._callback(txn)
                self._callback = None

            self._current_max_id += 1
            return self._current_max_id

    def get_next_mult_txn(self, txn: Cursor, n: int) -> List[int]:
        with self._lock:
            if self._current_max_id is None:
                assert self._callback is not None
                self._current_max_id = self._callback(txn)
                self._callback = None

            first_id = self._current_max_id + 1
            self._current_max_id += n
            return [first_id + i for i in range(n)]

    def check_consistency(
        self,
        db_conn: Connection,
        table: str,
        id_column: str,
        positive: bool = True,
    ) -> None:
        # There is nothing to do for in memory sequences
        pass


def build_sequence_generator(
    db_conn: "LoggingDatabaseConnection",
    database_engine: BaseDatabaseEngine,
    get_first_callback: GetFirstCallbackType,
    sequence_name: str,
    table: Optional[str],
    id_column: Optional[str],
    positive: bool = True,
) -> SequenceGenerator:
    """Get the best impl of SequenceGenerator available

    This uses PostgresSequenceGenerator on postgres, and a locally-locked impl on
    sqlite.

    Args:
        database_engine: the database engine we are connected to
        get_first_callback: a callback which gets the next sequence ID. Used if
            we're on sqlite.
        sequence_name: the name of a postgres sequence to use.
        table, id_column, stream_name, positive: If set then `check_consistency`
            is called on the created sequence. See docstring for
            `check_consistency` details.
    """
    if isinstance(database_engine, PostgresEngine):
        seq: SequenceGenerator = PostgresSequenceGenerator(sequence_name)
    else:
        seq = LocalSequenceGenerator(get_first_callback)

    if table:
        assert id_column
        seq.check_consistency(
            db_conn=db_conn,
            table=table,
            id_column=id_column,
            positive=positive,
        )

    return seq
