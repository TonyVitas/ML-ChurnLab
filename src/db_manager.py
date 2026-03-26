"""
Oracle DB connection module
"""

import oracledb
from contextlib import contextmanager
from typing import Optional


class OracleConnectionManager:

    def __init__(
        self,
        user: str,
        password: str,
        dsn: str,
        *,
        min_pool_size: int = 1,
        max_pool_size: int = 5,
        increment: int = 1,
    ):
        self.user = user
        self.password = password
        self.dsn = dsn
        self._pool: Optional[oracledb.ConnectionPool] = None
        self._pool_params = {
            "min": min_pool_size,
            "max": max_pool_size,
            "increment": increment,
        }

    def _create_pool(self) -> oracledb.ConnectionPool:
        """Create a new connection pool."""
        return oracledb.create_pool(
            user=self.user,
            password=self.password,
            dsn=self.dsn,
            **self._pool_params,
        )

    @property
    def pool(self) -> oracledb.ConnectionPool:
        """Lazy-init and return the connection pool."""
        if self._pool is None:
            self._pool = self._create_pool()
        return self._pool

    def validate_connection(self, conn: oracledb.Connection) -> bool:
        """
        Validate that a connection is alive.
        """
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            return True
        except oracledb.Error:
            return False

    def get_connection(self) -> oracledb.Connection:
        """
        Get a connection from the pool, validating it.
        If validation fails, the pool will issue a fresh connection on next acquire.
        """
        conn = self.pool.acquire()
        if not self.validate_connection(conn):
            try:
                conn.close()
            except oracledb.Error:
                pass
            conn = self.pool.acquire()
            if not self.validate_connection(conn):
                raise oracledb.Error("Could not obtain a valid connection")
        return conn

    @contextmanager
    def connection(self):
        """Context manager: get validated connection and release when done."""
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.release(conn)

    def close(self):
        """Close the connection pool."""
        if self._pool is not None:
            try:
                self._pool.close(force=True)
            except oracledb.Error:
                pass
            finally:
                self._pool = None
