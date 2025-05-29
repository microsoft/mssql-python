"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides a way to create a new connection object to interact with the database.
"""
from mssql_python import ddbc_bindings

# Global state: to track if pooling is enabled
_pooling_enabled = False

def enable_pooling(max_size=100, idle_timeout=600):
    """
    Enables connection pooling for all subsequent connect() calls.

    Args:
        max_size (int): Maximum number of connections in each pool.
        idle_timeout (int): Idle timeout in seconds after which connections are pruned.
    """
    global _pooling_enabled
    if _pooling_enabled:
        return

    if max_size <= 0 or idle_timeout < 0:
        raise ValueError("Invalid pooling parameters")

    # ddbc_bindings.enable_pooling(max_size, idle_timeout) //implementation will be added in upcoming PR
    _pooling_enabled = True