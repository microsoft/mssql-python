"""Parametrize, not retry: one hundred independent normal fixture executions."""

import pytest


@pytest.fixture(autouse=True, params=range(100))
def qualification_iteration(request):
    return request.param
