"""
Temporary dummy test used to verify that the PR pipeline correctly reports
failing tests. This test is intentionally designed to FAIL.

TODO: Remove this file once the pipeline failure-detection behavior has been
confirmed.
"""


def test_dummy_intentional_failure():
    # Intentionally failing assertion to validate pipeline failure detection.
    assert 1 == 2, "Intentional failure to verify the PR pipeline catches test failures"
