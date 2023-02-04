from . import shared

def test_dag_import():
    from . import example_dag
    shared.assert_has_valid_dag(example_dag)