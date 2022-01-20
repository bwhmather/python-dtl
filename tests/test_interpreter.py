import pyarrow as pa

from dtl.eval import evaluate


def test_import_export():
    pass


def test_rename_columns():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT old_column AS new_column FROM input;
    EXPORT output TO 'output';
    """
    inputs = {"input": pa.table({"old_column": [1, 2, 3, 4]})}
    outputs = evaluate(src, inputs)
    assert outputs["output"] == pa.table({"new_column": [1, 2, 3, 4]})


def test_add_columns():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT a, b, add(a, b) AS c FROM input;
    EXPORT output TO 'output';
    """
    inputs = {"input": pa.table({"a": [1, 2, 3, 4], "b": [3, 4, 5, 6]})}
    outputs = evaluate(src, inputs)
    assert outputs["output"] == pa.table(
        {"a": [1, 2, 3, 4], "b": [3, 4, 5, 6], "c": [4, 6, 8, 10]}
    )
