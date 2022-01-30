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


def test_add_function():
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


def test_recursive_add_function():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT a, b, add(add(a, b), c) AS c FROM input;
    EXPORT output TO 'output';
    """
    inputs = {
        "input": pa.table(
            {"a": [1, 2, 3, 4], "b": [3, 4, 5, 6], "c": [6, 5, 4, 3]}
        )
    }
    outputs = evaluate(src, inputs)
    assert outputs["output"] == pa.table(
        {"a": [1, 2, 3, 4], "b": [3, 4, 5, 6], "c": [10, 11, 12, 13]}
    )


def test_associativity():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT a - b - c AS r FROM input;
    EXPORT output TO 'output';
    """

    inputs = {"input": pa.table({"a": [2, 3], "b": [12, 13], "c": [20, 30]})}

    outputs = evaluate(src, inputs)
    assert outputs["output"] == pa.table({"r": [-30, -40]})


def test_precedence():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT a + b * c AS r FROM input;
    EXPORT output TO 'output';
    """

    inputs = {"input": pa.table({"a": [12, 54], "b": [2, 3], "c": [10, 20]})}

    outputs = evaluate(src, inputs)
    assert outputs["output"] == pa.table({"r": [32, 114]})
