import pyarrow as pa

from dtl import run_simple


def test_import_export():
    pass


def test_rename_columns():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT old_column AS new_column FROM input;
    EXPORT output TO 'output';
    """
    inputs = {"input": pa.table({"old_column": [1, 2, 3, 4]})}
    outputs = run_simple(src, inputs=inputs)
    assert outputs["output"] == pa.table({"new_column": [1, 2, 3, 4]})


def test_add_function():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT a, b, add(a, b) AS c FROM input;
    EXPORT output TO 'output';
    """
    inputs = {"input": pa.table({"a": [1, 2, 3, 4], "b": [3, 4, 5, 6]})}
    outputs = run_simple(src, inputs=inputs)
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
    outputs = run_simple(src, inputs=inputs)
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

    outputs = run_simple(src, inputs=inputs)
    assert outputs["output"] == pa.table({"r": [-30, -40]})


def test_precedence():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT a + b * c AS r FROM input;
    EXPORT output TO 'output';
    """

    inputs = {"input": pa.table({"a": [12, 54], "b": [2, 3], "c": [10, 20]})}

    outputs = run_simple(src, inputs=inputs)
    assert outputs["output"] == pa.table({"r": [32, 114]})


def test_join_on_simple():
    src = """
    WITH a AS IMPORT 'input_a';
    WITH b AS IMPORT 'input_b';
    WITH output AS
        SELECT key, a.value AS a, b.value AS b
        FROM a
        JOIN b ON a.key = b.key;
    EXPORT output TO 'output';
    """

    inputs = {
        "input_a": pa.table(
            {
                "key": [1, 2, 3, 4, 5],
                "value": ["one", "two", "three", "four", "five"],
            }
        ),
        "input_b": pa.table(
            {
                "key": [4, 3, 1],
                "value": ["FOUR", "THREE", "ONE"],
            }
        ),
    }
    outputs = run_simple(src, inputs=inputs)
    assert outputs["output"] == pa.table(
        {
            "key": [1, 3, 4],
            "a": ["one", "three", "four"],
            "b": ["ONE", "THREE", "FOUR"],
        }
    )


def test_add_literal():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS SELECT a + 1 AS a_plus_one FROM input;
    EXPORT output TO 'output';
    """
    inputs = {"input": pa.table({"a": [1, 2, 3]})}
    outputs = run_simple(src, inputs=inputs)
    assert outputs["output"] == pa.table({"a_plus_one": [2, 3, 4]})


def test_join_on_less_simple():
    src = """
    WITH a AS IMPORT 'input_a';
    WITH b AS IMPORT 'input_b';
    WITH output AS
        SELECT key, a.value AS a, b.value AS b
        FROM a
        JOIN b ON a.key + 1 = b.key;
    EXPORT output TO 'output';
    """

    inputs = {
        "input_a": pa.table(
            {
                "key": [1, 2, 3, 4, 5],
                "value": ["one", "two", "three", "four", "five"],
            }
        ),
        "input_b": pa.table(
            {
                "key": [4, 3, 1],
                "value": ["FOUR", "THREE", "ONE"],
            }
        ),
    }
    outputs = run_simple(src, inputs=inputs)
    assert outputs["output"] == pa.table(
        {
            "key": [2, 3],
            "a": ["two", "three"],
            "b": ["THREE", "FOUR"],
        }
    )


def test_where_simple():
    src = """
    WITH input AS IMPORT 'input';
    WITH output AS
        SELECT *
        FROM input
        WHERE input.a = input.b;
    EXPORT output TO 'output';
    """

    inputs = {
        "input": pa.table(
            {
                "a": [1, 2, 3, 4, 5, 0],
                "b": [5, 4, 3, 2, 1, 0],
            }
        ),
    }
    outputs = run_simple(src, inputs=inputs)
    assert outputs["output"] == pa.table(
        {
            "a": [3, 0],
            "b": [3, 0],
        }
    )
