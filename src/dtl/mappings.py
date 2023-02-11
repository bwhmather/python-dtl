import dataclasses
import functools
import itertools
from typing import Iterable

from dtl import ir


@dataclasses.dataclass(frozen=True, eq=False)
class Mapping:
    # The identifiers of the two arrays that this mapping connects.
    src: ir.ArrayExpression
    tgt: ir.ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class IdentityMapping(Mapping):
    pass


@dataclasses.dataclass(frozen=True, eq=False)
class ManyToOneMapping(Mapping):
    # Array mapping from rows in the source to indexes of rows in the target.
    tgt_index: ir.ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class OneToManyMapping(Mapping):
    # Array mapping from rows in the target to indexes of rows in the source.
    src_index: ir.ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class ManyToManyMapping(Mapping):
    # Pair of arrays forming a table that maps indexes in the source array into
    # indexes in the target array, and vice versa.
    src_index: ir.ArrayExpression
    tgt_index: ir.ArrayExpression


@functools.singledispatch
def _mappings_for_expression(
    expression: ir.Expression, /
) -> Iterable[Mapping]:
    raise NotImplementedError()


@_mappings_for_expression.register(ir.ShapeExpression)
def _mappings_for_shape_expression(
    expression: ir.ShapeExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.BooleanLiteralExpression)
def _mappings_for_boolean_literal_expression(
    expression: ir.BooleanLiteralExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.IntegerLiteralExpression)
def _mappings_for_integer_literal_expression(
    expression: ir.IntegerLiteralExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.FloatLiteralExpression)
def _mappings_for_float_literal_expression(
    expression: ir.FloatLiteralExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.TextLiteralExpression)
def _mappings_for_text_literal_expression(
    expression: ir.TextLiteralExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.BytesLiteralExpression)
def _mappings_for_bytes_literal_expression(
    expression: ir.BytesLiteralExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.ImportExpression)
def _mappings_for_import_expression(
    expression: ir.ImportExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.WhereExpression)
def _mappings_for_where_expression(
    expression: ir.WhereExpression, /
) -> Iterable[Mapping]:
    full_src_index = ir.RangeExpression(
        dtype=ir.DType.INDEX, shape=expression.mask.shape
    )
    masked_src_index = ir.WhereExpression(
        dtype=ir.DType.INDEX,
        shape=expression.shape,
        source=full_src_index,
        mask=expression.mask,
    )

    tgt_index = ir.RangeExpression(
        dtype=ir.DType.INDEX, shape=expression.shape
    )

    # TODO more efficient representation.
    yield ManyToManyMapping(
        src=expression.mask,
        tgt=expression,
        src_index=masked_src_index,
        tgt_index=tgt_index,
    )
    yield ManyToManyMapping(
        src=expression.source,
        tgt=expression,
        src_index=masked_src_index,
        tgt_index=tgt_index,
    )


@_mappings_for_expression.register(ir.PickExpression)
def _mappings_for_pick_expression(
    expression: ir.PickExpression, /
) -> Iterable[Mapping]:
    yield IdentityMapping(
        src=expression.indexes,
        tgt=expression,
    )
    yield ManyToManyMapping(
        src=expression.source,
        tgt=expression,
        src_index=ir.RangeExpression(
            dtype=ir.DType.INDEX, shape=expression.shape
        ),
        tgt_index=expression.indexes,
    )


@_mappings_for_expression.register(ir.IndexExpression)
def _mappings_for_index_expression(
    expression: ir.IndexExpression, /
) -> Iterable[Mapping]:
    yield IdentityMapping(src=expression.source, tgt=expression)


@_mappings_for_expression.register(ir.JoinLeftExpression)
def _mappings_for_join_left_expression(
    expression: ir.JoinLeftExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.JoinRightExpression)
def _mappings_for_join_right_expression(
    expression: ir.JoinRightExpression, /
) -> Iterable[Mapping]:
    return
    yield


@_mappings_for_expression.register(ir.JoinLeftEqualExpression)
def _mappings_for_join_left_equal_expression(
    expression: ir.JoinLeftEqualExpression, /
) -> Iterable[Mapping]:
    """
    Evaluates to an array of indexes into the left hand expression, where the
    value matches an equivalent value in the right expression.

    If there are multiple matches then the index will be repeated.

    Equivalent to, and generally compiled from:

    .. python::

        join_left = JoinLeftExpression(len(left), len(right))
        join_right = JoinRightExpression(len(left), len(right))
        mask = ir.EqualToExpression(
            PickExpression(left, join_left),
            PickExpression(right, join_right)
        )
        join_left_equal = where(join_left, mask)

    """

    left: ir.ArrayExpression
    right: ir.ArrayExpression
    right_index: ir.ArrayExpression
    raise NotImplementedError()


@_mappings_for_expression.register(ir.JoinRightEqualExpression)
def _mappings_for_join_right_equal_expression(
    expression: ir.JoinRightEqualExpression, /
) -> Iterable[Mapping]:
    """
    Evaluates to an array of indexes into the right hand expression, where the
    value matches an equivalent value in the left expression.

    If there are multiple matches then the index will be repeated.

    Equivalent to, and generally compiled from:

    .. python::

        join_left = JoinLeftExpression(len(left), len(right))
        join_right = JoinRightExpression(len(left), len(right))
        mask = ir.EqualToExpression(
            PickExpression(left, join_left),
            PickExpression(right, join_right)
        )
        join_right_equal = where(join_right, mask)

    """

    left: ir.ArrayExpression
    right: ir.ArrayExpression
    right_index: ir.ArrayExpression
    raise NotImplementedError()


@_mappings_for_expression.register(ir.AddExpression)
def _mappings_for_add_expression(
    expression: ir.AddExpression, /
) -> Iterable[Mapping]:
    yield IdentityMapping(src=expression.source_a, tgt=expression)
    yield IdentityMapping(src=expression.source_b, tgt=expression)


@_mappings_for_expression.register(ir.SubtractExpression)
def _mappings_for_subtract_expression(
    expression: ir.SubtractExpression, /
) -> Iterable[Mapping]:
    yield IdentityMapping(src=expression.source_a, tgt=expression)
    yield IdentityMapping(src=expression.source_b, tgt=expression)


@_mappings_for_expression.register(ir.MultiplyExpression)
def _mappings_for_multiply_expression(
    expression: ir.MultiplyExpression, /
) -> Iterable[Mapping]:
    yield IdentityMapping(src=expression.source_a, tgt=expression)
    yield IdentityMapping(src=expression.source_b, tgt=expression)


@_mappings_for_expression.register(ir.DivideExpression)
def _mappings_for_divide_expression(
    expression: ir.DivideExpression, /
) -> Iterable[Mapping]:
    yield IdentityMapping(src=expression.source_a, tgt=expression)
    yield IdentityMapping(src=expression.source_b, tgt=expression)


@_mappings_for_expression.register(ir.EqualToExpression)
def _mappings_for_equal_to_expression(
    expression: ir.EqualToExpression, /
) -> Iterable[Mapping]:
    yield IdentityMapping(src=expression.source_a, tgt=expression)
    yield IdentityMapping(src=expression.source_b, tgt=expression)


def _generate_candidate_mappings(
    roots: list[ir.Expression],
) -> Iterable[Mapping]:
    for expression in ir.traverse_depth_first(roots):
        yield from _mappings_for_expression(expression)


def _merge_mapping_pair(fst: Mapping, snd: Mapping, /) -> Mapping:
    assert fst.tgt == snd.src

    if isinstance(fst, IdentityMapping) and isinstance(snd, IdentityMapping):
        return IdentityMapping(src=fst.src, tgt=snd.tgt)

    if isinstance(fst, IdentityMapping) and isinstance(snd, ManyToManyMapping):
        return ManyToManyMapping(
            src=fst.src,
            tgt=snd.tgt,
            src_index=snd.src_index,
            tgt_index=snd.tgt_index,
        )

    if isinstance(fst, ManyToManyMapping) and isinstance(snd, IdentityMapping):
        return ManyToManyMapping(
            src=fst.src,
            tgt=snd.tgt,
            src_index=fst.src_index,
            tgt_index=fst.tgt_index,
        )

    if isinstance(fst, ManyToManyMapping) and isinstance(
        snd, ManyToManyMapping
    ):
        # Roughly equivalent to:
        #   SELECT
        #       fst.src_index
        #       snd.tgt_index
        #   FROM
        #       fst
        #   JOIN
        #       snd
        #   ON
        #       fst.tgt_index = snd.src_index
        #
        # We generate the join naively and rely on the optimiser to turn it into
        # nice fast `ir.JoinLeftEqualExpression`s.

        # Indexes into the first and second mappings that represent a full join
        # of the two tables.
        shape_full = ir.JoinShapeExpression(
            shape_a=fst.tgt_index.shape, shape_b=snd.src_index.shape
        )

        fst_index_full = ir.JoinLeftExpression(
            dtype=ir.DType.INDEX,
            shape=shape_full,
            shape_a=fst.tgt_index.shape,
            shape_b=snd.src_index.shape,
        )
        snd_index_full = ir.JoinRightExpression(
            dtype=ir.DType.INDEX,
            shape=shape_full,
            shape_a=fst.tgt_index.shape,
            shape_b=snd.src_index.shape,
        )

        # The value of `fst.tgt_index` and `snd.src_index` at each row in the
        # full join.  Used to evaluate `fst.tgt_index = snd.src_index` to get
        # the mask.
        # TODO better names.
        src_index_full = ir.PickExpression(
            dtype=fst.tgt_index.dtype,
            shape=shape_full,
            source=fst.tgt_index,
            indexes=fst_index_full,
        )
        tgt_index_full = ir.PickExpression(
            dtype=snd.src_index.dtype,
            shape=shape_full,
            source=snd.src_index,
            indexes=snd_index_full,
        )

        # The mask to apply to the indexes of the full join to get the rows of
        # the filtered join.
        mask = ir.EqualToExpression(
            dtype=ir.DType.BOOL,
            shape=shape_full,
            source_a=src_index_full,
            source_b=tgt_index_full,
        )

        # Indexes into the first and second mask that give the rows of the
        # filtered join.
        shape = ir.WhereShapeExpression(mask=mask)
        fst_index = ir.WhereExpression(
            dtype=ir.DType.INDEX,
            shape=shape,
            mask=mask,
            source=fst_index_full,
        )
        snd_index = ir.WhereExpression(
            dtype=ir.DType.INDEX,
            shape=shape,
            mask=mask,
            source=snd_index_full,
        )

        # Finally, the output of the filtered join!
        src_index = ir.PickExpression(
            dtype=fst.src_index.dtype,
            shape=shape,
            source=fst.src_index,
            indexes=fst_index,
        )
        tgt_index = ir.PickExpression(
            dtype=snd.tgt_index.dtype,
            shape=shape,
            source=snd.tgt_index,
            indexes=snd_index,
        )

        return ManyToManyMapping(
            src=fst.src,
            tgt=snd.tgt,
            src_index=src_index,
            tgt_index=tgt_index,
        )

    # TODO should be able to merge any sort of mapping.
    raise NotImplementedError(f"cannot merge {type(fst)} and {type(snd)}")


def _merge_mappings(
    mappings: Iterable[Mapping], /, *, roots: list[ir.Expression]
) -> Iterable[Mapping]:
    mappings = list(mappings)

    # Build dictionary mapping from sources to sets of mappings.
    mappings_by_src: dict[ir.Expression, set[Mapping]] = {}
    for mapping in mappings:
        mappings_by_src.setdefault(mapping.src, set()).add(mapping)

    mappings_by_tgt: dict[ir.Expression, set[Mapping]] = {}
    for mapping in mappings:
        mappings_by_tgt.setdefault(mapping.tgt, set()).add(mapping)

    # Set of all intermediate expressions that a e not in the roots list.
    nonroots = {*mappings_by_src, *mappings_by_tgt}.difference(roots)

    for nonroot in nonroots:
        # This looks back to front, but isn't.  The `src_mappings` are the
        # mappings from which we keep the source, `tgt_mappings` the target.
        # They are joined in the middle.
        src_mappings = mappings_by_tgt.pop(nonroot, set())
        tgt_mappings = mappings_by_src.pop(nonroot, set())

        for src_mapping, tgt_mapping in itertools.product(
            src_mappings, tgt_mappings
        ):
            mapping = _merge_mapping_pair(src_mapping, tgt_mapping)
            assert mapping.src == src_mapping.src
            assert mapping.tgt == tgt_mapping.tgt

            mappings_by_src[mapping.src].add(mapping)
            mappings_by_tgt[mapping.tgt].add(mapping)

        for mapping in src_mappings:
            mappings_by_src[mapping.src].remove(mapping)

        for mapping in tgt_mappings:
            mappings_by_tgt[mapping.tgt].remove(mapping)

    for group in mappings_by_src.values():
        yield from group


def generate_mappings(roots: list[ir.Expression]) -> Iterable[Mapping]:
    mappings = list(_generate_candidate_mappings(roots))
    return _merge_mappings(mappings, roots=roots)
