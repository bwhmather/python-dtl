import dataclasses
import typing
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
    cast,
    get_type_hints,
)

import lalr

T = TypeVar("T")
TT = TypeVar("TT")
NT = TypeVar("NT")


@dataclasses.dataclass(frozen=True)
class Delimiter:
    delimiter: typing.Any


class ParseError(Exception):
    pass


def _is_list_type(cls: type) -> bool:
    while hasattr(cls, "__origin__"):
        cls = cls.__origin__  # type: ignore

    return cls is list


def _list_item_type(cls: type) -> type:
    assert _is_list_type(cls)

    while cls.__origin__ != list:  # type: ignore
        cls = cls.__origin__  # type: ignore

    return cls.__args__[0]  # type: ignore


def _list_delimiter(cls: type[list]) -> Optional[Any]:
    assert _is_list_type(cls)
    if not hasattr(cls, "__metadata__"):
        return None

    for tag in cls.__metadata__:  # type: ignore
        if not isinstance(tag, Delimiter):
            continue
        return tag.delimiter

    return None


def _is_optional_type(cls: type) -> bool:
    if typing.get_origin(cls) is not Union:
        return False

    if type(None) not in typing.get_args(cls):
        return False

    return True


def _optional_item_type(cls: type) -> type:
    assert _is_optional_type(cls)
    variants = tuple(
        variant
        for variant in typing.get_args(cls)
        if variant is not type(None)
    )
    if len(variants) == 1:
        return variants[0]  # type: ignore
    return Union[(*variants,)]  # type: ignore


def _is_subclass(cls: type, base: type) -> bool:
    if _is_list_type(cls) or _is_list_type(base):
        if not _is_list_type(cls) or not _is_list_type(base):
            return False

        return issubclass(_list_item_type(cls), _list_item_type(base))

    if _is_optional_type(base):
        base = _optional_item_type(base)

        if _is_optional_type(cls):
            cls = _optional_item_type(cls)

    if _is_optional_type(cls):
        return False

    try:
        return issubclass(cls, base)
    except TypeError:
        print(f"{cls!r}, {base!r}")
        raise


def _create_empty_instance(cls: type[T]) -> T:
    if _is_list_type(cls):
        return []  # type: ignore

    if _is_optional_type(cls):
        return None  # type: ignore

    if dataclasses.is_dataclass(cls):
        kwargs = {
            field.name: _create_empty_instance(field.type)
            for field in dataclasses.fields(cls)
        }
        return cls(**kwargs)

    raise NotImplementedError(f"can't create empty instance for {cls}")


class UniqueQueue(Generic[T]):
    """
    A FIFO queue from which unique values can only be popped once.
    """

    def __init__(self, initial_values: list[T] = []):
        self.__cursor = 0
        self.__list = list()
        self.__set = set()
        for value in initial_values:
            self.__list.append(value)
            self.__set.add(value)

    def push(self, value: T) -> None:
        if value not in self.__set:
            self.__list.append(value)
            self.__set.add(value)

    def pop(self) -> T:
        value = self.__list[self.__cursor]
        self.__cursor += 1
        return value

    def __len__(self) -> int:
        return len(self.__list) - self.__cursor


def _describe(symbol: Any) -> str:
    if _is_optional_type(symbol):
        cls = _optional_item_type(symbol)
    elif _is_list_type(symbol):
        cls = _list_item_type(symbol)
    else:
        cls = symbol

    return cls.__name__


def _or_list(values: Iterable[Any]) -> str:
    # Values may not be sortable so we convert to strings first.
    names = sorted(str(value) for value in values)
    if len(names) > 1:
        return ", ".join(names[:-1]) + " or " + names[-1]
    else:
        return names[0]


class Parser(Generic[TT, NT]):
    def __init__(
        self,
        productions: list[lalr.Production],
        *,
        precedence_sets: list[lalr.Precedence],
        target: type[NT],
        actions: dict[lalr.Production, Callable],
    ):
        self._productions = productions
        self._grammar = lalr.Grammar(
            productions, precedence_sets=precedence_sets
        )
        self._parse_table = lalr.ParseTable(self._grammar, target)
        self._actions = actions

    @staticmethod
    def _token_symbol(token: TT) -> type[TT]:
        return type(token)

    @staticmethod
    def _token_value(token: TT) -> TT:
        return token

    def _action(self, production: lalr.Production, *values: TT | NT) -> NT:
        action = self._actions[production]
        result = action(*values)
        return cast(NT, result)

    # TODO should be possible to bound return type to the target type.
    def parse(self, tokens: Iterable[TT]) -> NT:
        try:
            return cast(
                NT,
                lalr.parse(
                    self._parse_table,
                    tokens,
                    action=self._action,
                    token_symbol=self._token_symbol,
                    token_value=self._token_value,
                ),
            )
        except lalr.exceptions.ParseError as exc:
            print(self)
            lookahead_token = exc.lookahead_token
            expected_symbols = exc.expected_symbols

            raise ParseError(
                f"expected {_or_list(_describe(sym) for sym in expected_symbols)} "
                f"before {lookahead_token if lookahead_token is not None else 'EOF'}"
            ) from exc

    def __str__(self) -> str:
        def _symbol_repr(symbol: type[NT | TT]) -> str:
            if _is_list_type(symbol):
                item_type = _symbol_repr(_list_item_type(symbol))
                delimiter = _symbol_repr(_list_delimiter(symbol))  # type: ignore
                if delimiter:
                    return f"List[{item_type}, delimiter={delimiter}]"
                else:
                    return f"List[{item_type}]"

            if _is_optional_type(symbol):
                item_type = _optional_item_type(symbol).__name__
                return f"Optional[{item_type}]"

            if hasattr(symbol, "__name__"):
                return symbol.__name__

            return repr(symbol)

        import io

        buff = io.StringIO()
        for production in self._productions:
            buff.write(_symbol_repr(production.name))
            buff.write(" => ")
            buff.write(
                ", ".join(
                    _symbol_repr(symbol) for symbol in production.symbols
                )
            )
            buff.write("\n")
        return buff.getvalue()


class ParserGenerator(Generic[TT, NT]):
    # TODO `type[T]` is covariant rather than invariant.
    def __init__(
        self,
        *,
        token_type: type[TT],
        node_type: type[NT],
        default_actions: dict[str, Callable],
    ):
        self.token_type = token_type
        self.node_type = node_type
        self._default_actions = default_actions

        self._productions: list[lalr.Production] = []
        self._actions: dict[lalr.Production, Callable] = {}

        self._precedence_sets: list[lalr.Precedence] = []

    def _add_production(
        self,
        cls: Any,
        pattern: list[
            type[NT] | type[TT] | type[Optional[NT]] | type[Optional[TT]]
        ],
        *,
        action: Callable,
    ) -> lalr.Production:
        production = lalr.Production(cls, pattern)
        self._productions.append(production)
        self._actions[production] = action
        print(production)
        return production

    def register(
        self,
        cls: type[NT],
        pattern: list[
            type[NT] | type[TT] | type[Optional[NT]] | type[Optional[TT]]
        ],
        **actions: Callable,
    ) -> None:
        if not issubclass(cls, self.node_type):
            raise TypeError(f"{cls} is not a subclass of {self.node_type}")

        for term in pattern:
            if _is_list_type(term):
                term_type = _list_item_type(term)
            elif _is_optional_type(term):
                term_type = _optional_item_type(term)
            else:
                term_type = term

            print(repr(term_type))
            if not issubclass(term_type, (self.node_type, self.token_type)):
                raise TypeError(
                    f"{term_type} is not a subclass of {self.node_type} or {self.token_type}"
                )

        actions = {**self._default_actions, **actions}
        pattern_iter = iter(enumerate(pattern))
        names: list[Optional[str]] = [None] * len(pattern)

        fields = dataclasses.fields(cls)
        hints = get_type_hints(cls, globalns=None, localns=None)

        for field in fields:
            if field.name in actions:
                continue

            while True:
                try:
                    index, pattern_type = next(pattern_iter)
                except StopIteration:
                    raise Exception(f"No binding found for {field.name!r}")

                if _is_subclass(pattern_type, hints[field.name]):
                    break

            names[index] = field.name

        def _action(*args: TT | NT | None) -> NT:
            kwargs = {
                name: value
                for name, value in zip(names, args)
                if name is not None
            }

            for name, action in actions.items():
                kwargs[name] = action(*args)

            return cls(**kwargs)

        self._add_production(cls, pattern, action=_action)

    def left(self, *args: type[TT]) -> None:
        self._precedence_sets.append(lalr.Left(*args))

    def right(self, *args: type[TT]) -> None:
        self._precedence_sets.append(lalr.Right(*args))

    def precedence(self, *args: type[TT]) -> None:
        self._precedence_sets.append(lalr.Precedence(*args))

    def _create_superclass_productions(self) -> None:
        """
        Add productions to automatically cast sub classes to their super class.
        """
        subclasses: UniqueQueue[type[TT] | type[NT]] = UniqueQueue()
        for production in self._productions:
            cls = production.name
            if _is_list_type(cls):
                continue
            if _is_optional_type(cls):
                cls = _optional_item_type(cls)
            subclasses.push(cls)

        while subclasses:
            subcls = subclasses.pop()
            for supercls in subcls.__bases__:
                if not issubclass(supercls, self.node_type):
                    continue
                if supercls is self.node_type:
                    continue
                self._add_production(
                    supercls, [subcls], action=lambda node: node
                )
                subclasses.push(supercls)

    def _create_list_productions(self) -> None:
        _appears_in_list = set()
        for production in self._productions:
            for symbol in production.symbols:
                if _is_list_type(symbol):
                    _appears_in_list.add(
                        (_list_item_type(symbol), _list_delimiter(symbol))
                    )

        for cls, delimiter in _appears_in_list:
            if delimiter:

                def _reduce_first(node):  # type: ignore
                    return [node]

                def _reduce_subsequent(prev, delimiter, node):  # type: ignore
                    return prev + [node]

                name = Annotated[List[cls], Delimiter(delimiter)]  # type: ignore
                self._add_production(name, [cls], action=_reduce_first)
                self._add_production(
                    name,
                    [name, delimiter, cls],  # type: ignore
                    action=_reduce_subsequent,
                )

            else:

                def _reduce_first(node):  # type: ignore
                    return [node]

                def _reduce_subsequent(prev, node):  # type: ignore
                    return prev + [node]

                name = List[cls]  # type: ignore
                self._add_production(name, [cls], action=_reduce_first)
                self._add_production(
                    name,
                    [name, cls],  # type: ignore
                    action=_reduce_subsequent,
                )

    def _create_optional_productions(self) -> None:
        appears_in_optional = set()

        for production in self._productions:
            for symbol in production.symbols:
                if _is_optional_type(symbol):
                    appears_in_optional.add(_optional_item_type(symbol))

        for cls in appears_in_optional:
            self._add_production(
                Optional[cls], [cls], action=lambda value: value
            )

    def _create_empty_variants(self) -> None:
        empty_queue: UniqueQueue[type[TT] | type[NT]] = UniqueQueue()

        for production in self._productions:
            for symbol in production.symbols:
                if _is_list_type(symbol) or _is_optional_type(symbol):
                    empty_queue.push(symbol)

        while empty_queue:
            empty_symbol = empty_queue.pop()

            expand_queue = UniqueQueue(self._productions)
            while expand_queue:
                production = expand_queue.pop()

                # For each instance of the empty class in the current
                # production, create a new production and push it on to the end
                # of the expansion queue.
                for index, symbol in enumerate(production.symbols):
                    if symbol == empty_symbol:
                        if len(production.symbols) == 1:
                            empty_queue.push(production.name)
                        else:

                            def _create_action(  # type: ignore
                                production, index, empty_symbol  # type: ignore
                            ):  # type: ignore
                                def _action(*args):  # type: ignore
                                    return self._actions[production](
                                        *args[:index],
                                        _create_empty_instance(empty_symbol),
                                        *args[index:],
                                    )

                                return _action

                            new_production = self._add_production(
                                production.name,
                                production.symbols[:index]
                                + production.symbols[index + 1 :],
                                action=_create_action(
                                    production, index, empty_symbol
                                ),
                            )
                            expand_queue.push(new_production)

    def parser(self, *, target: type[NT]) -> Parser[TT, NT]:
        self._create_superclass_productions()
        self._create_empty_variants()
        self._create_list_productions()
        self._create_optional_productions()
        return Parser(
            self._productions,
            precedence_sets=self._precedence_sets,
            target=target,
            actions=self._actions,
        )
