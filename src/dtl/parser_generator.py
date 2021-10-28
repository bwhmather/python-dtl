import dataclasses
import typing
from typing import List

import lalr


def _is_list_type(cls):
    if cls is list:
        return True

    # TODO change to types.GenericAlias in python39.
    if not isinstance(cls, typing._GenericAlias):
        return False

    if cls.__origin__ is not list:
        return False

    return True


def _list_item_type(cls):
    assert _is_list_type(cls)
    return cls.__args__[0]


def _create_empty_instance(cls):
    if _is_list_type(cls):
        return []

    if dataclasses.is_dataclass(cls):
        kwargs = {
            field.name: _create_empty_instance(field.type)
            for field in dataclasses.fields(cls)
        }
        return cls(**kwargs)

    raise NotImplementedError(f"can't create empty instance for {cls}")


def _reduce_first(node):
    return [node]


def _reduce_subsequent(prev, node):
    return prev + [node]


class UniqueQueue:
    """
    A FIFO queue from which unique values can only be popped once.
    """

    def __init__(self, initial_values=[]):
        self.__cursor = 0
        self.__list = list()
        self.__set = set()
        for value in initial_values:
            self.__list.append(value)
            self.__set.add(value)

    def push(self, value):
        if value not in self.__set:
            self.__list.append(value)
            self.__set.add(value)

    def pop(self):
        value = self.__list[self.__cursor]
        self.__cursor += 1
        return value

    def __len__(self):
        return len(self.__list) - self.__cursor


class Parser:
    def __init__(self, productions, *, target, actions):
        self._grammar = lalr.Grammar(productions)
        self._parse_table = lalr.ParseTable(self._grammar, target)
        self._actions = actions

    @staticmethod
    def _token_symbol(token):
        return type(token)

    @staticmethod
    def _token_value(token):
        return token

    def _action(self, production, *values):
        action = self._actions[production]
        result = action(*values)
        return result

    def parse(self, tokens):
        try:
            return lalr.parse(
                self._parse_table,
                tokens,
                action=self._action,
                token_symbol=self._token_symbol,
                token_value=self._token_value,
            )
        except lalr.exceptions.ParseError as exc:
            print(exc)
            raise


class ParserGenerator:
    def __init__(self, *, token_type, node_type):
        self.token_type = token_type
        self.node_type = node_type

        self._productions = []
        self._actions = {}

    def _add_production(self, cls, pattern, *, action):
        production = lalr.Production(cls, pattern)
        self._productions.append(production)
        self._actions[production] = action
        return production

    def register(self, cls, pattern, **actions):
        pattern = tuple(pattern)

        if not issubclass(cls, self.node_type):
            raise TypeError(f"{cls} is not a subclass of {self.node_type}")

        for term in pattern:
            term_type = _list_item_type(term) if _is_list_type(term) else term
            if not issubclass(term_type, (self.node_type, self.token_type)):
                raise TypeError(
                    f"{term_type} is not a subclass of {self.node_type} or {self.token_type}"
                )

        # Add productions to automatically cast sub classes to their super class.
        for base_cls in cls.__bases__:
            if (
                issubclass(base_cls, self.node_type)
                and base_cls is not self.node_type
            ):
                self._add_production(
                    base_cls, (cls,), action=lambda node: node
                )

        pattern_iter = iter(enumerate(pattern))
        names = [None] * len(pattern)
        index, pattern_type = next(pattern_iter)
        for field in dataclasses.fields(cls):
            if field.name in actions:
                continue

            while pattern_type is not field.type:
                index, pattern_type = next(pattern_iter)
            names[index] = field.name
        names = tuple(names)

        def _action(*args):
            kwargs = {
                name: value
                for name, value in zip(names, args)
                if name is not None
            }

            for name, action in actions.items():
                kwargs[name] = action(*args)

            return cls(**kwargs)

        self._add_production(cls, pattern, action=_action)

    def _create_list_productions(self):
        _appears_in_list = set()
        for production in self._productions:
            for symbol in production.symbols:
                if _is_list_type(symbol):
                    (cls,) = symbol.__args__
                    _appears_in_list.add(cls)

        for cls in _appears_in_list:
            self._add_production(List[cls], (cls,), action=_reduce_first)
            self._add_production(
                List[cls], (List[cls], cls), action=_reduce_subsequent
            )

    def _create_empty_variants(self):
        _appears_in_list = set()
        for production in self._productions:
            for symbol in production.symbols:
                if _is_list_type(symbol):
                    (cls,) = symbol.__args__
                    _appears_in_list.add(cls)

        empty_queue = UniqueQueue(List[cls] for cls in _appears_in_list)

        # Add list symbols to the empty queue.

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

                            def _create_action(
                                production, index, empty_symbol
                            ):
                                def _action(*args):
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

    def parser(self, *, target):
        self._create_list_productions()
        self._create_empty_variants()

        return Parser(self._productions, target=target, actions=self._actions)
