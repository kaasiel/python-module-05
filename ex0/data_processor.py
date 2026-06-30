#!/usr/bin/env python3
# ########################################################################### #
#   shebang: 1                                                                #
#                                                          :::      ::::::::  #
#   data_processor.py                                    :+:      :+:    :+:  #
#                                                      +:+ +:+         +:+    #
#   By: belaindr <belaindr@student.42antananarivo.   +#+  +:+       +#+       #
#                                                  +#+#+#+#+#+   +#+          #
#   Created: 2026/06/09 16:06:12 by belaindr            #+#    #+#            #
#   Updated: 2026/06/29 15:02:00 by belaindr           ###   ########.fr      #
#                                                                             #
# ########################################################################### #

from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        value: str = ""
        if self._data:
            value = self._data.pop(0)
            self._rank += 1
        return (self._rank - 1, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for x in data:
                if not isinstance(x, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        valid = self.validate(data)
        if not valid:
            raise Exception("Improper numeric data")
        if isinstance(data, (int, float)):
            self._data.append(str(data))
        else:
            for x in data:
                self._data.append(str(x))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for value in data:
                if not isinstance(value, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        valid = self.validate(data)
        if not valid:
            raise Exception("Improper Alphanumeric Data")
        if isinstance(data, str):
            self._data.append(data)
        else:
            for value in data:
                self._data.append(value)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True
        elif (isinstance(data, list)):
            for dict_data in data:
                if not isinstance(dict_data, dict):
                    return False
                for key, value in dict_data.items():
                    if not isinstance(key, str) or not isinstance(value, str):
                        return False
            return True
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        valid = self.validate(data)
        if not valid:
            raise Exception("Logprocessor error - Can't "
                            f"process element in stream: {data}")
        if isinstance(data, dict):
            level = data.get('log_level', '')
            msg = data.get('log_message', '')
            self._data.append(f"{level}: {msg}")
        else:
            for dicts in data:
                level = dicts.get('log_level', '')
                msg = dicts.get('log_message', '')
                self._data.append(f"{level}: {msg}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    data = NumericProcessor()
    print(f"Trying to validate input '42': {data.validate(42)}")
    print(f"Trying to validate input 'hello': {data.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        data.ingest('foo')  # type: ignore
    except Exception as error:
        print(error)
    print("Processing data: [1, 2, 3, 4]")
    try:
        data.ingest([1, 2, 3, 4])
        print("Extracting 3 values...")
        for _ in range(3):
            rank, value = data.output()
            print(f"Numeric value {rank}: {value}")
    except Exception as error:
        print(f"caught error: {error}")

    print("\nTesting Text Processor...")
    text_data = TextProcessor()
    print(f"Trying to validate input '42': {text_data.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    try:
        text_data.ingest(['Hello', 'Nexus', 'World'])
        print("Extracting 1 values...")
        rank, value = text_data.output()
        print(f"Numeric value {rank}: {value}")
    except Exception as error:
        print(f"caught error: {error}")

    print("\nTesting Log Processor...")
    log_data = LogProcessor()
    print(f"Trying to validate input 'Hello': {log_data.validate('hello')}")
    log_top = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
        ]
    print(f"Processing data: [{log_top}]")
    try:
        log_data.ingest(log_top)
        print("Extracting 2 values...")
        for _ in range(2):
            rank, value = log_data.output()
            print(f"Log entry {rank}: {value}")
    except Exception as error:
        print(f"caught Error: {error}")
