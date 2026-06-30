#!/usr/bin/env python3
# ########################################################################### #
#   shebang: 1                                                                #
#                                                          :::      ::::::::  #
#   data_stream.py                                       :+:      :+:    :+:  #
#                                                      +:+ +:+         +:+    #
#   By: belaindr <belaindr@student.42antananarivo.   +#+  +:+       +#+       #
#                                                  +#+#+#+#+#+   +#+          #
#   Created: 2026/06/17 05:35:53 by belaindr            #+#    #+#            #
#   Updated: 2026/06/29 15:01:22 by belaindr           ###   ########.fr      #
#                                                                             #
# ########################################################################### #

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0
        self._processed: int = 0

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

    def proc_name(self) -> str:
        return "DataProcessor"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
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
            raise Exception("NumericProcessor error - Can't "
                            f"process element in stream: {data}")
        if isinstance(data, (int, float)):
            self._data.append(str(data))
            self._processed += 1
        else:
            for x in data:
                self._data.append(str(x))
                self._processed += 1

    def proc_name(self) -> str:
        return "Numeric Processor"


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
            raise Exception("TextProcessor error - Can't "
                            f"process element in stream: {data}")
        if isinstance(data, str):
            self._data.append(data)
            self._processed += 1
        else:
            for value in data:
                self._data.append(value)
                self._processed += 1

    def proc_name(self) -> str:
        return "Text Processor"


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
            self._processed += 1
        else:
            for dicts in data:
                level = dicts.get('log_level', '')
                msg = dicts.get('log_message', '')
                self._data.append(f"{level}: {msg}")
                self._processed += 1

    def proc_name(self) -> str:
        return "Log Processor"


class DataStream():
    def __init__(self) -> None:
        self._list_proc: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._list_proc.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        if stream is not None:
            for values in stream:
                handeled = False
                if self._list_proc:
                    for proc in self._list_proc:
                        try:
                            proc.ingest(values)
                            handeled = True
                            break
                        except Exception:
                            continue
                    if not handeled:
                        print(f"DataStream error"
                              f" - Can't process element in stream: {values}")
                else:
                    print("No processor found")
        else:
            print("No data found")

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if self._list_proc:
            for proc in self._list_proc:
                print(f"{proc.proc_name()}: "
                      f"total {proc._processed} items processed,"
                      f"remaining {len(proc._data)} on processor")
        else:
            print("No processor found, no data")


def main() -> None:
    print("\nInitialize Data Stream...")

    data_operator = DataStream()
    data_operator.print_processors_stats()

    numericProcessor = NumericProcessor()
    textProcessor = TextProcessor()
    logProcessor = LogProcessor()

    first_back = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message':
                'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil isconnected'}
        ],
        42,
        ['Hi', 'five']
    ]

    print("\nRegistering Numeric Processor")
    try:
        data_operator.register_processor(numericProcessor)

        print(f"\nSend first batch of data on stream: {first_back}")
        data_operator.process_stream(first_back)
        data_operator.print_processors_stats()

        print("\nRegistering other data processors")
        data_operator.register_processor(textProcessor)
        data_operator.register_processor(logProcessor)

        print("\nSend the same batch again")
        data_operator.process_stream(first_back)
        data_operator.print_processors_stats()

        print("\nConsume some elements from the data"
              " processors: Numeric 3, Text 2, Log 1")
        try:
            for _ in range(3):
                numericProcessor.output()

            for _ in range(2):
                textProcessor.output()

            for _ in range(1):
                logProcessor.output()
        except Exception as err:
            print(f"Error: {err}")

        data_operator.print_processors_stats()
    except Exception as error:
        print(f"Something went wrong: {error}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    main()
