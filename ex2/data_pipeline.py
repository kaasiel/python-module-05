#!/usr/bin/env python3
# ########################################################################### #
#   shebang: 1                                                                #
#                                                          :::      ::::::::  #
#   data_pipeline.py                                     :+:      :+:    :+:  #
#                                                      +:+ +:+         +:+    #
#   By: belaindr <belaindr@student.42antananarivo.   +#+  +:+       +#+       #
#                                                  +#+#+#+#+#+   +#+          #
#   Created: 2026/06/19 12:09:21 by belaindr            #+#    #+#            #
#   Updated: 2026/06/29 15:00:43 by belaindr           ###   ########.fr      #
#                                                                             #
# ########################################################################### #

from abc import ABC, abstractmethod
from typing import Any, Protocol


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
        if value == "":
            raise Exception("empty or emptied stash")
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


class ExportPlugin(Protocol):
    def processor_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVexport():
    def processor_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        res: list[str] = [item[1] for item in data]
        print("CSV Output: ")
        print(",".join(res))


class JSONexport():
    def processor_output(self, data: list[tuple[int, str]]) -> None:
        res: list[str] = []
        res = [f'"item_{rank}": "{value}"' for rank, value in data]
        print("JSON Output:")
        print("{" + ", ".join(res) + "}")


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
                        print(f"DataStream error -"
                              f" Can't process element in stream: {values}")
                else:
                    print("No processor found")

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if self._list_proc:
            for proc in self._list_proc:
                print(f"{proc.proc_name()}: "
                      f"total {proc._processed} items processed,"
                      f"remaining {len(proc._data)} on processor")
        else:
            print("No processor found, no data")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._list_proc:
            res: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    res.append(proc.output())
                except Exception:
                    pass
            if res:
                plugin.processor_output(res)


def main() -> None:
    print("\nInitialize Data Stream...")

    data_operator = DataStream()
    data_operator.print_processors_stats()

    numericProcessor = NumericProcessor()
    textProcessor = TextProcessor()
    logProcessor = LogProcessor()
    CSVexporter = CSVexport()
    JSONexporter = JSONexport()

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
    second_batch = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'},
            {'log_level': 'NOTICE',
             'log_message': 'Certificateexpires in 10 days'}
        ], [32, 42, 64, 84, 128, 168], 'World hello']

    print("\nRegistering Processor")
    try:
        data_operator.register_processor(numericProcessor)
        data_operator.register_processor(textProcessor)
        data_operator.register_processor(logProcessor)

        print(f"\nSend first batch of data on stream: {first_back}")
        data_operator.process_stream(first_back)

        data_operator.print_processors_stats()

        print("\nSend 3 processed data from each processor to a CSV plugin:")
        try:
            data_operator.output_pipeline(3, CSVexporter)
        except Exception as error:
            print(f"nisy erreur {error}")
        data_operator.print_processors_stats()

        print(f"\nSend another batch of data: {second_batch}")
        data_operator.process_stream(second_batch)
        print()
        data_operator.print_processors_stats()

        print("\nSend 5 processed data from each processor to a JSON plugin:")
        try:
            data_operator.output_pipeline(5, JSONexporter)
        except Exception as error:
            print(f"nisy erreur {error}")

        print()
        data_operator.print_processors_stats()
    except (ValueError, TypeError) as er:
        print(er)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ==")
    main()
