from typing import cast, Union, Optional, Iterable, Dict, List, Any
from pathlib import Path
import shutil
import os
import os.path
import logging
import json
import csv
from collections import abc
from itertools import chain

from tqdm import tqdm as _tqdm
import toml
import joblib

PathType = Union[str, os.PathLike]


def folder(
    raw_path: PathType,
    expandvars: bool = False,
    exists: bool = False,
    reset: bool = False,
    touch: bool = False,
):
    if expandvars:
        raw_path = os.path.expandvars(raw_path)

    path = Path(raw_path)

    if exists:
        if not path.exists():
            raise FileNotFoundError(f'{raw_path} not found.')
        if not path.is_dir():
            raise NotADirectoryError(f'{raw_path} should be a folder.')

    if reset:
        if path.exists():
            # Remove children instead.
            for child in path.iterdir():
                if child.is_dir():
                    try:
                        shutil.rmtree(child)
                    except OSError:
                        logging.warning(f'Cannot remove folder {child}.')
                else:
                    child.unlink()
        else:
            os.makedirs(path, exist_ok=True)

    if touch:
        os.makedirs(path, exist_ok=True)

    return path


def file(
    raw_path: PathType,
    expandvars: bool = False,
    exists: bool = False,
):
    if expandvars:
        raw_path = os.path.expandvars(raw_path)

    path = Path(raw_path)

    if exists:
        if not path.exists():
            raise FileNotFoundError(f'{raw_path} not found.')
        if not path.is_file():
            raise IsADirectoryError(f'{raw_path} should be a file.')

    return path


def read_text_lines(
    raw_path: PathType,
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    strip: bool = False,
    skip_empty: bool = False,
    tqdm: bool = False,
):
    path = file(raw_path, expandvars=expandvars, exists=True)

    with path.open(
        mode='r',
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fin:
        if tqdm:
            fin = _tqdm(fin)

        for text in fin:
            text = cast(str, text)
            if strip:
                text = text.strip()
            if not skip_empty or text:
                yield text


def write_text_lines(
    raw_path: PathType,
    texts: Iterable[str],
    buffering: int = -1,
    expandvars: bool = False,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    strip: bool = False,
    skip_empty: bool = False,
    tqdm: bool = False,
):
    path = file(raw_path, expandvars=expandvars)

    with path.open(
        mode='w',
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fout:
        if tqdm:
            fout = _tqdm(fout)

        for text in texts:
            if strip:
                text = text.strip()
            if skip_empty and not text:
                continue
            fout.write(text)
            fout.write('\n')


def read_json_lines(
    raw_path: PathType,
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    skip_empty: bool = True,
    ignore_error: bool = False,
    silent: bool = False,
    tqdm: bool = False,
):
    for num, text in enumerate(
        read_text_lines(
            raw_path,
            expandvars=expandvars,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            strip=False,
            tqdm=tqdm,
        )
    ):
        try:
            struct: Union[Dict, List] = json.loads(text)
            if skip_empty and not struct:
                continue
            yield struct

        except json.JSONDecodeError:
            if not ignore_error:
                raise
            if not silent:
                logging.warning(f'Cannot parse #{num}: "{text}"')


def _encode_json_lines(
    structs: Iterable[Union[Dict, List]],
    skip_empty: bool,
    ensure_ascii: bool,
    silent: bool,
    ignore_error: bool,
):
    for num, struct in enumerate(structs):
        try:
            if skip_empty and not struct:
                continue
            text = json.dumps(struct, ensure_ascii=ensure_ascii)
            yield text

        except (TypeError, OverflowError, ValueError):
            if not ignore_error:
                raise
            if not silent:
                logging.warning(f'Cannot encode #{num}: "{struct}"')


def write_json_lines(
    raw_path: PathType,
    structs: Iterable[Union[Dict, List]],
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    skip_empty: bool = False,
    ensure_ascii: bool = True,
    ignore_error: bool = False,
    silent: bool = False,
    tqdm: bool = False,
):
    write_text_lines(
        raw_path,
        _encode_json_lines(
            structs,
            skip_empty=skip_empty,
            ensure_ascii=ensure_ascii,
            silent=silent,
            ignore_error=ignore_error,
        ),
        expandvars=expandvars,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
        tqdm=tqdm,
    )


def read_csv_lines(
    raw_path: PathType,
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    header_exists: bool = True,
    skip_header: bool = False,
    match_header: bool = True,
    to_dict: bool = False,
    ignore_error: bool = False,
    silent: bool = False,
    tqdm: bool = False,
    dialect: str = 'excel',
    **fmtparams: Dict[str, Any],
):
    path = file(raw_path, expandvars=expandvars, exists=True)

    if not header_exists and match_header:
        msg = 'Cannot match header if header does not exists.'
        if not ignore_error:
            raise RuntimeError(msg)
        elif not silent:
            logging.warning(msg)
        return

    if to_dict and not match_header:
        msg = 'Must match header before converting to dict.'
        if not ignore_error:
            raise RuntimeError(msg)
        elif not silent:
            logging.warning(msg)
        return

    with path.open(
        mode='r',
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fin:
        if tqdm:
            fin = _tqdm(fin)

        header = None
        for num, struct in enumerate(csv.reader(fin, dialect, **fmtparams)):
            if header_exists and num == 0:
                if not isinstance(struct, abc.Iterable):  # type: ignore
                    msg = 'Header not iterable.'
                    if not ignore_error:
                        raise TypeError(msg)
                    elif not silent:
                        logging.warning(msg)
                    return

                header = list(struct)
                if skip_header:
                    continue

            if match_header:
                # Make linter happy.
                assert isinstance(header, list)
                if len(header) != len(struct):
                    msg = f'Cannot match #{num} = {struct} with header header = {header}.'
                    if not ignore_error:
                        raise ValueError(msg)
                    elif not silent:
                        logging.warning(msg)
                    # Skip this line.
                    continue

                if to_dict:
                    struct = dict(zip(header, struct))

            yield struct


def write_csv_lines(
    raw_path: PathType,
    structs: Iterable[Dict],
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    ignore_error: bool = False,
    silent: bool = False,
    from_dict: bool = False,
    set_missing_key_to_none: bool = False,
    ignore_unknown_key: bool = True,
    tqdm: bool = False,
    dialect: str = 'excel',
    **fmtparams: Dict[str, Any],
):
    path = file(raw_path, expandvars=expandvars)

    with path.open(
        mode='w',
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fout:
        if tqdm:
            fout = _tqdm(fout)

        try:
            iter_structs = iter(structs)
        except TypeError:
            if not ignore_error:
                raise
            elif not silent:
                logging.warning('structs is not iterable.')
            return

        csv_writer = csv.writer(fout, dialect, **fmtparams)

        from_dict_keys = None
        if from_dict:
            try:
                first_struct = next(iter_structs)
            except StopIteration:
                msg = 'empty structs.'
                if not ignore_error:
                    raise ValueError(msg)
                elif not silent:
                    logging.warning(msg)
                return

            if not isinstance(first_struct, abc.Mapping):  # type: ignore
                msg = f'structs[0]={first_struct} should be a mapping.'
                if not ignore_error:
                    raise TypeError(msg)
                elif not silent:
                    logging.warning(msg)
                return

            from_dict_keys = list(first_struct)
            csv_writer.writerow(from_dict_keys)

            # "Put back" the first struct.
            iter_structs = chain((first_struct,), iter_structs)

        for num, struct in enumerate(iter_structs):
            if from_dict:
                if not isinstance(struct, abc.Mapping):  # type: ignore
                    msg = f'#{num} {struct} should be a mapping.'
                    if not ignore_error:
                        raise TypeError(msg)
                    elif not silent:
                        logging.warning(msg)
                    # Skip this line.
                    continue

                # Make linter happy.
                assert isinstance(from_dict_keys, list)
                items = []
                skip_this_struct = False
                for key in from_dict_keys:
                    if key not in struct and not set_missing_key_to_none:
                        msg = f'#{num} key "{key}" not found.'
                        if not ignore_error:
                            raise KeyError(msg)
                        if not silent:
                            logging.warning(msg + ' Skip.')
                        # Abort.
                        skip_this_struct = True
                        break

                    items.append(struct.get(key))

                if skip_this_struct:
                    continue
                if not ignore_unknown_key:
                    unknown_keys = set(struct) - set(from_dict_keys)
                    if unknown_keys:
                        msg = f'#{num} contains unknown_keys {unknown_keys}'
                        if not ignore_error:
                            raise KeyError(msg)
                        if not silent:
                            logging.warning(msg + ' Skip.')
                        continue

                csv_writer.writerow(items)

            else:
                if not isinstance(struct, abc.Iterable):  # type: ignore
                    msg = f'#{num} {struct} not iterable.'
                    if not ignore_error:
                        raise ValueError(msg)
                    if not silent:
                        logging.warning(msg + ' Skip.')
                    continue

                csv_writer.writerow(struct)


def read_json(
    raw_path: PathType,
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    ignore_error: bool = False,
    silent: bool = False,
):
    path = file(raw_path, expandvars=expandvars, exists=True)

    with path.open(
        mode='r',
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fin:
        try:
            struct: Union[Dict, List] = json.load(fin)
            return struct
        except json.JSONDecodeError:
            if not ignore_error:
                raise
            if not silent:
                logging.warning(f'Cannot load {path}')
            return {}


def write_json(
    raw_path: PathType,
    struct: Union[Dict, List],
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    ensure_ascii: bool = True,
    indent: Optional[int] = None,
    ignore_error: bool = False,
    silent: bool = False,
):
    path = file(raw_path, expandvars=expandvars)

    with path.open(
        mode='w',
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fout:
        try:
            json.dump(struct, fout, ensure_ascii=ensure_ascii, indent=indent)
        except (TypeError, OverflowError, ValueError):
            if not ignore_error:
                raise
            if not silent:
                logging.warning(f'Cannot encode "{struct}"')


def read_toml(
    raw_path: PathType,
    expandvars: bool = False,
):
    path = file(raw_path, expandvars=expandvars, exists=True)
    return toml.load(path)


def write_toml(
    raw_path: PathType,
    struct: Dict,
    expandvars: bool = False,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
):
    path = file(raw_path, expandvars=expandvars)

    with path.open(
        mode='w',
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fout:
        toml.dump(struct, fout)


def read_joblib(
    raw_path: PathType,
    expandvars: bool = False,
    mmap_mode: Optional[str] = None,
):
    path = file(raw_path, expandvars=expandvars, exists=True)
    return joblib.load(path, mmap_mode=mmap_mode)


def write_joblib(
    raw_path: PathType,
    struct: Any,
    expandvars: bool = False,
    compress: int = 0,
    protocol: Optional[int] = None,
    cache_size: Optional[int] = None,
):
    path = file(raw_path, expandvars=expandvars)

    joblib.dump(
        struct,
        path,
        compress=compress,
        protocol=protocol,
        cache_size=cache_size,
    )
