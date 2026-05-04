from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shutil

import pandas as pd


def build_output_file_name(base_name: str) -> str:
    current_date = datetime.now().strftime("%Y%m%d")
    return f"{base_name}_{current_date}.xlsx"


def validate_required_columns(df: pd.DataFrame, required_columns: set[str]) -> None:
    if not required_columns:
        return

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise KeyError(f"В исходной таблице отсутствуют обязательные колонки: {missing}")


def archive_existing_output(
    output_file: Path,
    archive_dir: Path,
    keep_last: int = 10,
) -> None:
    """
    Если output_file уже существует:
    1) переносит его в archive_dir с добавлением timestamp к имени
    2) оставляет в архиве только keep_last последних версий для этого output_file

    Пример:
        out/result_job3.xlsx
    ->  arh/result_job3_20260409_153045.xlsx
    """
    if not output_file.exists():
        return

    archive_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archived_name = f"{output_file.stem}_{timestamp}{output_file.suffix}"
    archived_path = archive_dir / archived_name

    shutil.move(str(output_file), str(archived_path))

    pattern = f"{output_file.stem}_*{output_file.suffix}"
    archived_files = sorted(
        archive_dir.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    for old_file in archived_files[keep_last:]:
        old_file.unlink(missing_ok=True)