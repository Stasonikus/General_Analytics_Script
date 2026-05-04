import importlib
import sys

from src.core.config import DEFAULT_JOB, JOB_MODULES
from src.core.logger import get_logger  # ✔ ИСПРАВЛЕНО


print("RUNNING MAIN FROM:", __file__)
logger = get_logger(__name__)


def print_available_jobs() -> None:
    """
    Печатает список доступных обработок.
    """
    logger.info("Доступные обработки:")

    for job_name, job_info in JOB_MODULES.items():
        description = job_info.get("description", "")
        logger.info(f" - {job_name}")
        if description:
            logger.info(f"   {description}")

    logger.info("Запуск:")
    logger.info("python -m src.main <job_name>")


def load_job_module(job_name: str):
    """
    Загружает модуль обработки по имени из реестра JOB_MODULES.
    """
    job_info = JOB_MODULES.get(job_name)

    if not job_info:
        available = ", ".join(sorted(JOB_MODULES))
        raise ValueError(
            f"Неизвестная обработка: '{job_name}'. "
            f"Доступные обработки: {available}"
        )

    module_path = job_info["module"]
    return importlib.import_module(module_path)


def main():
    """
    Варианты запуска:

    1) Показать список доступных job:
        python -m src.main

    2) Запустить job по имени:
        python -m src.main job_example
    """
    if len(sys.argv) == 1:
        logger.info("Запуск без указания job: показ списка доступных обработок")
        print_available_jobs()
        return

    job_name = sys.argv[1].strip()

    if job_name in {"-h", "--help", "help", "list", "--list"}:
        print_available_jobs()
        return

    try:
        logger.info(f"Запуск обработки: {job_name}")

        job_module = load_job_module(job_name)

        if not hasattr(job_module, "run"):
            raise AttributeError(
                f"В модуле '{job_name}' отсутствует функция run()"
            )

        job_module.run()

        logger.info(f"Обработка успешно завершена: {job_name}")

    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    except AttributeError as e:
        logger.error(str(e))
        sys.exit(1)

    except Exception:
        logger.exception("Непредвиденная ошибка при выполнении программы")
        sys.exit(1)


if __name__ == "__main__":
    main()