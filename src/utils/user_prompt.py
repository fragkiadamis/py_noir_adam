from src.utils.log_utils import get_logger

logger = get_logger()

def ask_yes_no(question: str) -> bool:
    while True:
        answer = input(f"{question} (y/n): ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        logger.info("Please answer 'y' or 'n'.")
