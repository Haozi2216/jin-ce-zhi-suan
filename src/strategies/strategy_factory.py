# src/strategies/strategy_factory.py
from src.strategies.implemented_strategies import (
    Strategy00, Strategy01, Strategy02, Strategy03, Strategy04, Strategy05,
    Strategy06, Strategy07, Strategy08, Strategy09
)
from src.strategies.strategy_manager_repo import (
    load_custom_strategies,
    load_disabled_ids,
    load_deleted_ids,
    instantiate_custom_strategy
)

def create_strategies():
    """
    Generate the active stock strategies.
    """
    strategies = [
        Strategy00(),
        Strategy01(),
        Strategy02(),
        Strategy03(),
        Strategy04(),
        Strategy05(),
        Strategy06(),
        Strategy07(),
        Strategy08(),
        Strategy09()
    ]
    disabled_ids = load_disabled_ids()
    deleted_ids = load_deleted_ids()
    if deleted_ids:
        strategies = [s for s in strategies if str(s.id) not in deleted_ids]
    custom_rows = load_custom_strategies()
    for row in custom_rows:
        sid = str(row.get("id", "")).strip()
        if sid and sid in deleted_ids:
            continue
        if sid and sid in disabled_ids:
            continue
        try:
            strategy = instantiate_custom_strategy(row)
            if strategy is not None:
                strategies.append(strategy)
        except Exception:
            continue
    if disabled_ids:
        strategies = [s for s in strategies if str(s.id) not in disabled_ids]
    return strategies
