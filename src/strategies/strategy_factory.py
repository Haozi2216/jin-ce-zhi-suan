# src/strategies/strategy_factory.py
from src.strategies.implemented_strategies import (
    Strategy01, Strategy02, Strategy03, Strategy04, Strategy05,
    Strategy06, Strategy07, Strategy08
)

def create_strategies():
    """
    Generate the active stock strategies.
    """
    strategies = [
        Strategy01(),
        Strategy02(),
        Strategy03(),
        Strategy04(),
        Strategy05(),
        Strategy06(),
        Strategy07(),
        Strategy08()
    ]
    return strategies
