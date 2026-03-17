# src/strategies/strategy_factory.py
from src.strategies.implemented_strategies import (
    Strategy00, Strategy01, Strategy02, Strategy03, Strategy04, Strategy05,
    Strategy06, Strategy07, Strategy08, Strategy09
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
    return strategies
