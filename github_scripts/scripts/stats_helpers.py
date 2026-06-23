#!/usr/bin/env python3
"""
Helper module for statistical calculations.
This file must be transferred along with the main script to test
the htcondor_transfer_input_files feature.
"""


def compute_statistics(numbers):
    """
    Compute basic statistics for a list of numbers.

    Args:
        numbers: List of numeric values

    Returns:
        Dictionary with count, sum, and average
    """
    count = len(numbers)
    total = sum(numbers)
    avg = total / count if count > 0 else 0

    return {
        "count": count,
        "sum": total,
        "average": avg,
        "min": min(numbers) if numbers else None,
        "max": max(numbers) if numbers else None,
    }


def format_statistics(stats, sample_name):
    """
    Format statistics as a string for output.

    Args:
        stats: Dictionary of statistics
        sample_name: Name of the sample

    Returns:
        Formatted string
    """
    lines = [
        f"Sample: {sample_name}",
        f"Numbers processed: {stats['count']}",
        f"Sum: {stats['sum']}",
        f"Average: {stats['average']:.2f}",
        f"Min: {stats['min']}",
        f"Max: {stats['max']}",
    ]
    return "\n".join(lines)
