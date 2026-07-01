# Exercise: RDD Actions

## Overview
Practice using RDD actions to retrieve and aggregate data. Understand when actions trigger computation.

**Duration:** 30-45 minutes  
**Mode:** Individual

---

## Core Tasks

### Task 1: Basic Retrieval Actions

Create `rdd_actions.py`:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDActions")

numbers = sc.parallelize([10, 5, 8, 3, 15, 12, 7, 20, 1, 9])

# Task A: collect() - Get all elements
all_nums = # YOUR CODE
print(f"All numbers: {all_nums}")

# Task B: count() - Count elements
count = # YOUR CODE
print(f"Count: {count}")

# Task C: first() - Get first element
first = # YOUR CODE
print(f"First: {first}")

# Task D: take(n) - Get first n elements
first_three = # YOUR CODE
print(f"First 3: {first_three}")

# Task E: top(n) - Get largest n elements
top_three = # YOUR CODE
print(f"Top 3: {top_three}")

# Task F: takeOrdered(n) - Get smallest n elements
smallest_three = # YOUR CODE
print(f"Smallest 3: {smallest_three}")

sc.stop()
```

### Task 2: Aggregation Actions

Add to your script:

```python
# Task A: reduce() - Sum all numbers
total = # YOUR CODE using reduce with lambda
print(f"Sum: {total}")

# Task B: reduce() - Find maximum
maximum = # YOUR CODE using reduce
print(f"Max: {maximum}")

# Task C: reduce() - Find minimum
minimum = # YOUR CODE using reduce
print(f"Min: {minimum}")

# Task D: fold() - Sum with zero value
folded_sum = # YOUR CODE
print(f"Folded sum: {folded_sum}")
```

### Task 3: countByValue()

```python
# Given: colors with duplicates
colors = sc.parallelize(["red", "blue", "red", "green", "blue", "red", "yellow"])

# Count occurrences of each color
color_counts = # YOUR CODE
print(f"Color counts: {dict(color_counts)}")

# Expected: {'red': 3, 'blue': 2, 'green': 1, 'yellow': 1}
```

---


## Deliverables

1. `rdd_actions.py` - Complete script
2. Console output showing all results

---

## Definition of Done

- [ ] All retrieval actions (collect, count, first, take, top, takeOrdered) work
- [ ] Aggregation actions (reduce, fold) calculate correct values
- [ ] countByValue correctly counts occurrences
- [ ] Script runs without errors

---

## Additional Resources
- Written Content: `actions.md`
