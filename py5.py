import pandas as pd

# Create inline data
data = {'Name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Emily'],
        'Age': [25, 32, 18, 47, 29],
        'Salary': [50000, 70000, 3000, 90000, 60000]}

# Load inline data into a Pandas DataFrame
df = pd.DataFrame(data)

# Calculate average salary
avg_salary = df['Salary'].mean()

# Print average salary
print(f"Average salary: ${avg_salary:.2f}")