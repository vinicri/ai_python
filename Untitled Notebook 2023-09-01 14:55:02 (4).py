# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 1

# COMMAND ----------

from random import choice
from string import ascii_lowercase, digits
text = ''.join(choice(ascii_lowercase + digits) for _ in range(9 * 1024 * 1024))
displayHTML(text)

# COMMAND ----------

text = ''.join(choice(ascii_lowercase + digits) for _ in range(9 * 1024 * 1024))
displayHTML(text)

# COMMAND ----------

text = ''.join(choice(ascii_lowercase + digits) for _ in range(9 * 1024 * 1024))
displayHTML(text)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 
