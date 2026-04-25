import pandas as pd
import os

# loading bronze data
transactions = pd.read_csv("data/bronze/transactions.csv")
users = pd.read_csv("data/bronze/users.csv")
print("Data loaded")

#cleaning to silver)
transactions["city"] = transactions["city"].str.title()
users["city"] = users["city"].str.title()
transactions = transactions[transactions["amount"] >= 0]
print("cleaned data") 

# gold layer
gold = transactions.groupby("date").agg(total_amount=("amount", "sum"), transaction_count=("id","count")).reset_index()

#saving
os.makedirs("data/silver", exist_ok=True)
os.makedirs("data/gold", exist_ok=True)

transactions.to_csv("data/silver/transactions_clean.csv", index=False)
gold.to_csv("data/gold/summary.csv", index=False)

print("Pipeline finished")