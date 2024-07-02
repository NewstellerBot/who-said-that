import json
import pandas as pd
import requests
import asyncio
import numpy as np

from tqdm import tqdm
from collections import Counter

with open("data.json", "r") as f:
    data = json.load(f)

df = pd.DataFrame(data)
df.set_index("id")

# some messages have a different type than text
# for example files, geo locations, videos, etc,
# we do not include them in the analysis;
# thus, we drop them here
df.dropna(inplace=True)

# Drop any links as we do not want the model
# to learn based on links, but rather actual
# messages
df = df[
    [("http" not in s) and ("tg://" not in s) and bool(s.strip()) for s in df["text"]]
]

# Drop messages with less than 5 words
# We need to give our model something to work with
df = df[[len(s.split()) > 1 for s in df["text"]]]

# Transforms the labels from telegram ids to simple
# numbers from 0 to 5
labels = df["from"]
lton = {l: i for i, l in enumerate(set(labels))}
df["from"] = df["from"].map(lton)

# limit the number per class to n
n = min(Counter(df["from"].values).values())

for cls in set(df["from"]):
    ids = df[df["from"] == cls].id.values
    n_to_drop = len(ids) - n
    if n_to_drop > 0:
        ids_to_drop = np.random.choice(ids, size=n_to_drop, replace=False)
        df = df[~df["id"].isin(ids_to_drop)]

cls_to_name = {0: "Krystian", 1: "Tim", 2: "Alina", 3: "Mashiko", 4: "Mira"}


async def insert_to_db(obj):
    requests.post("http://localhost:3000/api/create-text", json=obj)


async def main():
    id = 1
    for row in tqdm(df.iterrows()):
        _, (_, text, _from) = row
        obj = {"text": text, "from": cls_to_name.get(_from, "unknown"), "id": id}
        asyncio.create_task(insert_to_db(obj))
        id += 1


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
