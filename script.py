#!/usr/bin/python

import sqlite3
from collections import defaultdict

connection = sqlite3.connect("user_data.db")
cursor = connection.cursor()

data = "Expeditor_Data.txt"
output_file = "sorted_data.txt"
read_data = '''INSERT INTO users (first_name, last_name, street, city, state, age) VALUES (?, ?, ?, ?, ?, ?)'''


def iter_data() -> any:
    with open(data) as d:
        for row in d:
            # Remove commas and periods, strip newline, strip " at start and end, split values on "".
            yield row.replace(",", "").replace(".", "").title().rstrip().strip('"').split('""')

# Remove the previous table if it exists.
cursor.execute("DROP TABLE IF EXISTS users")

# Create a table with the user's name, address, and age.
cursor.execute("CREATE TABLE users (first_name TEXT NOT NULL, last_name TEXT NOT NULL, street TEXT NOT NULL, city TEXT NOT NULL, state TEXT NOT NULL, age NUM NOT NULL)")

# Bulk add to the table from the provided text file.
with connection:
    connection.executemany(read_data, iter_data())

# Removes extra space at the end of a street.
cursor.execute("UPDATE users SET street = trim(street)")
# Capitalizes the state abbreviation.
cursor.execute("UPDATE users SET state = upper(state)")

# Group by Household (Address and count of occupants).
household = """SELECT street || " " || city || " " || state, COUNT(first_name)
FROM users
GROUP BY street, city, state;"""

# List occupants who are 19+ only: First, Last, address, age.
# Sorted by last name, first name.
members = cursor.execute("""SELECT first_name, last_name, street || " " || city || " " || state, age
FROM users
WHERE age >= 19
ORDER BY last_name, first_name;""").fetchall()

members_dict = defaultdict(list)

# Add household members to a list under the key of their address.
for i in members:
    members_dict[i[2]].append(i)

# Return users on an indented new line.
def household_members(id:str) -> str:
    return "\n\t".join(str(list(i)) for i in members_dict[id])

# Create a new document and write in the grouped, sorted, and ordered data.
with open(output_file, "w") as o:
    for id in cursor.execute(household):
        o.write("".join((id[0], ", ", str(id[1]), " Household Occupants", "\n\t", household_members(id[0]).replace("[","").replace("]","") or "'No one 19 or older lives in this Household.'", "\n")))

connection.close()
