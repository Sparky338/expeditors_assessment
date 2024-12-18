import sqlite3

connection = sqlite3.connect("user_data.db")
cursor = connection.cursor()

data = "Expeditor_Data.txt"
output_file = "sorted_data.txt"
query = '''INSERT INTO users (first_name, last_name, street, city, state, age) VALUES (?, ?, ?, ?, ?, ?)'''


def iter_data():
    with open(data) as d:
        for row in d:
            # remove commas, strip newline, strip " at start and end, split values on ""
            yield row.replace(",", "").rstrip().strip('"').split('""')


cursor.execute("DROP TABLE IF EXISTS users")
cursor.execute("CREATE TABLE users (first_name TEXT NOT NULL, last_name TEXT NOT NULL, street TEXT NOT NULL, city TEXT NOT NULL, state TEXT NOT NULL, age NUM NOT NULL)")
with connection:
    connection.executemany(query, iter_data())

# group by Household (Address and count of occupants),
# list occupants who are 19+ only: First, last, address, age.
# Sorted by last name, first name

with open(output_file, "w") as o:
    o.write(str(cursor.execute("SELECT * FROM users").fetchall()))

connection.close()
    #     o.write("\n".join(sorted(d.read().splitlines())))
