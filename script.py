import sqlite3

connection = sqlite3.connect("user_data.db")
cursor = connection.cursor()

data = "Expeditor_Data.txt"
output_file = "sorted_data.txt"
read_data = '''INSERT INTO users (first_name, last_name, street, city, state, age) VALUES (?, ?, ?, ?, ?, ?)'''


def iter_data():
    with open(data) as d:
        for row in d:
            # remove commas and periods, strip newline, strip " at start and end, split values on ""
            yield row.replace(",", "").replace(".", "").upper().rstrip().strip('"').split('""')

# Remove the previous table if it exists.
cursor.execute("DROP TABLE IF EXISTS users")

# create a table with the user's name, address, and age.
cursor.execute("CREATE TABLE users (first_name TEXT NOT NULL, last_name TEXT NOT NULL, street TEXT NOT NULL, city TEXT NOT NULL, state TEXT NOT NULL, age NUM NOT NULL)")

# Bulk add to the table from the provided text file.
with connection:
    connection.executemany(read_data, iter_data())

# removes extra space at the end of a street.
cursor.execute("UPDATE users SET street = trim(street)")

# group by Household (Address and count of occupants),
address = """SELECT street, city, state, COUNT(first_name)
FROM users
GROUP BY street, city, state"""
# # list occupants who are 19+ only: First, last, address, age.
# SELECT first_name, last_name, address -IF POSSIBLE- (street, city, state), age
# WHERE age >= 19
# # Sorted by last name, first name
# ORDER BY last_name, first_name;

with open(output_file, "w") as o:
    o.write(str(cursor.execute(address).fetchall()))

connection.close()
    #     o.write("\n".join(sorted(d.read().splitlines())))
