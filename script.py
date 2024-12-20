import sqlite3
from collections import defaultdict

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
household = """SELECT street || " " || city || " " || state, COUNT(first_name)
FROM users
GROUP BY street, city, state;"""

# # list occupants who are 19+ only: First, last, address, age.
# # Sorted by last name, first name
# def mem(id):
#     cursor.execute("""SELECT first_name, last_name, street || " " || city || " " || state, age
#     FROM users
#     WHERE age >= 19 AND street || " " || city || " " || state LIKE "{curr}"
#     ORDER BY last_name, first_name;""".format(curr = f"{id}"))#.fetchall()
#     # NEEDS WORK TO OPERATE CORRECTLY
# GROUP BY street, city, state

members = cursor.execute("""SELECT first_name, last_name, street || " " || city || " " || state, age
    FROM users
    WHERE age >= 19
    ORDER BY last_name, first_name;""").fetchall()

members_dict = defaultdict(list)

for i in members:
    members_dict[i[2]].append(i)


for id in cursor.execute(household):
    print(id, "\n", "\t", members_dict[id[0]])# POSSIBLE OPTION f"{[x for x in res if x[2] == id[0]]}" + "\n"), better would be to create an object from res and reference that

# print(cursor.execute(members).fetchall())

# with open(output_file, "w") as o:
#     o.write(str(cursor.execute(household).fetchall()).strip("[").strip("]"))

connection.close()
    #     o.write("\n".join(sorted(d.read().splitlines())))
