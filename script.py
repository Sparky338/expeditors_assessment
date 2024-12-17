import sqlite3

connection = sqlite3.connect("user_data.db")
cursor = connection.cursor()

data = "Expeditor_Data.txt"
output_file = "sorted_data.txt"



with open(data) as d:
    # print(d.read())

    # group by Household (Address and count of occupants),
    # list occupants who are 19+ only: First, last, address, age.
    # Sorted by last name, first name

    cursor.execute("DROP TABLE IF EXISTS users")
    cursor.execute("CREATE TABLE users (first_name TEXT NOT NULL, last_name TEXT NOT NULL, street TEXT NOT NULL, city TEXT NOT NULL, state TEXT NOT NULL, age NUM NOT NULL)")
    cursor.execute("INSERT INTO users (first_name, last_name, street, city, state, age) VALUES ('Dave', 'some', '123 main', 'seattle', 'wa', '42')")
    # cursor.execute("INSERT users FROM `d` WITH(FIRSTROW = 1, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');")

    # with open(output_file, "w") as o:
    #     o.write("\n".join(sorted(d.read().splitlines())))
