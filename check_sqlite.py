import sqlite3


db_path = 'db.sqlite'
conn = sqlite3.connect(db_path)
# conn.row_factory = sqlite3.Row
curs = conn.cursor()
curs.execute('SELECT COUNT(*) FROM film_work;')
data = curs.fetchone()
print(data[0])
conn.close()


