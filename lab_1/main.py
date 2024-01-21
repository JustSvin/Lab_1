import sqlite3
import pandas as pd


con = sqlite3.connect("./DB.db")

cursor = con.cursor()

# Запросы к БД
print("Задание 1")
df1 = pd.read_sql('''
SELECT buy_step.buy_id, buy_step.step_id, city.name_city
FROM buy_step, city
WHERE buy_step.step_id = 3
ORDER BY name_city ASC, buy_id ASC;
''', con)
print(df1)
print()

print("Задание 2")
df1 = pd.read_sql('''
SELECT
    buy_step.buy_id AS 'Заказ',
    client.name_client AS 'Клиент',
    (buy_book.amount * book.price) AS 'Стоимость'
FROM buy_step, buy, buy_book, book
JOIN client ON client.client_id = buy.client_id
WHERE buy.client_id = buy_step.buy_id AND
      buy_book.book_id = book.book_id AND
      buy_book.buy_id = buy_step.buy_id AND
      ((buy_book.amount * book.price) > 2000 OR
        (buy_book.amount * book.price) < 500)
GROUP BY buy.buy_id
ORDER BY (buy_book.amount * book.price) DESC, client.name_client ASC;
''', con)
print(df1)
print()

print("Задание 3")
df1 = pd.read_sql('''
SELECT
    book.title AS 'Название',
    author.name_author AS 'Автор'
FROM book, buy_book, author
WHERE buy_book.book_id = book.book_id AND
      book.author_id = author.author_id
GROUP BY author.name_author
HAVING SUM(buy_book.amount) > (SELECT SUM(buy_book.amount)
                                FROM buy_book, book
                                WHERE buy_book.book_id = book.book_id
                                GROUP BY book.author_id);
''', con)
print(df1)
print()

print("Задание 4")
cursor.executescript('''
UPDATE book
SET price = ROUND(price) + 0.99
WHERE book_id IN (
    SELECT buy_book.book_id
    FROM buy_book
    GROUP BY buy_book.book_id
    HAVING SUM(buy_book.amount) > (
        SELECT AVG(amount)
        FROM buy_book
    )
);

UPDATE book
SET price = CEILING(price) + 0.99
WHERE book_id NOT IN (
    SELECT buy_book.book_id
    FROM buy_book
    GROUP BY buy_book.book_id
    HAVING SUM(buy_book.amount) > (
        SELECT AVG(amount)
        FROM buy_book
    )
);
''')
cursor.execute("SELECT * FROM book;")
res = cursor.fetchall()
print([description[0] for description in cursor.description])
for row in res:
    print(row)
print()

print("Задание 5")
cursor.execute('''
SELECT 
  author.name_author AS 'Автор', 
  book.title AS 'Книга', 
  book.price AS 'Стоимость', 
  SUM(book.price) OVER (PARTITION BY author.name_author ORDER BY book.price ASC) as 'Стоимость_с_накоплением'
FROM author, book
WHERE author.author_id = book.author_id
ORDER BY author.name_author DESC, book.price ASC;
''')
res = cursor.fetchall()
print([description[0] for description in cursor.description])
for row in res:
    print(row)
print()

con.close()

'''
SELECT
    author.name_author,
    book.title,
    (SELECT SUM(buy_book.amount) FROM buy_book, book WHERE buy_book.book_id = book.book_id) AS 'Сумма_с_накоплением',
    SUM('Сумма_с_накоплением') OVER (PARTITION BY author.name_author ORDER BY 'Сумма_с_накоплением') AS 'Стоимость'
FROM
    book, author
ORDER BY
    author.name_author DESC,
    book.title ASC;
'''