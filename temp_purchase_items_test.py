import sqlite3
import pandas as pd
from init_database import reset_database
from services.import_executor import execute_import

reset_database()
conn = sqlite3.connect('vyapaariq.db')
conn.row_factory = sqlite3.Row
user_id = 1
conn.execute('INSERT INTO users (id, name, email, password) VALUES (?, ?, ?, ?)', (user_id, 'User', 'u@test.com', 'pwd'))
supplier_id = conn.execute('INSERT INTO suppliers (user_id, name) VALUES (?, ?) RETURNING id', (user_id, 'Supply')).fetchone()[0]
product_id = conn.execute('INSERT INTO products (user_id, name, supplier_id) VALUES (?, ?, ?) RETURNING id', (user_id, 'Red Bull Can', supplier_id)).fetchone()[0]
purchase_id = conn.execute('INSERT INTO purchases (user_id, supplier_id, purchase_date, total_amount, status) VALUES (?, ?, DATE(\'2024-01-01\'), ?, ?) RETURNING id', (user_id, supplier_id, 100, 'Delivered')).fetchone()[0]
conn.commit()

df = pd.DataFrame({'purchase_id':[purchase_id],'product_name':['  Red Bull Can  '],'quantity':[2],'unit_cost':[10]})
result = execute_import(df, 'purchase_items', conn, user_id)
print('RESULT', result)
print('ROWS', conn.execute('SELECT purchase_id,product_id,quantity,unit_cost,user_id FROM purchase_items').fetchall())
conn.close()
