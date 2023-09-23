#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
import threading
import queue
import datetime
# In[ ]:

# SQLite 连接池类
class SQLiteConnectionPool:
    def __init__(self, database_path,max_connections=5):
        self.max_connections = max_connections
        self.database_path = database_path
        self.connections = queue.Queue(maxsize=max_connections)
        self.lock = threading.Lock()

        # 创建初始连接
        for _ in range(max_connections):
            self.connections.put(self.create_connection())

    def create_connection(self):
        return sqlite3.connect(self.database_path)

    def get_connection(self):
        return self.connections.get()

    def release_connection(self, conn):
        self.connections.put(conn)

    def set_database_path(self, new_database_path):
        self.database_path = new_database_path




def create_new_db(my_new_database):
    # 指定新数据库文件的名称
    database_name = my_new_database+'.db'
    # 连接到数据库文件，如果文件不存在，会创建新的数据库文件
    conn = sqlite3.connect(database_name)


def show_all_tables(database_name):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    # 查询数据库中的所有表单名称
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = c.fetchall()

    if tables:
        print("All tables in the database:")
        for table in tables:
            print(table[0])
    else:
        print("The database does not contain any tables.")

    # 关闭连接
    conn.close()

# 调用函数并传入数据库文件名
#database_name = "your_database.db"  # 替换为你的数据库文件名
#show_all_tables(database_name)

# 定义一个函数来显示某个表格中的所有数据
def show_all_data(database_name, table_name):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    try:
        # 执行 SELECT 查询以获取表格中的所有数据
        c.execute(f'''
            SELECT ROWID,* FROM {table_name};
        ''')

        # 获取查询结果
        rows = c.fetchall()

        if rows:
            # 打印列名
            column_names = [description[0] for description in c.description]
            print("Column Names:", column_names)

            # 打印数据
            for row in rows:
                print(row)
        else:
            print(f"No data found in table '{table_name}'.")

    except sqlite3.Error as e:
        print(f"Error retrieving data: {e}")
    finally:
        conn.close()

# 调用函数并传入数据库文件名和表格名
# database_name = "your_database.db"
# table_name = "your_table"
# show_all_data(database_name, table_name)


# In[3]:


# 定义一个函数来创建数据库表(非.db文件)
def create_table(database_name, table_name):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    # 创建表并定义初始列
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Name TEXT
        );
    ''')

    # 提交更改并关闭连接
    conn.commit()
    conn.close()


# In[ ]:


# 定义一个函数来添加新列
def add_column(database_name, table_name, column_name, column_type):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    # 使用ALTER TABLE语句添加新列
    c.execute(f'''
        ALTER TABLE {table_name}
        ADD COLUMN {column_name} {column_type};
    ''')

    # 提交更改并关闭连接
    conn.commit()
    conn.close()


# In[ ]:


# 定义一个函数来插入数据
def insert_data(database_name, table_name, column_names, data):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    
    # 检查数据与列名的数量是否匹配
    if len(data) != len(column_names):
        print("Error: The number of data values does not match the number of columns.")
        conn.close()
        return

    # 构建插入数据的SQL语句
    insert_sql = f'''
        INSERT INTO {table_name} ({','.join(column_names)})
        VALUES ({','.join(['?'] * len(column_names))});
    '''

    # 插入数据
    c.execute(insert_sql, data)

    # 提交更改并关闭连接
    conn.commit()
    conn.close()
# 调用函数并传入数据库文件名、表名和数据
#database_name = "my_database.db"
#table_name = "my_table"
#data = [
#    ("Alice", 25, "alice@example.com"),
#    ("Bob", 30, "bob@example.com"),
#    ("Carol", 28, "carol@example.com", "extra_value")  # 包含额外的值，但会被跳过
#]
#column_names = ["Name", "Age", "Email"]  # 替换为实际的列名
#insert_data(database_name, table_name, data)


# In[ ]:


# 定义一个函数来删除数据，根据rowid或name
def delete_row_by_id(database_name, table_name, identifier):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    # 构建DELETE语句，根据传入的参数选择删除条件
    delete_sql = f'''
        DELETE FROM {table_name}
        WHERE rowid = ? ;
    '''

    # 执行DELETE操作
    c.execute(delete_sql, identifier)

    # 提交更改并关闭连接
    conn.commit()
    conn.close()

# 调用函数并传入数据库文件名、表名以及要删除的rowid或name
#database_name = "my_database.db"
#table_name = "my_table"
#identifier = 2  # 替换为要删除的rowid或name
#delete_row_by_id_or_name(database_name, table_name, identifier)


# In[ ]:


# 定义一个函数来替换指定列的数据，根据rowid或name
def replace_data_by_id(database_name, table_name, identifier, column_name, new_value):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()
    
    if isinstance(column_name, (list, tuple)) :
        # 构建 SET 子句，将每个列名和占位符组合成 SET 子句的一部分
        set_clause = ', '.join([f'{col} = ?' for col in column_name])

        update_sql = f'''
            UPDATE {table_name}
            SET {set_clause}
            WHERE rowid = ? ;
        '''
        # 构建参数元组，包括新值和标识符
        params = list(new_value) + [identifier]

        # 执行UPDATE操作，传递参数元组
        c.execute(update_sql, params)
    else :
        # 使用UPDATE语句根据传入的参数选择更新条件和新值
        update_sql = f'''
            UPDATE {table_name}
            SET {column_name} = ?
            WHERE rowid = ?;
        '''
        # 执行UPDATE操作
        c.execute(update_sql, (new_value,  identifier))


    # 提交更改并关闭连接
    conn.commit()
    conn.close()

# 调用函数并传入数据库文件名、表名、rowid或name、要更新的列名和新的值
#database_name = "my_database.db"
#table_name = "my_table"
#identifier = "陈氏"  # 替换为rowid或name
#column_name = "Age"  # 替换为要更新的列名
#new_value = 14  # 新的值
#replace_data_by_id_or_name(database_name, table_name, identifier, column_name, new_value)


# In[ ]:


# 定义一个函数来查阅某一行的所有数据，根据rowid或name
def get_data_by_id_or_name(database_name, table_name, identifier):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    # 使用SELECT语句根据传入的参数选择查阅条件
    select_sql = f'''
        SELECT * FROM {table_name}
        WHERE rowid = ? OR Name = ?;
    '''
    
    # 执行SELECT操作
    c.execute(select_sql, (identifier, identifier))
    row = c.fetchone()
    
     # 关闭连接
    conn.close()
    
    if row:
        return row
    else:
        print("No data found for the specified identifier.")
        return None

# 调用函数并传入数据库文件名、表名以及rowid或name
#database_name = "my_database.db"
#table_name = "my_table"
#identifier = "陈氏"  # 替换为rowid或name


# In[ ]:


# 定义一个函数来删除单个表格
def drop_table(database_name, table_name):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    # 使用DROP TABLE语句删除指定表格
    drop_sql = f'''
        DROP TABLE IF EXISTS {table_name};
    '''

    # 执行DROP TABLE操作
    c.execute(drop_sql)

    # 提交更改并关闭连接
    conn.commit()
    conn.close()

# 调用函数并传入数据库文件名和要删除的表格名
#database_name = "my_database.db"
#table_name = "my_table_to_delete"
#drop_table(database_name, table_name)

# 一次性删除所有表格
def clear_all_tables(database_path):
    try:
        # 从连接池获取连接
        conn = pool.get_connection()
        cursor = conn.cursor()

        # 获取所有表格名称
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # 清空所有表格
        for table in tables:
            cursor.execute(f"DELETE FROM {table[0]};")

        # 提交更改
        conn.commit()
        cursor.close()

        # 释放连接回连接池
        pool.release_connection(conn)

        print("所有表格已清空成功")

    except sqlite3.Error as e:
        print(f"清空表格失败: {e}")

# 调用方法清空所有表格
#clear_all_tables('example.db')



def rename_column(database_path, table_name, old_column_name, new_column_name):
    # 初始化连接池
    pool = SQLiteConnectionPool(database_path,max_connections=5)
    try:
        # 修改连接池的数据库路径
        pool.set_database_path(database_path)
        # 从连接池获取连接
        conn = pool.get_connection()
        cursor = conn.cursor()

        # 禁用外键约束
        cursor.execute('PRAGMA foreign_keys=off;')
        conn.commit()

        # 获取旧表格的列信息
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # 构建新表格的列信息，修改需要修改的列名
        new_columns = []
        for col_info in columns:
            col_name = col_info[1]
            if col_name == old_column_name:
                col_name = new_column_name
            new_columns.append(f"{col_name} {col_info[2]}")

        # 使用新列信息创建新表格
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name}_temp ({', '.join(new_columns)});"
        cursor.execute(create_table_sql)

        # 复制数据
        cursor.execute(f'INSERT INTO {table_name}_temp SELECT * FROM {table_name};')

        # 删除旧表格
        cursor.execute(f'DROP TABLE IF EXISTS {table_name};')

        # 重命名临时表格为原表格
        cursor.execute(f'ALTER TABLE {table_name}_temp RENAME TO {table_name};')

        # 启用外键约束
        cursor.execute('PRAGMA foreign_keys=on;')

        # 提交更改
        conn.commit()
        cursor.close()

        # 释放连接回连接池
        pool.release_connection(conn)

        print(f"列名'{old_column_name}'已成功修改为'{new_column_name}'.")

    except sqlite3.Error as e:
        print(f"列名修改失败: {e}")


# 调用函数并传入数据库文件名、表名、旧列名、新列名和新列的数据类型
# database_name = "your_database.db"
# table_name = "your_table"
# old_column_name = "old_column"
# new_column_name = "new_column"
# new_column_type = "TEXT"  # 替换为新列的数据类型
# rename_column(database_name, table_name, old_column_name, new_column_name, new_column_type)

def modify_column_property(database_path, table_name, column_name, new_data_type):
    # 初始化连接池
    pool = SQLiteConnectionPool(database_path,max_connections=5)
    try:
        # 修改连接池的数据库路径
        pool.set_database_path(database_path)
        # 从连接池获取连接
        conn = pool.get_connection()
        cursor = conn.cursor()

        # 禁用外键约束
        cursor.execute('PRAGMA foreign_keys=off;')
        conn.commit()

        # 获取旧表格的列信息
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # 构建新表格的列信息，根据需要修改目标列的属性
        new_columns = []
        for col_info in columns:
            col_name = col_info[1]
            col_type = col_info[2]
            if col_name == column_name:
                col_type = new_data_type
            new_columns.append(f"{col_name} {col_type}")

        # 使用新列信息创建新表格
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name}_temp ({', '.join(new_columns)});"
        cursor.execute(create_table_sql)

        # 复制数据
        cursor.execute(f'INSERT INTO {table_name}_temp SELECT * FROM {table_name};')

        # 删除旧表格
        cursor.execute(f'DROP TABLE IF EXISTS {table_name};')

        # 重命名临时表格为原表格
        cursor.execute(f'ALTER TABLE {table_name}_temp RENAME TO {table_name};')

        # 启用外键约束
        cursor.execute('PRAGMA foreign_keys=on;')

        # 提交更改
        conn.commit()
        cursor.close()

        # 释放连接回连接池
        pool.release_connection(conn)

        print(f"列'{column_name}'的属性已成功修改为'{new_data_type}'.")

    except sqlite3.Error as e:
        print(f"列属性修改失败: {e}")

def get_column_info(database_path, table_name, column_name):
    try:
        # 连接到SQLite数据库
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        # 使用PRAGMA table_info查询列信息
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()

        # 查找目标列的信息
        target_column_info = None
        for column_info in columns_info:
            if column_info[1] == column_name:
                target_column_info = column_info
                break

        if target_column_info:
            print(f"列名: {target_column_info[1]}")
            print(f"数据类型: {target_column_info[2]}")
            print(f"是否为主键: {'Yes' if target_column_info[5] == 1 else 'No'}")
        else:
            print(f"列名 '{column_name}' 不存在于表 '{table_name}' 中。")

        # 关闭数据库连接
        conn.close()

    except sqlite3.Error as e:
        print(f"获取列信息失败: {e}")


#打印列名
def column_names(database_path, table_name):
    try:
        # 连接到数据库
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        # 查询表结构信息
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()

        # 提取列名
        column_names = [col[1] for col in columns]

        # 打印列名
        print(f"列名: {column_names}")

        # 关闭数据库连接
        conn.close()

    except sqlite3.Error as e:
        print(f"发生错误: {e}")


# 获取时间

def get_datetime():
    return datetime.datetime.now()

# 在两个存在的列中添加新列
def add_column_between_columns(database_path, table_name, new_column_name, column1_name, column2_name):
    try:
        # 连接到SQLite数据库
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        # 获取表的列名列表和原始列的位置
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]

        if column1_name not in columns or column2_name not in columns:
            print(f"列 '{column1_name}' 或 '{column2_name}' 不存在于表中，无法插入新列。")
            return

        index1 = columns.index(column1_name)
        index2 = columns.index(column2_name)

        # 确定新列的位置
        new_column_index = min(index1, index2) + 1

        # 生成修改表结构的SQL语句
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {new_column_name} NULL;")

        # 调整其他列的位置
        for i in range(len(columns) - 1, new_column_index, -1):
            cursor.execute(f"UPDATE {table_name} SET {columns[i]} = {columns[i-1]};")

        # 提交更改
        conn.commit()

        print(f"成功在 '{column1_name}' 和 '{column2_name}' 之间插入新列 '{new_column_name}'.")

    except sqlite3.Error as e:
        print(f"插入新列失败: {e}")

    finally:
        cursor.close()
        conn.close()
# 示例用法
# add_column_between_columns('your_database.db', 'your_table', 'new_column', 'existing_column1', 'existing_column2')

# 删除列，使用列在数据库中的数字索引，0.1.2.4
def delete_column_by_index(database_path, table_name, column_index):
    try:
        # 连接到SQLite数据库
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        # 获取表的列名列表
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        # 检查列索引是否有效
        if column_index < 0 or column_index >= len(columns):
            print("列索引无效，无法删除列。")
            return

        # 获取要删除的列名
        column_to_delete = columns[column_index][1]

        # 删除列
        cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_to_delete};")

        # 提交更改
        conn.commit()

        print(f"成功删除列'{column_to_delete}'.")

    except sqlite3.Error as e:
        print(f"删除列失败: {e}")

    finally:
        cursor.close()
        conn.close()
