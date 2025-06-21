import sqlite3
import pandas as pd
from PyQt5.QtCore import QThread, pyqtSignal


class SQLQueryThread(QThread):
    """SQL查询线程，避免界面卡顿"""
    result_ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)
    progress_updated = pyqtSignal(int)
    
    def __init__(self, sql_query, tables):
        super().__init__()
        self.sql_query = sql_query
        self.tables = tables  # 表数据字典 {表名: DataFrame}
    
    def run(self):
        try:
            self.progress_updated.emit(10)
            
            # 在当前线程中创建新的数据库连接
            conn = sqlite3.connect(':memory:')
            
            # 将所有表导入到数据库
            self.progress_updated.emit(20)
            for table_name, df in self.tables.items():
                df.to_sql(table_name, conn, index=False, if_exists='replace')
            
            self.progress_updated.emit(50)
            # 执行查询
            result = pd.read_sql_query(self.sql_query, conn)
            
            # 关闭连接
            conn.close()
            
            self.progress_updated.emit(100)
            self.result_ready.emit(result)
        except Exception as e:
            self.error_occurred.emit(str(e))





