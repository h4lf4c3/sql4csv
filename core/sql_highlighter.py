from PyQt5.QtCore import QRegExp, Qt
from PyQt5.QtGui import QColor, QTextCharFormat, QFont, QSyntaxHighlighter

class SQLSyntaxHighlighter(QSyntaxHighlighter):
    """SQL语法高亮器"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        self.highlighting_rules = []
        
        # 关键字格式
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor(0, 0, 255))  # 蓝色
        keyword_format.setFontWeight(QFont.Bold)
        
        # SQL关键字列表
        keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "ORDER BY", "GROUP BY",
            "HAVING", "LIMIT", "OFFSET", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN",
            "OUTER JOIN", "ON", "AS", "UNION", "ALL", "INSERT", "INTO", "VALUES",
            "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "INDEX", "VIEW", "DROP",
            "ALTER", "ADD", "COLUMN", "CONSTRAINT", "PRIMARY", "KEY", "FOREIGN",
            "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "NULL", "IS", "NOT", "LIKE",
            "IN", "BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
            "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "CAST", "COALESCE"
        ]
        
        # 添加关键字规则
        for word in keywords:
            pattern = QRegExp("\\b" + word + "\\b", Qt.CaseInsensitive)
            rule = (pattern, keyword_format)
            self.highlighting_rules.append(rule)
        
        # 函数格式
        function_format = QTextCharFormat()
        function_format.setForeground(QColor(170, 85, 0))  # 棕色
        function_format.setFontWeight(QFont.Bold)
        
        # 函数规则
        function_pattern = QRegExp("\\b[A-Za-z0-9_]+(?=\\()", Qt.CaseInsensitive)
        self.highlighting_rules.append((function_pattern, function_format))
        
        # 数字格式
        number_format = QTextCharFormat()
        number_format.setForeground(QColor(0, 128, 0))  # 绿色
        
        # 数字规则
        number_pattern = QRegExp("\\b[0-9]+\\b")
        self.highlighting_rules.append((number_pattern, number_format))
        
        # 字符串格式
        string_format = QTextCharFormat()
        string_format.setForeground(QColor(163, 21, 21))  # 红色
        
        # 单引号字符串规则
        string_pattern = QRegExp("'[^']*'")
        self.highlighting_rules.append((string_pattern, string_format))
        
        # 双引号字符串规则
        string_pattern = QRegExp('"[^"]*"')
        self.highlighting_rules.append((string_pattern, string_format))
        
        # 注释格式
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor(128, 128, 128))  # 灰色
        comment_format.setFontItalic(True)
        
        # 单行注释规则
        comment_pattern = QRegExp("--[^\n]*")
        self.highlighting_rules.append((comment_pattern, comment_format))
        
        # 多行注释规则 (/* ... */)
        self.comment_start_expression = QRegExp("/\\*")
        self.comment_end_expression = QRegExp("\\*/")
        self.multi_line_comment_format = comment_format
    
    def highlightBlock(self, text):
        # 应用单行规则
        for pattern, format in self.highlighting_rules:
            expression = QRegExp(pattern)
            index = expression.indexIn(text)
            while index >= 0:
                length = expression.matchedLength()
                self.setFormat(index, length, format)
                index = expression.indexIn(text, index + length)
        
        self.setCurrentBlockState(0)
        
        # 处理多行注释
        start_index = 0
        if self.previousBlockState() != 1:
            start_index = self.comment_start_expression.indexIn(text)
        
        while start_index >= 0:
            end_index = self.comment_end_expression.indexIn(text, start_index)
            
            if end_index == -1:
                self.setCurrentBlockState(1)
                comment_length = len(text) - start_index
            else:
                comment_length = end_index - start_index + self.comment_end_expression.matchedLength()
            
            self.setFormat(start_index, comment_length, self.multi_line_comment_format)
            start_index = self.comment_start_expression.indexIn(text, start_index + comment_length)