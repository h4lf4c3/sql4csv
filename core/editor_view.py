import json
import os
import duckdb
from datetime import datetime
import numpy as np
import pandas as pd
from PyQt5.QtCore import Qt, QTimer, QEvent
from PyQt5.QtGui import QFont, QKeySequence
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTextEdit, QTableWidget, QTableWidgetItem,
    QFileDialog, QMessageBox, QSplitter, QTabWidget, QComboBox,
    QLineEdit, QGroupBox, QHeaderView, QCheckBox,
    QSpinBox, QProgressBar, QDialog, QApplication
)

from chart_widget import ChartWidget
from sql_highlighter import SQLSyntaxHighlighter
from sql_query_thread import SQLQueryThread


class AdvancedCSVSQLEditor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.df = None
        self.db_connection = None
        self.table_name = "data_table"
        self.tables = {}  # 存储多个表的字典 {表名: DataFrame}
        self.query_history = []
        self.custom_templates = self.load_custom_templates()
        # self.init_ui()    # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 创建主布局
        main_layout = QVBoxLayout(central_widget)
        
        # 创建工具栏
        self.create_toolbar(main_layout)
        
        # 创建进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        # 创建分割器
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)
        
        # 左侧面板 - SQL编辑器和控制
        left_panel = self.create_left_panel()
        splitter.addWidget(left_panel)
        
        # 右侧面板 - 数据显示和图表
        right_panel = self.create_right_panel()
        splitter.addWidget(right_panel)
        
        # 设置分割器比例
        splitter.setSizes([500, 1100])
        
        # 状态栏
        self.statusBar().showMessage('请先加载CSV或Excel文件')
        
    def create_toolbar(self, layout):
        """创建工具栏"""
        toolbar_layout = QHBoxLayout()
        
        # 文件操作按钮
        self.load_btn = QPushButton('📁 加载文件')
        self.load_btn.clicked.connect(self.load_file)
        toolbar_layout.addWidget(self.load_btn)
        
        # 添加多表管理按钮
        self.manage_tables_btn = QPushButton('📑 管理表')
        self.manage_tables_btn.clicked.connect(self.manage_tables)
        toolbar_layout.addWidget(self.manage_tables_btn)
        
        # 文件信息显示
        self.file_info_label = QLabel('未加载文件')
        toolbar_layout.addWidget(self.file_info_label)
        
        toolbar_layout.addStretch()
        
        # 数据预处理选项
        self.auto_clean_cb = QCheckBox('自动清理数据')
        self.auto_clean_cb.setChecked(True)
        toolbar_layout.addWidget(self.auto_clean_cb)
        
        # 显示行数限制
        toolbar_layout.addWidget(QLabel('显示行数:'))
        self.display_limit_spin = QSpinBox()
        self.display_limit_spin.setRange(100, 10000)
        self.display_limit_spin.setValue(1000)
        toolbar_layout.addWidget(self.display_limit_spin)
        
        # 导出按钮
        self.export_btn = QPushButton('💾 导出结果')
        self.export_btn.clicked.connect(self.export_results)
        self.export_btn.setEnabled(False)
        toolbar_layout.addWidget(self.export_btn)
        
        # 帮助按钮
        self.help_btn = QPushButton('❓ 帮助')
        self.help_btn.clicked.connect(self.show_help)
        toolbar_layout.addWidget(self.help_btn)
        
        layout.addLayout(toolbar_layout)
        
    def create_left_panel(self):
        """创建左侧SQL编辑面板"""
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        
        # 表列表组
        tables_group = QGroupBox('数据表列表')
        tables_layout = QVBoxLayout(tables_group)
        
        # 表列表
        self.tables_list = QTableWidget()
        self.tables_list.setColumnCount(3)
        self.tables_list.setHorizontalHeaderLabels(['表名', '行数', '列数'])
        self.tables_list.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tables_list.setSelectionBehavior(QTableWidget.SelectRows)
        self.tables_list.setSelectionMode(QTableWidget.SingleSelection)
        self.tables_list.itemClicked.connect(self.on_table_selected)
        tables_layout.addWidget(self.tables_list)
        
        # 表元数据按钮
        metadata_btn = QPushButton('📋 查看表结构')
        metadata_btn.clicked.connect(self.show_table_metadata)
        tables_layout.addWidget(metadata_btn)
        
        left_layout.addWidget(tables_group)
        
        # SQL编辑器组
        sql_group = QGroupBox('SQL查询编辑器')
        sql_layout = QVBoxLayout(sql_group)
        
        # SQL编辑器
        self.sql_editor = QTextEdit()
        self.sql_editor.setFont(QFont('Consolas', 10))
        self.sql_editor.setPlaceholderText(
            "请输入SQL查询语句...\n\n示例:\nSELECT * FROM table1 LIMIT 10;\n\n"
            "-- 多表关联查询示例:\nSELECT a.字段1, b.字段2 \nFROM table1 a \nJOIN table2 b ON a.ID = b.ID \nWHERE a.字段1 > 100;"
        )
        
        # 应用SQL语法高亮
        self.sql_highlighter = SQLSyntaxHighlighter(self.sql_editor.document())
        
        sql_layout.addWidget(self.sql_editor)
        
        # 查询按钮
        query_layout = QHBoxLayout()
        self.execute_btn = QPushButton('▶️ 执行查询')
        self.execute_btn.clicked.connect(self.execute_query)
        self.execute_btn.setEnabled(False)
        query_layout.addWidget(self.execute_btn)
        
        self.clear_btn = QPushButton('🗑️ 清空')
        self.clear_btn.clicked.connect(self.clear_sql)
        query_layout.addWidget(self.clear_btn)
        
        self.format_btn = QPushButton('🎨 格式化')
        self.format_btn.clicked.connect(self.format_sql)
        query_layout.addWidget(self.format_btn)
        
        # 添加查看数据库结构按钮
        self.schema_btn = QPushButton('📊 查看数据库结构')
        self.schema_btn.clicked.connect(self.show_database_schema)
        query_layout.addWidget(self.schema_btn)
        
        sql_layout.addLayout(query_layout)
        left_layout.addWidget(sql_group)
        
        # 查询历史
        history_group = QGroupBox('查询历史')
        history_layout = QVBoxLayout(history_group)
        
        self.history_list = QTextEdit()
        self.history_list.setMaximumHeight(100)
        self.history_list.setReadOnly(True)
        history_layout.addWidget(self.history_list)
        
        left_layout.addWidget(history_group)
        
        # 常用查询模板
        template_group = QGroupBox('常用查询模板')
        template_layout = QVBoxLayout(template_group)
        
        # 模板选择下拉菜单
        template_select_layout = QHBoxLayout()
        template_select_layout.addWidget(QLabel('选择模板:'))
        
        self.template_combo = QComboBox()
        self.template_combo.setMinimumWidth(200)
        self.load_templates_to_combo()
        template_select_layout.addWidget(self.template_combo)
        
        # 应用模板按钮
        apply_template_btn = QPushButton('📋 应用模板')
        apply_template_btn.clicked.connect(self.apply_selected_template)
        template_select_layout.addWidget(apply_template_btn)
        
        template_layout.addLayout(template_select_layout)
        
        # 自定义模板管理
        custom_template_layout = QHBoxLayout()
        
        add_template_btn = QPushButton('➕ 添加模板')
        add_template_btn.clicked.connect(self.add_custom_template)
        custom_template_layout.addWidget(add_template_btn)
        
        edit_template_btn = QPushButton('✏️ 编辑模板')
        edit_template_btn.clicked.connect(self.edit_custom_template)
        custom_template_layout.addWidget(edit_template_btn)
        
        delete_template_btn = QPushButton('🗑️ 删除模板')
        delete_template_btn.clicked.connect(self.delete_custom_template)
        custom_template_layout.addWidget(delete_template_btn)
        
        template_layout.addLayout(custom_template_layout)
        
        left_layout.addWidget(template_group)
        
        # 数据信息面板
        info_group = QGroupBox('数据信息')
        info_layout = QVBoxLayout(info_group)
        
        self.info_text = QTextEdit()
        self.info_text.setMaximumHeight(200)
        self.info_text.setReadOnly(True)
        info_layout.addWidget(self.info_text)
        
        left_layout.addWidget(info_group)
        
        return left_widget
        
    def create_right_panel(self):
        """创建右侧数据显示面板"""
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        
        # 创建标签页
        self.tab_widget = QTabWidget()
        
        # 原始数据标签页
        self.original_table = QTableWidget()
        self.original_table.setSelectionMode(QTableWidget.ContiguousSelection)  # 允许连续选择
        self.tab_widget.addTab(self.original_table, '📊 原始数据')
        
        # 查询结果标签页
        self.result_table = QTableWidget()
        self.result_table.setSelectionMode(QTableWidget.ContiguousSelection)  # 允许连续选择
        self.tab_widget.addTab(self.result_table, '🔍 查询结果')
        
        # 为表格添加复制功能
        self.setup_copy_functionality()
        
        # 图表标签页
        self.chart_widget = ChartWidget()
        self.tab_widget.addTab(self.chart_widget, '📈 数据可视化')
        
        # 数据分析标签页
        analysis_widget = self.create_analysis_widget()
        self.tab_widget.addTab(analysis_widget, '📋 数据分析')
        
        right_layout.addWidget(self.tab_widget)
        
        return right_widget
        
    def create_analysis_widget(self):
        """创建数据分析标签页"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # 分析控制面板
        control_panel = QWidget()
        control_layout = QHBoxLayout(control_panel)
        
        self.analyze_btn = QPushButton('🔬 生成数据分析报告')
        self.analyze_btn.clicked.connect(self.generate_analysis)
        control_layout.addWidget(self.analyze_btn)
        
        control_layout.addStretch()
        layout.addWidget(control_panel)
        
        # 分析结果显示
        self.analysis_text = QTextEdit()
        self.analysis_text.setReadOnly(True)
        layout.addWidget(self.analysis_text)
        
        return widget
        
    def load_file(self):
        """加载CSV或Excel文件"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, '选择文件', '', 
            'CSV文件 (*.csv);;Excel文件 (*.xlsx *.xls);;所有文件 (*)'
        )
        
        if file_path:
            # 弹出对话框让用户输入表名
            from PyQt5.QtWidgets import QInputDialog
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            default_table_name = file_name.lower().replace(' ', '_')
            
            table_name, ok = QInputDialog.getText(
                self, '输入表名', 
                '请为导入的数据表指定一个名称（仅使用字母、数字和下划线）：',
                text=default_table_name
            )
            
            if not ok or not table_name:
                return
                
            # 验证表名是否合法（只包含字母、数字和下划线）
            import re
            if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                QMessageBox.warning(self, '警告', '表名只能包含字母、数字和下划线')
                return
                
            # 检查表名是否已存在
            if table_name in self.tables and QMessageBox.question(
                self, '确认覆盖', 
                f'表 "{table_name}" 已存在，是否覆盖？',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            ) != QMessageBox.Yes:
                return
                
            try:
                self.progress_bar.setVisible(True)
                self.progress_bar.setValue(0)
                
                # 根据文件扩展名选择读取方法
                if file_path.lower().endswith('.csv'):
                    # 尝试不同的编码
                    encodings = ['utf-8', 'gbk', 'gb2312', 'latin1']
                    for encoding in encodings:
                        try:
                            df = pd.read_csv(file_path, encoding=encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        raise Exception("无法识别文件编码")
                        
                elif file_path.lower().endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file_path)
                else:
                    raise Exception("不支持的文件格式")
                
                self.progress_bar.setValue(50)
                
                # 数据清理
                if self.auto_clean_cb.isChecked():
                    df = self.clean_data(df)
                
                # 保存到表字典中
                self.tables[table_name] = df
                
                # 如果是第一个表，设为当前表
                if len(self.tables) == 1 or self.df is None:
                    self.df = df
                    self.table_name = table_name
                
                # 更新数据库
                self.create_database()
                
                self.progress_bar.setValue(80)
                
                # 更新表列表
                self.update_tables_list()
                
                # 显示原始数据
                self.display_original_data()
                
                # 更新图表组件
                self.chart_widget.update_data(self.df)
                
                self.progress_bar.setValue(100)
                
                # 更新界面状态
                file_name = os.path.basename(file_path)
                self.file_info_label.setText(f'已加载: {file_name} ({len(self.df)}行, {len(self.df.columns)}列)')
                self.execute_btn.setEnabled(True)
                self.statusBar().showMessage(f'文件加载成功: {file_name}')
                
                # 显示数据信息
                self.show_data_info()
                
                # 隐藏进度条
                QTimer.singleShot(1000, lambda: self.progress_bar.setVisible(False))
                
            except Exception as e:
                self.progress_bar.setVisible(False)
                QMessageBox.critical(self, '错误', f'加载文件失败:\n{str(e)}')
                
    def clean_data(self, df):
        """数据清理"""
        if df is not None:
            # 删除完全空白的行
            df = df.dropna(how='all')
            
            # 清理列名（去除前后空格）
            df.columns = df.columns.str.strip()
            
            # 重置索引
            df = df.reset_index(drop=True)
            
        return df
                
    def create_database(self):
        """创建内存DuckDB数据库"""
        # 关闭旧连接（如果存在）
        if self.db_connection is not None:
            self.db_connection.close()
        # 创建新连接
        self.db_connection = duckdb.connect(':memory:')
        # 将所有表导入到数据库
        for table_name, df in self.tables.items():
            # DuckDB可以直接从DataFrame创建表
            df.to_sql(table_name, self.db_connection, index=False, if_exists="replace")
            # self.db_connection.register(table_name, df)
        # 启用执行按钮
        self.execute_btn.setEnabled(len(self.tables) > 0)
        
    def update_tables_list(self):
        """更新表列表显示"""
        self.tables_list.setRowCount(len(self.tables))
        
        for i, (table_name, df) in enumerate(self.tables.items()):
            # 表名
            name_item = QTableWidgetItem(table_name)
            self.tables_list.setItem(i, 0, name_item)
            
            # 行数
            rows_item = QTableWidgetItem(str(len(df)))
            self.tables_list.setItem(i, 1, rows_item)
            
            # 列数
            cols_item = QTableWidgetItem(str(len(df.columns)))
            self.tables_list.setItem(i, 2, cols_item)
            
        # 更新文件信息标签
        if len(self.tables) > 0:
            self.file_info_label.setText(f'已加载: {len(self.tables)}个表')
        else:
            self.file_info_label.setText('未加载文件')
    
    def on_table_selected(self, item):
        """处理表选择事件"""
        row = item.row()
        table_name = self.tables_list.item(row, 0).text()
        
        if table_name in self.tables:
            # 更新当前表
            self.table_name = table_name
            self.df = self.tables[table_name]
            
            # 显示表数据
            self.display_original_data()
            
            # 更新图表组件
            self.chart_widget.update_data(self.df)
            
            # 更新状态栏
            self.statusBar().showMessage(f'已选择表: {table_name} ({len(self.df)}行, {len(self.df.columns)}列)')
    
    def show_table_metadata(self):
        """显示表结构"""
        # 检查是否有选中的表
        selected_items = self.tables_list.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, '警告', '请先选择一个表')
            return
            
        row = selected_items[0].row()
        table_name = self.tables_list.item(row, 0).text()
        
        if table_name not in self.tables:
            return
            
        df = self.tables[table_name]
        
        # 创建表结构对话框
        from PyQt5.QtWidgets import QDialog, QVBoxLayout, QTableWidget, QTableWidgetItem
        
        dialog = QDialog(self)
        dialog.setWindowTitle(f'表结构: {table_name}')
        dialog.resize(600, 400)
        
        layout = QVBoxLayout(dialog)
        
        # 创建表结构表格
        metadata_table = QTableWidget()
        metadata_table.setColumnCount(4)
        metadata_table.setHorizontalHeaderLabels(['列名', '数据类型', '非空值数', '唯一值数'])
        metadata_table.setRowCount(len(df.columns))
        
        for i, col in enumerate(df.columns):
            # 列名
            metadata_table.setItem(i, 0, QTableWidgetItem(col))
            
            # 数据类型
            dtype = str(df[col].dtype)
            metadata_table.setItem(i, 1, QTableWidgetItem(dtype))
            
            # 非空值数
            non_null = df[col].count()
            metadata_table.setItem(i, 2, QTableWidgetItem(f'{non_null}/{len(df)} ({non_null/len(df)*100:.1f}%)'))
            
            # 唯一值数
            unique = df[col].nunique()
            metadata_table.setItem(i, 3, QTableWidgetItem(f'{unique} ({unique/len(df)*100:.1f}%)'))
        
        metadata_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(metadata_table)
        
        dialog.exec_()
    
    def display_original_data(self):
        """显示原始数据"""
        self.populate_table(self.original_table, self.df)
        
    def setup_copy_functionality(self):
        """设置表格的复制功能"""
        # 为原始数据表和结果表安装事件过滤器
        self.original_table.installEventFilter(self)
        self.result_table.installEventFilter(self)
        
        # 添加右键菜单
        self.original_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.original_table.customContextMenuRequested.connect(self.show_table_context_menu)
        self.result_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.result_table.customContextMenuRequested.connect(self.show_table_context_menu)
    
    def eventFilter(self, source, event):
        """事件过滤器，处理键盘事件"""
        # 检查是否是键盘事件，以及事件源是否是表格
        if (event.type() == QEvent.KeyPress and 
            (source is self.original_table or source is self.result_table)):
            
            # 检查是否是Ctrl+C
            if event.key() == Qt.Key_C and event.modifiers() == Qt.ControlModifier:
                self.copy_selection(source)
                return True  # 事件已处理
                
        # 其他事件交给默认处理器
        return super().eventFilter(source, event)
    
    def copy_selection(self, table):
        """复制表格中选中的单元格内容到剪贴板"""
        selection = table.selectedRanges()
        if not selection:  # 没有选中任何内容
            return
            
        # 获取选中区域
        selected_text = []
        for ranges in selection:
            for row in range(ranges.topRow(), ranges.bottomRow() + 1):
                row_text = []
                for col in range(ranges.leftColumn(), ranges.rightColumn() + 1):
                    item = table.item(row, col)
                    if item is not None:
                        row_text.append(item.text())
                    else:
                        row_text.append('')  # 空单元格
                selected_text.append('\t'.join(row_text))
        
        # 将内容复制到剪贴板
        clipboard_text = '\n'.join(selected_text)
        QApplication.clipboard().setText(clipboard_text)
        
        # 显示状态栏消息
        self.statusBar().showMessage('已复制选中内容到剪贴板', 2000)
    
    def show_table_context_menu(self, position):
        """显示表格右键菜单"""
        # 确定事件源
        sender = self.sender()
        if not sender.selectedRanges():  # 没有选中任何内容
            return
            
        # 创建右键菜单
        from PyQt5.QtWidgets import QMenu, QAction
        menu = QMenu()
        copy_action = QAction('复制 (Ctrl+C)', self)
        copy_action.triggered.connect(lambda: self.copy_selection(sender))
        menu.addAction(copy_action)
        
        # 显示菜单
        menu.exec_(sender.mapToGlobal(position))
    
    def populate_table(self, table_widget, dataframe):
        """填充表格数据"""
        if dataframe is None or dataframe.empty:
            table_widget.setRowCount(0)
            table_widget.setColumnCount(0)
            return
            
        # 限制显示行数以提高性能
        limit = self.display_limit_spin.value()
        display_df = dataframe.head(limit) if len(dataframe) > limit else dataframe
        
        table_widget.setRowCount(len(display_df))
        table_widget.setColumnCount(len(display_df.columns))
        
        # 设置列标题
        table_widget.setHorizontalHeaderLabels([str(col) for col in display_df.columns])
        
        # 填充数据
        for i, row in enumerate(display_df.itertuples(index=False)):
            for j, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                table_widget.setItem(i, j, item)
                
        # 调整列宽
        table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        
        # 如果数据被截断，显示提示
        if len(dataframe) > limit:
            table_widget.setRowCount(len(display_df) + 1)
            for j in range(len(display_df.columns)):
                item = QTableWidgetItem(f"... 还有 {len(dataframe) - limit} 行数据未显示")
                item.setBackground(Qt.lightGray)
                table_widget.setItem(len(display_df), j, item)
                
    def show_data_info(self):
        """显示数据信息"""
        if self.df is None:
            return
            
        info_text = f"📊 数据概览 (加载时间: {datetime.now().strftime('%H:%M:%S')})\n"
        info_text += f"{'='*50}\n"
        info_text += f"📏 数据维度: {len(self.df)} 行 × {len(self.df.columns)} 列\n"
        info_text += f"💾 内存使用: {self.df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB\n\n"
        
        info_text += "📋 列信息:\n"
        for i, (col, dtype) in enumerate(zip(self.df.columns, self.df.dtypes)):
            null_count = self.df[col].isnull().sum()
            null_pct = (null_count / len(self.df)) * 100
            unique_count = self.df[col].nunique()
            
            info_text += f"{i+1:2d}. {col:<20} | {str(dtype):<10} | 空值: {null_count:4d}({null_pct:5.1f}%) | 唯一值: {unique_count}\n"
            
        # 数值列统计
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            info_text += f"\n📊 数值列统计:\n"
            for col in numeric_cols:
                stats = self.df[col].describe()
                info_text += f"{col}: 均值={stats['mean']:.2f}, 中位数={stats['50%']:.2f}, 标准差={stats['std']:.2f}\n"
            
        self.info_text.setText(info_text)
        
    def get_default_templates(self):
        """获取默认模板"""
        return [
            ('查看所有数据', 'SELECT * FROM table_name LIMIT 100;'),
            ('数据统计', 'SELECT COUNT(*) as 总行数 FROM table_name;'),
            ('列信息', 'PRAGMA table_info(table_name);'),
            ('数值列统计', 'SELECT\n  AVG(column_name) as 平均值,\n  MIN(column_name) as 最小值,\n  MAX(column_name) as 最大值\nFROM table_name;'),
            ('分组统计', 'SELECT column_name, COUNT(*) as 数量\nFROM table_name\nGROUP BY column_name\nORDER BY 数量 DESC;'),
            ('去重查询', 'SELECT DISTINCT column_name FROM table_name;'),
            ('条件筛选', 'SELECT * FROM table_name\nWHERE column_name > value\nLIMIT 100;'),
            ('排序查询', 'SELECT * FROM table_name\nORDER BY column_name DESC\nLIMIT 100;'),
            ('多表关联查询', 'SELECT a.column1, b.column2\nFROM table1 a\nJOIN table2 b ON a.id = b.id\nWHERE a.column1 > value\nLIMIT 100;'),
            ('查看所有表', 'SELECT name FROM sqlite_master WHERE type="table";'),
            ('查看表结构', 'PRAGMA table_info(table_name);')
        ]
        
    def show_database_schema(self):
        """显示数据库中的所有表和列信息"""
        if not self.tables:
            QMessageBox.warning(self, '警告', '没有已导入的表')
            return
            
        # 创建对话框
        from PyQt5.QtWidgets import QDialog, QVBoxLayout, QTreeWidget, QTreeWidgetItem
        
        dialog = QDialog(self)
        dialog.setWindowTitle('数据库结构')
        dialog.resize(600, 500)
        
        layout = QVBoxLayout(dialog)
        
        # 创建树形结构
        tree = QTreeWidget()
        tree.setHeaderLabels(['名称', '类型', '备注'])
        tree.setColumnWidth(0, 250)
        
        # 添加表和列
        for table_name, df in self.tables.items():
            # 创建表节点
            table_item = QTreeWidgetItem(tree)
            table_item.setText(0, table_name)
            table_item.setText(1, '表')
            table_item.setText(2, f'{len(df)}行, {len(df.columns)}列')
            
            # 添加列节点
            for col in df.columns:
                col_item = QTreeWidgetItem(table_item)
                col_item.setText(0, col)
                col_item.setText(1, str(df[col].dtype))
                
                # 添加列统计信息
                non_null = df[col].count()
                unique = df[col].nunique()
                col_item.setText(2, f'非空: {non_null}/{len(df)}, 唯一值: {unique}')
        
        tree.expandAll()  # 展开所有节点
        layout.addWidget(tree)
        
        # 添加帮助信息
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        help_text.setMaximumHeight(150)
        help_text.setHtml("""
        <h3>多表关联查询示例</h3>
        <pre>
        -- 内连接示例
        SELECT a.列名1, b.列名2
        FROM 表1 a
        JOIN 表2 b ON a.关联列 = b.关联列
        WHERE a.列名1 > 值
        LIMIT 100;
        
        -- 左连接示例
        SELECT a.列名1, b.列名2
        FROM 表1 a
        LEFT JOIN 表2 b ON a.关联列 = b.关联列
        WHERE a.列名1 > 值
        LIMIT 100;
        </pre>
        """)
        layout.addWidget(help_text)
        
        # 添加复制到编辑器按钮
        copy_btn = QPushButton('复制选中表名到编辑器')
        def copy_to_editor():
            selected_items = tree.selectedItems()
            if selected_items:
                item = selected_items[0]
                # 如果选中的是列，获取其父表
                if item.parent():
                    table_name = item.parent().text(0)
                    col_name = item.text(0)
                    text = f"{table_name}.{col_name}"
                else:
                    text = item.text(0)
                    
                # 插入到编辑器当前位置
                self.sql_editor.insertPlainText(text)
                
        copy_btn.clicked.connect(copy_to_editor)
        layout.addWidget(copy_btn)
        
        dialog.exec_()
    
    def load_custom_templates(self):
        """加载自定义模板"""
        try:
            config_file = 'sql_templates.json'
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            print(f"加载自定义模板失败: {e}")
            return []
    
    def save_custom_templates(self):
        """保存自定义模板"""
        try:
            config_file = 'sql_templates.json'
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(self.custom_templates, f, ensure_ascii=False, indent=2)
        except Exception as e:
            QMessageBox.critical(self, '错误', f'保存模板失败:\n{str(e)}')
    
    def load_templates_to_combo(self):
        """加载模板到下拉菜单"""
        self.template_combo.clear()
        self.template_combo.addItem('-- 请选择模板 --', '')
        
        # 添加默认模板
        self.template_combo.addItem('--- 默认模板 ---', '')
        for name, sql in self.get_default_templates():
            self.template_combo.addItem(f"📋 {name}", sql)
        
        # 添加自定义模板
        if self.custom_templates:
            self.template_combo.addItem('--- 自定义模板 ---', '')
            for template in self.custom_templates:
                self.template_combo.addItem(f"⭐ {template['name']}", template['sql'])
    
    def apply_selected_template(self):
        """应用选中的模板"""
        sql = self.template_combo.currentData()
        if sql:
            self.sql_editor.setText(sql)
        else:
            QMessageBox.information(self, '提示', '请先选择一个模板')
    
    def add_custom_template(self):
        """添加自定义模板"""
        from PyQt5.QtWidgets import QDialog, QDialogButtonBox, QFormLayout
        
        dialog = QDialog(self)
        dialog.setWindowTitle('添加自定义模板')
        dialog.setModal(True)
        dialog.resize(500, 400)
        
        layout = QVBoxLayout(dialog)
        form_layout = QFormLayout()
        
        # 模板名称
        name_edit = QLineEdit()
        name_edit.setPlaceholderText('请输入模板名称')
        form_layout.addRow('模板名称:', name_edit)
        
        # 模板描述
        desc_edit = QLineEdit()
        desc_edit.setPlaceholderText('请输入模板描述（可选）')
        form_layout.addRow('模板描述:', desc_edit)
        
        layout.addLayout(form_layout)
        
        # SQL内容
        layout.addWidget(QLabel('SQL内容:'))
        sql_edit = QTextEdit()
        sql_edit.setFont(QFont('Consolas', 10))
        sql_edit.setPlaceholderText('请输入SQL语句...')
        # 如果编辑器中有内容，预填充
        current_sql = self.sql_editor.toPlainText().strip()
        if current_sql:
            sql_edit.setText(current_sql)
        layout.addWidget(sql_edit)
        
        # 按钮
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        if dialog.exec_() == QDialog.Accepted:
            name = name_edit.text().strip()
            desc = desc_edit.text().strip()
            sql = sql_edit.toPlainText().strip()
            
            if not name:
                QMessageBox.warning(self, '警告', '请输入模板名称')
                return
            
            if not sql:
                QMessageBox.warning(self, '警告', '请输入SQL内容')
                return
            
            # 检查名称是否重复
            for template in self.custom_templates:
                if template['name'] == name:
                    QMessageBox.warning(self, '警告', '模板名称已存在')
                    return
            
            # 添加模板
            new_template = {
                'name': name,
                'description': desc,
                'sql': sql,
                'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.custom_templates.append(new_template)
            self.save_custom_templates()
            self.load_templates_to_combo()
            
            QMessageBox.information(self, '成功', f'模板 "{name}" 添加成功')
    
    def edit_custom_template(self):
        """编辑自定义模板"""
        if not self.custom_templates:
            QMessageBox.information(self, '提示', '没有自定义模板可编辑')
            return
        
        from PyQt5.QtWidgets import QDialog, QDialogButtonBox, QFormLayout, QListWidget
        
        # 选择要编辑的模板
        select_dialog = QDialog(self)
        select_dialog.setWindowTitle('选择要编辑的模板')
        select_dialog.setModal(True)
        select_dialog.resize(400, 300)
        
        layout = QVBoxLayout(select_dialog)
        layout.addWidget(QLabel('请选择要编辑的模板:'))
        
        template_list = QListWidget()
        for i, template in enumerate(self.custom_templates):
            template_list.addItem(f"{template['name']} - {template.get('description', '无描述')}")
        layout.addWidget(template_list)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(select_dialog.accept)
        button_box.rejected.connect(select_dialog.reject)
        layout.addWidget(button_box)
        
        if select_dialog.exec_() != QDialog.Accepted or template_list.currentRow() < 0:
            return
        
        selected_index = template_list.currentRow()
        selected_template = self.custom_templates[selected_index]
        
        # 编辑模板
        dialog = QDialog(self)
        dialog.setWindowTitle('编辑自定义模板')
        dialog.setModal(True)
        dialog.resize(500, 400)
        
        layout = QVBoxLayout(dialog)
        form_layout = QFormLayout()
        
        # 模板名称
        name_edit = QLineEdit(selected_template['name'])
        form_layout.addRow('模板名称:', name_edit)
        
        # 模板描述
        desc_edit = QLineEdit(selected_template.get('description', ''))
        form_layout.addRow('模板描述:', desc_edit)
        
        layout.addLayout(form_layout)
        
        # SQL内容
        layout.addWidget(QLabel('SQL内容:'))
        sql_edit = QTextEdit()
        sql_edit.setFont(QFont('Consolas', 10))
        sql_edit.setText(selected_template['sql'])
        layout.addWidget(sql_edit)
        
        # 按钮
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        if dialog.exec_() == QDialog.Accepted:
            name = name_edit.text().strip()
            desc = desc_edit.text().strip()
            sql = sql_edit.toPlainText().strip()
            
            if not name:
                QMessageBox.warning(self, '警告', '请输入模板名称')
                return
            
            if not sql:
                QMessageBox.warning(self, '警告', '请输入SQL内容')
                return
            
            # 检查名称是否与其他模板重复
            for i, template in enumerate(self.custom_templates):
                if i != selected_index and template['name'] == name:
                    QMessageBox.warning(self, '警告', '模板名称已存在')
                    return
            
            # 更新模板
            self.custom_templates[selected_index].update({
                'name': name,
                'description': desc,
                'sql': sql,
                'modified_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            self.save_custom_templates()
            self.load_templates_to_combo()
            
            QMessageBox.information(self, '成功', f'模板 "{name}" 更新成功')
    
    def delete_custom_template(self):
        """删除自定义模板"""
        if not self.custom_templates:
            QMessageBox.information(self, '提示', '没有自定义模板可删除')
            return
        
        from PyQt5.QtWidgets import QDialog, QDialogButtonBox, QListWidget
        
        # 选择要删除的模板
        dialog = QDialog(self)
        dialog.setWindowTitle('删除自定义模板')
        dialog.setModal(True)
        dialog.resize(400, 300)
        
        layout = QVBoxLayout(dialog)
        layout.addWidget(QLabel('请选择要删除的模板:'))
        
        template_list = QListWidget()
        for template in self.custom_templates:
            template_list.addItem(f"{template['name']} - {template.get('description', '无描述')}")
        layout.addWidget(template_list)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        if dialog.exec_() == QDialog.Accepted and template_list.currentRow() >= 0:
            selected_index = template_list.currentRow()
            template_name = self.custom_templates[selected_index]['name']
            
            reply = QMessageBox.question(
                self, '确认删除', 
                f'确定要删除模板 "{template_name}" 吗？\n此操作不可撤销。',
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                del self.custom_templates[selected_index]
                self.save_custom_templates()
                self.load_templates_to_combo()
                QMessageBox.information(self, '成功', f'模板 "{template_name}" 删除成功')
    
    def insert_template(self, sql):
        """插入SQL模板（保留兼容性）"""
        self.sql_editor.setText(sql)
        
    def clear_sql(self):
        """清空SQL编辑器"""
        self.sql_editor.clear()
        
    def format_sql(self):
        """格式化SQL语句"""
        sql = self.sql_editor.toPlainText()
        if sql.strip():
            # 简单的SQL格式化
            formatted_sql = sql.replace(',', ',\n    ')
            formatted_sql = formatted_sql.replace(' FROM ', '\nFROM ')
            formatted_sql = formatted_sql.replace(' WHERE ', '\nWHERE ')
            formatted_sql = formatted_sql.replace(' GROUP BY ', '\nGROUP BY ')
            formatted_sql = formatted_sql.replace(' ORDER BY ', '\nORDER BY ')
            formatted_sql = formatted_sql.replace(' HAVING ', '\nHAVING ')
            self.sql_editor.setText(formatted_sql)
        
    def manage_tables(self):
        """管理数据表"""
        if not self.tables:
            QMessageBox.information(self, '提示', '没有已导入的表可管理')
            return
            
        from PyQt5.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QListWidget, QPushButton
        
        dialog = QDialog(self)
        dialog.setWindowTitle('管理数据表')
        dialog.resize(500, 400)
        
        layout = QVBoxLayout(dialog)
        
        # 表列表
        layout.addWidget(QLabel('已导入的表:'))
        tables_list = QListWidget()
        tables_list.addItems(self.tables.keys())
        layout.addWidget(tables_list)
        
        # 按钮区域
        buttons_layout = QHBoxLayout()
        
        rename_btn = QPushButton('重命名表')
        delete_btn = QPushButton('删除表')
        
        buttons_layout.addWidget(rename_btn)
        buttons_layout.addWidget(delete_btn)
        layout.addLayout(buttons_layout)
        
        # 重命名表
        def rename_table():
            selected_items = tables_list.selectedItems()
            if not selected_items:
                QMessageBox.warning(dialog, '警告', '请先选择一个表')
                return
                
            old_name = selected_items[0].text()
            
            from PyQt5.QtWidgets import QInputDialog
            new_name, ok = QInputDialog.getText(
                dialog, '重命名表', 
                '请输入新的表名（仅使用字母、数字和下划线）：',
                text=old_name
            )
            
            if not ok or not new_name:
                return
                
            # 验证表名是否合法
            import re
            if not re.match(r'^[a-zA-Z0-9_]+$', new_name):
                QMessageBox.warning(dialog, '警告', '表名只能包含字母、数字和下划线')
                return
                
            # 检查新表名是否已存在
            if new_name in self.tables and new_name != old_name:
                QMessageBox.warning(dialog, '警告', f'表名 "{new_name}" 已存在')
                return
                
            # 重命名表
            self.tables[new_name] = self.tables.pop(old_name)
            
            # 如果重命名的是当前表，更新当前表名
            if self.table_name == old_name:
                self.table_name = new_name
                
            # 更新数据库
            self.create_database()
            
            # 更新表列表
            self.update_tables_list()
            tables_list.clear()
            tables_list.addItems(self.tables.keys())
            
            QMessageBox.information(dialog, '成功', f'表 "{old_name}" 已重命名为 "{new_name}"')
        
        # 删除表
        def delete_table():
            selected_items = tables_list.selectedItems()
            if not selected_items:
                QMessageBox.warning(dialog, '警告', '请先选择一个表')
                return
                
            table_name = selected_items[0].text()
            
            if QMessageBox.question(
                dialog, '确认删除', 
                f'确定要删除表 "{table_name}" 吗？\n此操作不可撤销。',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            ) != QMessageBox.Yes:
                return
                
            # 删除表
            del self.tables[table_name]
            
            # 如果删除的是当前表，更新当前表
            if self.table_name == table_name:
                if self.tables:
                    # 选择第一个表作为当前表
                    self.table_name = next(iter(self.tables))
                    self.df = self.tables[self.table_name]
                    self.display_original_data()
                    self.chart_widget.update_data(self.df)
                else:
                    # 没有表了
                    self.table_name = "data_table"
                    self.df = None
                    self.original_table.setRowCount(0)
                    self.original_table.setColumnCount(0)
                    self.chart_widget.update_data(None)
                    
            # 更新数据库
            self.create_database()
            
            # 更新表列表
            self.update_tables_list()
            tables_list.clear()
            tables_list.addItems(self.tables.keys())
            
            QMessageBox.information(dialog, '成功', f'表 "{table_name}" 已删除')
        
        # 连接按钮信号
        rename_btn.clicked.connect(rename_table)
        delete_btn.clicked.connect(delete_table)
        
        dialog.exec_()
    
    def execute_query(self):
        """执行SQL查询"""
        if self.db_connection is None:
            QMessageBox.warning(self, '警告', '请先加载数据文件')
            return
            
        sql_query = self.sql_editor.toPlainText().strip()
        if not sql_query:
            QMessageBox.warning(self, '警告', '请输入SQL查询语句')
            return
            
        # 添加到查询历史
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.query_history.append(f"[{timestamp}] {sql_query[:50]}...")
        self.update_history_display()
        
        # 显示进度条
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        
        # 禁用执行按钮
        self.execute_btn.setEnabled(False)
        self.execute_btn.setText('执行中...')
        
        # 创建查询线程
        self.query_thread = SQLQueryThread(sql_query, self.tables)
        self.query_thread.result_ready.connect(self.on_query_success)
        self.query_thread.error_occurred.connect(self.on_query_error)
        self.query_thread.progress_updated.connect(self.progress_bar.setValue)
        self.query_thread.start()
        
    def update_history_display(self):
        """更新查询历史显示"""
        history_text = '\n'.join(self.query_history[-10:])  # 只显示最近10条
        self.history_list.setText(history_text)
        
    def on_query_success(self, result_df):
        """查询成功回调"""
        self.populate_table(self.result_table, result_df)
        self.tab_widget.setCurrentIndex(1)  # 切换到结果标签页
        
        # 更新图表组件数据
        self.chart_widget.update_data(result_df)
        
        # 保存结果用于导出
        self.query_result = result_df
        self.export_btn.setEnabled(True)
        
        # 更新状态
        self.execute_btn.setEnabled(True)
        self.execute_btn.setText('▶️ 执行查询')
        self.statusBar().showMessage(f'查询完成，返回 {len(result_df)} 行 × {len(result_df.columns)} 列结果')
        
        # 隐藏进度条
        QTimer.singleShot(1000, lambda: self.progress_bar.setVisible(False))
        
    def on_query_error(self, error_msg):
        """查询错误回调"""
        QMessageBox.critical(self, 'SQL查询错误', f'查询执行失败:\n{error_msg}')
        
        # 恢复按钮状态
        self.execute_btn.setEnabled(True)
        self.execute_btn.setText('▶️ 执行查询')
        self.statusBar().showMessage('查询失败')
        self.progress_bar.setVisible(False)
        
    def generate_analysis(self):
        """生成数据分析报告"""
        if self.df is None:
            QMessageBox.warning(self, '警告', '请先加载数据文件')
            return
            
        try:
            analysis = f"📊 数据分析报告\n"
            analysis += f"{'='*60}\n"
            analysis += f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            # 基本信息
            analysis += f"📋 基本信息:\n"
            analysis += f"  • 总行数: {len(self.df):,}\n"
            analysis += f"  • 总列数: {len(self.df.columns)}\n"
            analysis += f"  • 内存使用: {self.df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB\n\n"
            
            # 数据质量
            analysis += f"🔍 数据质量:\n"
            total_cells = len(self.df) * len(self.df.columns)
            null_cells = self.df.isnull().sum().sum()
            analysis += f"  • 空值比例: {null_cells/total_cells*100:.2f}% ({null_cells:,}/{total_cells:,})\n"
            analysis += f"  • 重复行数: {self.df.duplicated().sum():,}\n\n"
            
            # 列类型分布
            analysis += f"📊 列类型分布:\n"
            dtype_counts = self.df.dtypes.value_counts()
            for dtype, count in dtype_counts.items():
                analysis += f"  • {dtype}: {count} 列\n"
            analysis += "\n"
            
            # 数值列统计
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                analysis += f"📈 数值列统计 ({len(numeric_cols)} 列):\n"
                for col in numeric_cols:
                    stats = self.df[col].describe()
                    analysis += f"  • {col}:\n"
                    analysis += f"    - 均值: {stats['mean']:.2f}\n"
                    analysis += f"    - 中位数: {stats['50%']:.2f}\n"
                    analysis += f"    - 标准差: {stats['std']:.2f}\n"
                    analysis += f"    - 范围: [{stats['min']:.2f}, {stats['max']:.2f}]\n"
                analysis += "\n"
            
            # 分类列统计
            categorical_cols = self.df.select_dtypes(include=['object']).columns
            if len(categorical_cols) > 0:
                analysis += f"📝 分类列统计 ({len(categorical_cols)} 列):\n"
                for col in categorical_cols[:5]:  # 只显示前5列
                    unique_count = self.df[col].nunique()
                    most_common = self.df[col].value_counts().head(3)
                    analysis += f"  • {col}:\n"
                    analysis += f"    - 唯一值数量: {unique_count}\n"
                    analysis += f"    - 最常见值: {most_common.index[0]} ({most_common.iloc[0]} 次)\n"
                analysis += "\n"
            
            # 建议
            analysis += f"💡 数据处理建议:\n"
            if null_cells > 0:
                analysis += f"  • 考虑处理 {null_cells:,} 个空值\n"
            if self.df.duplicated().sum() > 0:
                analysis += f"  • 考虑删除 {self.df.duplicated().sum():,} 行重复数据\n"
            if len(numeric_cols) >= 2:
                analysis += f"  • 可以进行相关性分析和回归分析\n"
            if len(categorical_cols) > 0:
                analysis += f"  • 可以进行分组统计和交叉分析\n"
                
            self.analysis_text.setText(analysis)
            self.tab_widget.setCurrentIndex(3)  # 切换到分析标签页
            
        except Exception as e:
            QMessageBox.critical(self, '错误', f'生成分析报告失败:\n{str(e)}')
        
    def export_results(self):
        """导出查询结果"""
        if not hasattr(self, 'query_result') or self.query_result is None:
            QMessageBox.warning(self, '警告', '没有可导出的查询结果')
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, '保存查询结果', '', 
            'CSV文件 (*.csv);;Excel文件 (*.xlsx)'
        )
        
        if file_path:
            try:
                if file_path.lower().endswith('.csv'):
                    self.query_result.to_csv(file_path, index=False, encoding='utf-8-sig')
                elif file_path.lower().endswith('.xlsx'):
                    self.query_result.to_excel(file_path, index=False)
                    
                QMessageBox.information(self, '成功', f'结果已导出到:\n{file_path}')
                
            except Exception as e:
                QMessageBox.critical(self, '错误', f'导出失败:\n{str(e)}')
    
    def show_help(self):
        """显示SQLite函数帮助对话框"""
        help_dialog = QDialog(self)
        help_dialog.setWindowTitle('SQLite 函数帮助')
        help_dialog.setModal(True)
        help_dialog.resize(800, 600)
        
        layout = QVBoxLayout(help_dialog)
        
        # 创建标签页
        tab_widget = QTabWidget()
        
        # 聚合函数
        aggregate_tab = QTextEdit()
        aggregate_tab.setReadOnly(True)
        aggregate_content = """
<h3>聚合函数 (Aggregate Functions)</h3><br>

<b>COUNT()</b> - 计算行数<br>
• COUNT(*) - 计算所有行数<br>
• COUNT(column) - 计算非NULL值的行数<br>
• COUNT(DISTINCT column) - 计算不重复值的行数<br>
示例: SELECT COUNT(*) FROM data_table;<br><br>

<b>SUM()</b> - 求和<br>
• SUM(column) - 计算数值列的总和<br>
示例: SELECT SUM(salary) FROM data_table;<br><br>

<b>AVG()</b> - 平均值<br>
• AVG(column) - 计算数值列的平均值<br>
示例: SELECT AVG(age) FROM data_table;<br><br>

<b>MIN()</b> - 最小值<br>
• MIN(column) - 找到列中的最小值<br>
示例: SELECT MIN(price) FROM data_table;<br><br>

<b>MAX()</b> - 最大值<br>
• MAX(column) - 找到列中的最大值<br>
示例: SELECT MAX(score) FROM data_table;<br><br>

<b>GROUP_CONCAT()</b> - 字符串连接<br>
• GROUP_CONCAT(column, separator) - 将组内的值连接成字符串<br>
示例: SELECT GROUP_CONCAT(name, ', ') FROM data_table GROUP BY department;<br>
"""
        aggregate_tab.setHtml(aggregate_content)
        tab_widget.addTab(aggregate_tab, "聚合函数")
        
        # 字符串函数
        string_tab = QTextEdit()
        string_tab.setReadOnly(True)
        string_content = """
<h3>字符串函数 (String Functions)</h3><br>

<b>LENGTH()</b> - 字符串长度<br>
• LENGTH(string) - 返回字符串的字符数<br>
示例: SELECT LENGTH(name) FROM data_table;<br><br>

<b>UPPER()</b> - 转大写<br>
• UPPER(string) - 将字符串转换为大写<br>
示例: SELECT UPPER(name) FROM data_table;<br><br>

<b>LOWER()</b> - 转小写<br>
• LOWER(string) - 将字符串转换为小写<br>
示例: SELECT LOWER(email) FROM data_table;<br><br>

<b>SUBSTR()</b> - 子字符串<br>
• SUBSTR(string, start, length) - 提取子字符串<br>
示例: SELECT SUBSTR(phone, 1, 3) FROM data_table;<br><br>

<b>TRIM()</b> - 去除空格<br>
• TRIM(string) - 去除首尾空格<br>
• LTRIM(string) - 去除左侧空格<br>
• RTRIM(string) - 去除右侧空格<br>
示例: SELECT TRIM(name) FROM data_table;<br><br>

<b>REPLACE()</b> - 替换字符串<br>
• REPLACE(string, old, new) - 替换字符串中的内容<br>
示例: SELECT REPLACE(phone, '-', '') FROM data_table;<br><br>

<b>LIKE</b> - 模式匹配<br>
• column LIKE pattern - 使用通配符匹配<br>
• % 匹配任意字符序列<br>
• _ 匹配单个字符<br>
示例: SELECT * FROM data_table WHERE name LIKE '张%';<br>
"""
        string_tab.setHtml(string_content)
        tab_widget.addTab(string_tab, "字符串函数")
        
        # 数学函数
        math_tab = QTextEdit()
        math_tab.setReadOnly(True)
        math_content = """
<h3>数学函数 (Math Functions)</h3><br>

<b>ABS()</b> - 绝对值<br>
• ABS(number) - 返回数字的绝对值<br>
示例: SELECT ABS(profit) FROM data_table;<br><br>

<b>ROUND()</b> - 四舍五入<br>
• ROUND(number, digits) - 四舍五入到指定小数位<br>
示例: SELECT ROUND(price, 2) FROM data_table;<br><br>

<b>CEIL()</b> - 向上取整<br>
• CEIL(number) - 返回大于等于该数的最小整数<br>
示例: SELECT CEIL(score/10.0) FROM data_table;<br><br>

<b>FLOOR()</b> - 向下取整<br>
• FLOOR(number) - 返回小于等于该数的最大整数<br>
示例: SELECT FLOOR(price) FROM data_table;<br><br>

<b>RANDOM()</b> - 随机数<br>
• RANDOM() - 返回随机整数<br>
示例: SELECT * FROM data_table ORDER BY RANDOM() LIMIT 10;<br><br>

<b>POWER()</b> - 幂运算<br>
• POWER(base, exponent) - 计算base的exponent次方<br>
示例: SELECT POWER(2, 3); -- 结果为8<br><br>

<b>SQRT()</b> - 平方根<br>
• SQRT(number) - 计算平方根<br>
示例: SELECT SQRT(area) FROM data_table;<br>
"""
        math_tab.setHtml(math_content)
        tab_widget.addTab(math_tab, "数学函数")
        
        # 日期时间函数
        datetime_tab = QTextEdit()
        datetime_tab.setReadOnly(True)
        datetime_content = """
<h3>日期时间函数 (Date/Time Functions)</h3><br>

<b>DATE()</b> - 提取日期<br>
• DATE(datetime) - 从日期时间中提取日期部分<br>
示例: SELECT DATE(created_at) FROM data_table;<br><br>

<b>TIME()</b> - 提取时间<br>
• TIME(datetime) - 从日期时间中提取时间部分<br>
示例: SELECT TIME(created_at) FROM data_table;<br><br>

<b>DATETIME()</b> - 日期时间格式化<br>
• DATETIME(date_string) - 将字符串转换为日期时间<br>
示例: SELECT DATETIME('2023-01-01 12:00:00');<br><br>

<b>STRFTIME()</b> - 格式化日期<br>
• STRFTIME(format, datetime) - 按指定格式格式化日期<br>
• %Y - 四位年份, %m - 月份, %d - 日期<br>
• %H - 小时, %M - 分钟, %S - 秒<br>
示例: SELECT STRFTIME('%Y-%m', date_column) FROM data_table;<br><br>

<b>JULIANDAY()</b> - 儒略日<br>
• JULIANDAY(date) - 转换为儒略日数字<br>
示例: SELECT JULIANDAY('now') - JULIANDAY(birth_date) AS days_lived;<br><br>

<b>NOW/CURRENT_TIMESTAMP</b> - 当前时间<br>
• datetime('now') - 当前UTC时间<br>
• datetime('now', 'localtime') - 当前本地时间<br>
示例: SELECT datetime('now', 'localtime');<br>
"""
        datetime_tab.setHtml(datetime_content)
        tab_widget.addTab(datetime_tab, "日期时间函数")
        
        # 条件函数
        conditional_tab = QTextEdit()
        conditional_tab.setReadOnly(True)
        conditional_content = """
<h3>条件函数 (Conditional Functions)</h3><br>

<b>CASE WHEN</b> - 条件判断<br>
• CASE WHEN condition THEN result ELSE default END<br>
示例: <br>
SELECT name,<br>
       CASE WHEN age < 18 THEN '未成年'<br>
            WHEN age < 60 THEN '成年'<br>
            ELSE '老年' END AS age_group<br>
FROM data_table;<br><br>

<b>IFNULL()</b> - 空值处理<br>
• IFNULL(value, replacement) - 如果值为NULL则返回替换值<br>
示例: SELECT IFNULL(phone, '未提供') FROM data_table;<br><br>

<b>NULLIF()</b> - 相等时返回NULL<br>
• NULLIF(value1, value2) - 如果两值相等则返回NULL<br>
示例: SELECT NULLIF(score, 0) FROM data_table;<br><br>

<b>COALESCE()</b> - 返回第一个非NULL值<br>
• COALESCE(value1, value2, ...) - 返回第一个非NULL的值<br>
示例: SELECT COALESCE(mobile, phone, '无联系方式') FROM data_table;<br>
"""
        conditional_tab.setHtml(conditional_content)
        tab_widget.addTab(conditional_tab, "条件函数")
        
        # 窗口函数
        window_tab = QTextEdit()
        window_tab.setReadOnly(True)
        window_content = """
<h3>窗口函数 (Window Functions)</h3><br>

<b>ROW_NUMBER()</b> - 行号<br>
• ROW_NUMBER() OVER (ORDER BY column) - 为每行分配唯一行号<br>
示例: SELECT *, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank FROM data_table;<br><br>

<b>RANK()</b> - 排名<br>
• RANK() OVER (ORDER BY column) - 计算排名(相同值有相同排名)<br>
示例: SELECT *, RANK() OVER (ORDER BY score DESC) as rank FROM data_table;<br><br>

<b>DENSE_RANK()</b> - 密集排名<br>
• DENSE_RANK() OVER (ORDER BY column) - 密集排名(无间隙)<br>
示例: SELECT *, DENSE_RANK() OVER (ORDER BY grade DESC) as rank FROM data_table;<br><br>

<b>LAG()/LEAD()</b> - 前后行值<br>
• LAG(column, offset) OVER (ORDER BY column) - 获取前面行的值<br>
• LEAD(column, offset) OVER (ORDER BY column) - 获取后面行的值<br>
示例: SELECT date, sales, LAG(sales) OVER (ORDER BY date) as prev_sales FROM data_table;<br><br>

<b>PARTITION BY</b> - 分组窗口<br>
• 在窗口函数中使用PARTITION BY进行分组<br>
示例: SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) FROM data_table;<br>
"""
        window_tab.setHtml(window_content)
        tab_widget.addTab(window_tab, "窗口函数")
        
        layout.addWidget(tab_widget)
        
        # 关闭按钮
        close_btn = QPushButton('关闭')
        close_btn.clicked.connect(help_dialog.accept)
        layout.addWidget(close_btn)
        
        help_dialog.exec_()
