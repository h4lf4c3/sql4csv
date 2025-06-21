import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QMessageBox, QComboBox
)
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

class ChartWidget(QWidget):
    """图表显示组件"""

    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # 图表控制面板
        control_panel = QWidget()
        control_layout = QHBoxLayout(control_panel)

        # 图表类型选择
        control_layout.addWidget(QLabel('图表类型:'))
        self.chart_type_combo = QComboBox()
        self.chart_type_combo.addItems(['柱状图', '折线图', '散点图', '饼图', '直方图', '箱线图', '热力图'])
        control_layout.addWidget(self.chart_type_combo)

        # X轴选择
        control_layout.addWidget(QLabel('X轴:'))
        self.x_axis_combo = QComboBox()
        control_layout.addWidget(self.x_axis_combo)

        # Y轴选择
        control_layout.addWidget(QLabel('Y轴:'))
        self.y_axis_combo = QComboBox()
        control_layout.addWidget(self.y_axis_combo)

        # 生成图表按钮
        self.generate_btn = QPushButton('生成图表')
        self.generate_btn.clicked.connect(self.generate_chart)
        control_layout.addWidget(self.generate_btn)

        layout.addWidget(control_panel)

        # 图表显示区域
        self.figure = Figure(figsize=(10, 6))
        self.canvas = FigureCanvas(self.figure)
        layout.addWidget(self.canvas)

        self.data = None

    def update_data(self, dataframe):
        """更新数据并刷新列选择"""
        self.data = dataframe

        # 清空并更新列选择
        self.x_axis_combo.clear()
        self.y_axis_combo.clear()

        if dataframe is not None and not dataframe.empty:
            columns = list(dataframe.columns)
            self.x_axis_combo.addItems([''] + columns)
            self.y_axis_combo.addItems([''] + columns)

    def generate_chart(self):
        """生成图表"""
        if self.data is None or self.data.empty:
            QMessageBox.warning(self, '警告', '没有可用数据')
            return

        chart_type = self.chart_type_combo.currentText()
        x_col = self.x_axis_combo.currentText()
        y_col = self.y_axis_combo.currentText()

        if not x_col and chart_type not in ['直方图']:
            QMessageBox.warning(self, '警告', '请选择X轴列')
            return

        try:
            self.figure.clear()
            ax = self.figure.add_subplot(111)

            # 设置中文字体
            plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
            plt.rcParams['axes.unicode_minus'] = False

            if chart_type == '柱状图':
                if y_col:
                    # 数值型柱状图
                    self.data.groupby(x_col)[y_col].sum().plot(kind='bar', ax=ax)
                else:
                    # 计数柱状图
                    self.data[x_col].value_counts().plot(kind='bar', ax=ax)
                ax.set_title(f'{x_col} 柱状图')

            elif chart_type == '折线图':
                if y_col:
                    ax.plot(self.data[x_col], self.data[y_col], marker='o')
                    ax.set_xlabel(x_col)
                    ax.set_ylabel(y_col)
                ax.set_title(f'{x_col} vs {y_col} 折线图')

            elif chart_type == '散点图':
                if y_col:
                    ax.scatter(self.data[x_col], self.data[y_col], alpha=0.6)
                    ax.set_xlabel(x_col)
                    ax.set_ylabel(y_col)
                ax.set_title(f'{x_col} vs {y_col} 散点图')

            elif chart_type == '饼图':
                if y_col:
                    data_grouped = self.data.groupby(x_col)[y_col].sum()
                else:
                    data_grouped = self.data[x_col].value_counts()
                ax.pie(data_grouped.values, labels=data_grouped.index, autopct='%1.1f%%')
                ax.set_title(f'{x_col} 饼图')

            elif chart_type == '直方图':
                col = y_col if y_col else x_col
                if col and pd.api.types.is_numeric_dtype(self.data[col]):
                    ax.hist(self.data[col].dropna(), bins=20, alpha=0.7)
                    ax.set_xlabel(col)
                    ax.set_ylabel('频次')
                    ax.set_title(f'{col} 直方图')

            elif chart_type == '箱线图':
                if y_col and pd.api.types.is_numeric_dtype(self.data[y_col]):
                    if x_col:
                        # 分组箱线图
                        groups = [group[y_col].dropna() for name, group in self.data.groupby(x_col)]
                        ax.boxplot(groups, labels=self.data[x_col].unique())
                    else:
                        # 单列箱线图
                        ax.boxplot(self.data[y_col].dropna())
                    ax.set_title(f'{y_col} 箱线图')

            elif chart_type == '热力图':
                # 选择数值列
                numeric_cols = self.data.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) >= 2:
                    corr_matrix = self.data[numeric_cols].corr()
                    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', ax=ax)
                    ax.set_title('相关性热力图')

            plt.tight_layout()
            self.canvas.draw()

        except Exception as e:
            QMessageBox.critical(self, '错误', f'生成图表失败:\n{str(e)}')