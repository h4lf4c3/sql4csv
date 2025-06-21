import sys
from PyQt5.QtWidgets import QApplication
from editor_view import AdvancedCSVSQLEditor


def main():
    app = QApplication(sys.argv)
    
    # 设置应用样式
    app.setStyle('Fusion')
    
    # 设置应用图标和信息
    app.setApplicationName('高级CSV/Excel SQL查询分析工具')
    app.setApplicationVersion('2.0')
    
    window = AdvancedCSVSQLEditor()
    window.show()
    
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()