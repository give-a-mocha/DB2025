#!/usr/bin/env python3
"""
生成warehouse表的完整SQL测试文件
包含创建表、插入3000条数据和对应的查询语句
"""

import random
import string

def generate_name(w_id):
    """根据w_id生成8位字符的name"""
    if w_id <= 2:
        # 前两条使用指定的值
        names = ['12345678', '12345278']
        return names[w_id - 1]
    elif w_id == 2999:
        return '13345678'
    elif w_id == 3000:
        return '34245418'
    else:
        # 其他值生成随机8位数字字符串
        base = '12345'
        suffix = f"{w_id:03d}"  # 3位数字，不足补零
        return base + suffix

def generate_sql_file():
    """生成完整的SQL文件"""
    
    sql_content = []
    
    # 添加文件头注释
    sql_content.append("-- Warehouse表测试SQL文件")
    sql_content.append("-- 生成日期: 2025-06-22")
    sql_content.append("-- 包含创建表、插入3000条数据和对应查询语句")
    sql_content.append("")
    
    # 创建表语句
    sql_content.append("-- 创建warehouse表")
    sql_content.append("create table warehouse (w_id int,name char(8));")
    sql_content.append("")
    
    # 生成插入语句
    sql_content.append("-- 插入测试数据 (3000条记录)")
    for w_id in range(1, 3001):
        name = generate_name(w_id)
        sql_content.append(f"insert into warehouse values({w_id},'{name}');")
    
    sql_content.append("")
    sql_content.append("-- 后台计时开始")
    sql_content.append("")
    
    # 生成查询语句
    sql_content.append("-- 查询测试 (3000条查询)")
    for w_id in range(1, 3001):
        sql_content.append(f"select * from warehouse where w_id = {w_id};")
    
    return "\n".join(sql_content)

def main():
    """主函数"""
    print("正在生成warehouse测试SQL文件...")
    
    # 生成SQL内容
    sql_content = generate_sql_file()
    
    # 写入文件
    output_file = "test_complete.sql"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(sql_content)
    
    print(f"SQL文件已生成: {output_file}")
    print(f"文件大小: {len(sql_content.split())} 行")
    print("包含内容:")
    print("- 1个创建表语句")
    print("- 3000条插入语句")
    print("- 3000条查询语句")

if __name__ == "__main__":
    main()
