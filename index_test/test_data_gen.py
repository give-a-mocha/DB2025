import random
import string
from datetime import datetime, timedelta

def generate_random_string(length):
    """Generates a random string of specified length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_char(length):
    """Generates a random string of specified length containing only letters."""
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_random_zip():
    """Generates a random zip code."""
    return ''.join(random.choices(string.digits, k=9))

def generate_random_phone():
    """Generates a random phone number."""
    return ''.join(random.choices(string.digits, k=16))

def generate_random_date_char():
    """Generates a random date string in 'YYYY-MM-DD HH:MM:SS' format."""
    year = random.randint(2000, 2023)
    month = random.randint(1, 12)
    day = random.randint(1, 28) # To avoid issues with different month lengths
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"

def generate_random_date_char_long():
    """Generates a random date string in 'YYYY-MM-DD HH:MM:SS.ffffff' format."""
    year = random.randint(2000, 2023)
    month = random.randint(1, 12)
    day = random.randint(1, 28) # To avoid issues with different month lengths
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    microsecond = random.randint(0, 999999)
    return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}.{microsecond:06d}"


def generate_sql_statements(num_warehouses=2, num_districts_per_warehouse=5, num_customers_per_district=20, num_items=200, num_stock_per_warehouse=200, num_transactions=50):
    sql_statements = []

    # Create Table Statements
    sql_statements.append("create table warehouse (w_id int, w_name char(10), w_street_1 char(20), w_street_2 char(20), w_city char(20), w_state char(2), w_zip char(9), w_tax float, w_ytd float);\n")
    sql_statements.append("create table district (d_id int, d_w_id int, d_name char(10), d_street_1 char(20), d_street_2 char(20), d_city char(20), d_state char(2), d_zip char(9), d_tax float, d_ytd float, d_next_o_id int);\n")
    sql_statements.append("create table customer (c_id int, c_d_id int, c_w_id int, c_first char(16), c_middle char(2), c_last char(16), c_street_1 char(20), c_street_2 char(20), c_city char(20), c_state char(2), c_zip char(9), c_phone char(16), c_since char(30), c_credit char(2), c_credit_lim int, c_discount float, c_balance float, c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, c_data char(50));\n")
    sql_statements.append("create table history (h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int, h_w_id int, h_date char(19), h_amount float, h_data char(24));\n")
    sql_statements.append("create table new_orders (no_o_id int, no_d_id int, no_w_id int);\n")
    sql_statements.append("create table orders (o_id int, o_d_id int, o_w_id int, o_c_id int, o_entry_d char(19), o_carrier_id int, o_ol_cnt int, o_all_local int);\n")
    sql_statements.append("create table order_line ( ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, ol_i_id int, ol_supply_w_id int, ol_delivery_d char(30), ol_quantity int, ol_amount float, ol_dist_info char(24));\n")
    sql_statements.append("create table item (i_id int, i_im_id int, i_name char(24), i_price float, i_data char(50));\n")
    sql_statements.append("create table stock (s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(24), s_dist_02 char(24), s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd float, s_order_cnt int, s_remote_cnt int, s_data char(50));\n")

    # Insert Data Statements

    # warehouse
    for w_id in range(1, num_warehouses + 1):
        sql_statements.append(f"insert into warehouse values({w_id}, '{generate_random_char(10)}', '{generate_random_string(20)}', '{generate_random_string(20)}', '{generate_random_string(20)}', '{generate_random_char(2)}', '{generate_random_zip()}', {round(random.uniform(0, 0.2), 2)}, {round(random.uniform(1000, 10000), 2)});\n")

        # district
        for d_id in range(1, num_districts_per_warehouse + 1):
            sql_statements.append(f"insert into district values({d_id}, {w_id}, '{generate_random_char(10)}', '{generate_random_string(20)}', '{generate_random_string(20)}', '{generate_random_string(20)}', '{generate_random_char(2)}', '{generate_random_zip()}', {round(random.uniform(0, 0.2), 2)}, {round(random.uniform(1000, 10000), 2)}, 1);\n") # d_next_o_id starts at 1

            # customer
            for c_id in range(1, num_customers_per_district + 1):
                sql_statements.append(f"insert into customer values({c_id}, {d_id}, {w_id}, '{generate_random_char(16)}', '{generate_random_char(2)}', '{generate_random_char(16)}', '{generate_random_string(20)}', '{generate_random_string(20)}', '{generate_random_string(20)}', '{generate_random_char(2)}', '{generate_random_zip()}', '{generate_random_phone()}', '{generate_random_date_char_long()}', '{random.choice(['GC', 'BC'])}', {random.randint(1000, 10000)}, {round(random.uniform(0, 0.5), 2)}, {round(random.uniform(-500, 500), 2)}, {round(random.uniform(0, 1000), 2)}, {random.randint(0, 10)}, {random.randint(0, 10)}, '{generate_random_string(50)}');\n")

    # item
    for i_id in range(1, num_items + 1):
        sql_statements.append(f"insert into item values({i_id}, {random.randint(1, 10000)}, '{generate_random_char(24)}', {round(random.uniform(1, 100), 2)}, '{generate_random_string(50)}');\n")

    # stock
    for w_id in range(1, num_warehouses + 1):
        for i_id in range(1, num_stock_per_warehouse + 1):
             sql_statements.append(f"insert into stock values({i_id}, {w_id}, {random.randint(10, 100)}, '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', '{generate_random_string(24)}', {round(random.uniform(0, 1000), 2)}, {random.randint(0, 10)}, {random.randint(0, 10)}, '{generate_random_string(50)}');\n")


    # Transaction Statements
    for _ in range(num_transactions):
        w_id = random.randint(1, num_warehouses)
        d_id = random.randint(1, num_districts_per_warehouse)
        c_id = random.randint(1, num_customers_per_district)
        i_id = random.randint(1, num_items)
        o_id = random.randint(1, 1000) # Example order ID range
        carrier_id = random.randint(1, 10) # Example carrier ID range
        ol_quantity = random.randint(1, 10) # Example order line quantity
        ol_amount = round(random.uniform(1, 500), 2) # Example order line amount
        entry_d = generate_random_date_char()
        delivery_d = generate_random_date_char_long()
        dist_info = generate_random_string(24)


        sql_statements.append("begin;\n")
        sql_statements.append(f"select c_discount, c_last, c_credit, w_tax from customer, warehouse where w_id={w_id} and c_w_id=w_id and c_d_id={d_id} and c_id={c_id};\n")
        sql_statements.append(f"select d_next_o_id, d_tax from district where d_id={d_id} and d_w_id={w_id};\n")
        sql_statements.append(f"update district set d_next_o_id={o_id + 1} where d_id={d_id} and d_w_id={w_id};\n") # Increment d_next_o_id
        sql_statements.append(f"insert into orders values ({o_id}, {d_id}, {w_id}, {c_id}, '{entry_d}', {carrier_id}, {ol_quantity}, 1);\n")
        sql_statements.append(f"insert into new_orders values ({o_id}, {d_id}, {w_id});\n")
        sql_statements.append(f"select i_price, i_name, i_data from item where i_id={i_id};\n")
        sql_statements.append(f"select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id={i_id} and s_w_id={w_id};\n")
        sql_statements.append(f"update stock set s_quantity=s_quantity - {ol_quantity} where s_i_id={i_id} and s_w_id={w_id};\n") # Decrease stock quantity
        sql_statements.append(f"insert into order_line values ({o_id}, {d_id}, {w_id}, 1, {i_id}, {w_id}, '{delivery_d}', {ol_quantity}, {ol_amount}, '{dist_info}');\n")
        sql_statements.append(f"select i_price, i_name, i_data from item where i_id={i_id};\n")
        sql_statements.append("commit;\n")

    return sql_statements

def main():
    output_file = "test_data.sql"
    sql_statements = generate_sql_statements(num_warehouses=2, num_districts_per_warehouse=5, num_customers_per_district=20, num_items=200, num_stock_per_warehouse=200, num_transactions=50)

    with open(output_file, "w") as file:
        for statement in sql_statements:
            file.write(statement)

    print(f"Generated SQL statements written to {output_file}")

if __name__ == "__main__":
    main()