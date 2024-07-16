import sqlite3
import json
from datetime import datetime

conn = sqlite3.connect('proper_backend.db')
cursor = conn.cursor()


def create_user(username, email, balance=0.0, positions='{}'):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('trading_system.db')
        
        
        # Insert a new user into the Users table
        cursor.execute('''
        INSERT INTO Users (username, email, balance, positions)
        VALUES (?, ?, ?, ?)
        ''', (username, email, balance, positions))
        
        # Commit the transaction
        conn.commit()
        
        print(f"User {username} created successfully.")
    
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the connection
        conn.close()


#Order Matching algorithm prototype 

def fetch_highest_buy_order():
    cursor.execute("SELECT * FROM BuyOrders ORDER BY price DESC, order_date ASC LIMIT 1")
    return cursor.fetchone()
    #ASC Limit 1 -> Earliest Order is prioritzed incase the price is the same

def fetch_lowest_sell_order():
    cursor.execute("SELECT * FROM SellOrders ORDER BY price ASC, order_date ASC LIMIT 1")
    return cursor.fetchone()
    #ASC Limit 1 -> Earliest Order is priotizied incase the price is the same 

def update_user_balance(user_id, amount):
    cursor.execute("UPDATE Users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()



def add_transaction(buyer_id, seller_id, stock_symbol, quantity, price):
    cursor.execute("INSERT INTO Transactions (buyer_id, seller_id, quantity, price, transaction_date) VALUES (?, ?, ?, ?, ?, ?)",
                   (buyer_id, seller_id, quantity, price, datetime.now()))
    conn.commit()

def remove_order(order_type, order_id):
    if order_type == 'buy':
        cursor.execute("DELETE FROM BuyOrders WHERE buy_order_id = ?", (order_id,))
    elif order_type == 'sell':
        cursor.execute("DELETE FROM SellOrders WHERE sell_order_id = ?", (order_id,))
    conn.commit()


#Mock Ordermatching algorithm, Highest buy_order price = highest priority
#Lowest sell_rder price = highest prriority

#If price is the same, then order date -> earlier is prioritized


def execute_trade():
    while True:
        buy_order = fetch_highest_buy_order()
        sell_order = fetch_lowest_sell_order()

        if not buy_order or not sell_order:
            print("No more orders to match.")
            break

        buy_order_id, buy_user_id, buy_quantity, buy_price, _ = buy_order
        sell_order_id, sell_user_id, sell_quantity, sell_price, _ = sell_order

        if buy_price >= sell_price:
            trade_quantity = min(buy_quantity, sell_quantity)
            trade_price = sell_price

            update_user_balance(buy_user_id, -trade_quantity * trade_price)
            update_user_balance(sell_user_id, trade_quantity * trade_price)

            add_transaction(buy_user_id, sell_user_id, trade_quantity, trade_price)

            if buy_quantity > sell_quantity:
                cursor.execute("UPDATE BuyOrders SET quantity = quantity - ? WHERE buy_order_id = ?", (trade_quantity, buy_order_id))
                remove_order('sell', sell_order_id)
            elif sell_quantity > buy_quantity:
                cursor.execute("UPDATE SellOrders SET quantity = quantity - ? WHERE sell_order_id = ?", (trade_quantity, sell_order_id))
                remove_order('buy', buy_order_id)
            else:
                remove_order('buy', buy_order_id)
                remove_order('sell', sell_order_id)

            conn.commit()
        else:
            print("No matching buy and sell orders.")
            break



# Close the database connection
conn.close()