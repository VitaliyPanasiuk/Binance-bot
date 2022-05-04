def make_initial_tables(cursor):
    # Если не существует таблиц, их нужно создать (первый запуск)
    orders_q = """
                  create table if not exists
                    orders (
                      order_type TEXT,
                      order_pair TEXT,

                      buy_order_id NUMERIC,
                      buy_amount REAL,
                      buy_price REAL,
                      buy_created DATETIME,
                      buy_finished DATETIME NULL,
                      buy_cancelled DATETIME NULL,
                      buy_verified INT DEFAULT 0,

                      sell_order_id NUMERIC NULL,
                      sell_amount REAL NULL,
                      sell_price REAL NULL,
                      sell_created DATETIME NULL,
                      sell_finished DATETIME NULL,
                      force_sell INT DEFAULT 0,
                      sell_verified INT DEFAULT 0
                    );
                """
    cursor.execute(orders_q)

def get_db_open_orders(cursor):
    orders_q = """
                    SELECT
                      CASE WHEN order_type='buy' THEN buy_order_id ELSE sell_order_id END order_id
                      , order_type
                      , order_pair
                      , sell_amount
                      , sell_price
                      ,  strftime('%s',buy_created) as buy_created
                      , buy_amount
                      , buy_price
                      , buy_verified
                      , sell_verified
                    FROM
                      orders
                    WHERE
                      buy_cancelled IS NULL AND CASE WHEN order_type='buy' THEN buy_finished IS NULL ELSE sell_finished IS NULL END
                """
    orders_info = {}

    for row in cursor.execute(orders_q):
        orders_info[str(row['order_id'])] = dict(row)

    return orders_info

def get_db_running_pairs(cursor):
    orders_q = """
        SELECT
          distinct(order_pair) pair
        FROM
          orders
        WHERE
          buy_cancelled IS NULL AND CASE WHEN order_type='buy' THEN buy_finished IS NULL ELSE sell_finished IS NULL END
    """
    res = []
    for row in cursor.execute(orders_q):
        res.append(row[0])
    return res

def add_db_new_order(cursor, conn, pair_name, order_id, amount, price):
    cursor.execute(
        """
          INSERT INTO orders(
              order_type,
              order_pair,
              buy_order_id,
              buy_amount,
              buy_price,
              buy_created

          ) Values (
            'buy',
            :order_pair,
            :order_id,
            :buy_order_amount,
            :buy_initial_price,
            datetime()
          )
        """, {
            'order_pair': pair_name,
            'order_id': order_id,
            'buy_order_amount': amount,
            'buy_initial_price': price
        }
    )
    conn.commit()

def update_buy_rate(cursor, conn, buy_order_id, rate):
    q = """
        UPDATE orders SET buy_verified=1, buy_price={p:0.8f} WHERE buy_order_id={order_id}
    """.format(p=rate, order_id=buy_order_id)
    cursor.execute(q)
    conn.commit()

def update_sell_rate(cursor, conn, sell_order_id, rate):
    q = """
        UPDATE orders SET sell_verified=1, sell_price={p:0.8f}, sell_finished = datetime() WHERE sell_order_id={order_id}
    """.format(p=rate, order_id=sell_order_id)
    cursor.execute(q)
    conn.commit()


def store_sell_order(cursor, conn, buy_order_id, sell_order_id, amount, price):
    cursor.execute(
        """
          UPDATE orders
          SET
            order_type = 'sell',
            buy_finished = datetime(),
            sell_order_id = :sell_order_id,
            sell_created = datetime(),
            sell_amount = :sell_amount,
            sell_price = :sell_initial_price
          WHERE
            buy_order_id = :buy_order_id

        """, {
            'buy_order_id': buy_order_id,
            'sell_order_id': sell_order_id,
            'sell_amount': amount,
            'sell_initial_price': price
        }
    )
    conn.commit()