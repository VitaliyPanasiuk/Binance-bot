from pickle import TRUE
import sqlite3

import time
import threading

import binance_bot.indicators as ta

from config import (bot, pairs, log, TIMEFRAME, KLINES_LIMITS, POINTS_TO_ENTER, USE_OPEN_CANDLES, WINDOWS)
from misc import adjust_to_step, sync_time, calc_buy_avg_rate, calc_sell_avg_rate, get_order_trades
from db_queries import (make_initial_tables, get_db_open_orders, get_db_running_pairs,
                        add_db_new_order, update_buy_rate, store_sell_order, update_sell_rate)

limits = bot.exchangeInfo()






def main_flow():
    while True:
        try:
            # Устанавливаем соединение с локальной базой данных
            conn = sqlite3.connect('binance.db')
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Если не существует таблиц, их нужно создать (первый запуск)
            make_initial_tables(cursor)

            log.debug("Получаем все неисполненные ордера по БД")
            orders_info = get_db_open_orders(cursor)

            # формируем словарь из указанных пар, для удобного доступа
            all_pairs = {pair['quote'].upper() + pair['base'].upper():pair for pair in pairs if pair['active']}

            if orders_info:
                log.debug("Получены неисполненные ордера из БД: {orders}".format(orders=[(order, orders_info[order]['order_pair']) for order in orders_info]))

                # Проверяем каждый неисполненный по базе ордер
                for order in orders_info:
                    # Получаем по ордеру последнюю информацию по бирже
                    stock_order_data = bot.orderInfo(symbol=orders_info[order]['order_pair'], orderId=order)

                    order_status = stock_order_data['status']
                    log.debug("Состояние ордера {order} - {status}".format(order=order, status=order_status))

                    # Если ордер на покупку
                    if orders_info[order]['order_type'] == 'buy':
                        if not orders_info[order]['buy_verified']:
                            # По ордеру не были получены сделки
                            order_trades = get_order_trades(
                                order_id=orders_info[order]['order_id'],
                                pair=orders_info[order]['order_pair'], bot=bot
                            )

                            avg_rate = calc_buy_avg_rate(order_trades, log)
                            if avg_rate > 0:
                                update_buy_rate(cursor, conn, orders_info[order]['order_id'], avg_rate)
                            else:
                                log.debug("Не удается вычислить цену покупки, пропуск")
                                continue

                        # Если ордер уже исполнен
                        if order_status == 'FILLED':
                            got_qty = float(stock_order_data['executedQty'])
                            log.info("""
                                Ордер {order} выполнен, получено {exec_qty:0.8f}.
                                Проверяем, не стоит ли создать ордер на продажу
                            """.format(
                                order=order, exec_qty=got_qty
                            ))

                            # смотрим, какие ограничения есть для создания ордера на продажу
                            for elem in limits['symbols']:
                                if elem['symbol'] == orders_info[order]['order_pair']:
                                    CURR_LIMITS = elem
                                    break
                            else:
                                raise Exception("Не удалось найти настройки выбранной пары " + pair_name)

                            got_qty = adjust_to_step(got_qty, CURR_LIMITS['filters'][2]['stepSize'])

                            prices = bot.tickerBookTicker(
                                symbol=orders_info[order]['order_pair']
                            )

                            # Берем цены покупок (нужно будет продавать по рынку)
                            curr_rate = float(prices['bidPrice'])

                            price_change = (curr_rate/orders_info[order]['buy_price']-1)*100
                            log.debug("Цена изменилась на {r:0.8f}%, процент для продажи {sp:0.8f}".format(r=price_change, sp=all_pairs[stock_order_data['symbol']]['profit_markup']))
                            
                            log.debug("Проверяем индикаторы")
                            # Получаем свечи и берем цены закрытия, high, low
                            klines = bot.klines(
                                symbol=pair_name.upper(),
                                interval=TIMEFRAME,
                                limit=KLINES_LIMITS
                            )
                            klines = klines[:len(klines)-int(not USE_OPEN_CANDLES)]

                            closes = [float(x[4]) for x in klines]
                            high = [float(x[2]) for x in klines]
                            low = [float(x[3]) for x in klines]

                            # Скользящая средняя
                            sma_5 = ta.SMA(closes, 5)
                            sma_100 = ta.SMA(closes, 100)
                            ema_5 = ta.EMA(closes, 5)
                            ema_100 = ta.EMA(closes, 100)

                            # Считаем, каким был бы stop-loss, если применить к нему % в зависимости от насколько изменилась цена от цены покупки
                            multiplier = -1
                            if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop2']:
                                curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss2']*multiplier+100)
                                if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop3']:
                                    curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss3']*multiplier+100)
                                    if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop4']:
                                        curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss4']*multiplier+100)
                                        if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop5']:
                                            curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss5']*multiplier+100)
                            else:
                                curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss']*multiplier+100)
                            
                            if price_change >= all_pairs[stock_order_data['symbol']]['profit_markup'] or ema_5[-1] < ema_100[-1] and sma_5[-1] < sma_100[-1]:

                                # Отправляем команду на создание ордера с рассчитанными параметрами
                                new_order = bot.createOrder(
                                    symbol=orders_info[order]['order_pair'],
                                    recvWindow=5000,
                                    side='SELL',
                                    type='MARKET',
                                    quantity="{quantity:0.{precision}f}".format(
                                        quantity=got_qty, precision=CURR_LIMITS['baseAssetPrecision']
                                    ),
                                    newOrderRespType='FULL'
                                )
                                # Если ордер создался без ошибок, записываем данные в базу данных
                                if 'orderId' in new_order:
                                    log.info("Создан ордер на продажу по рынку {new_order}".format(new_order=new_order))
                                    store_sell_order(cursor, conn, order, new_order['orderId'], got_qty, 0)

                                    order_trades = get_order_trades(
                                        order_id=new_order['orderId'],
                                        pair=pair_name, bot=bot
                                    )

                                    avg_rate = calc_sell_avg_rate(order_trades, log)
                                    if avg_rate > 0:
                                        update_sell_rate(cursor, conn, new_order['orderId'], avg_rate)
                                    else:
                                        log.debug("Не удается вычислить цену покупки, пропуск")
                                        continue

                                # Если были ошибки при создании, выводим сообщение
                                else:
                                    log.warning("Не удалось создать ордер на продажу {new_order}".format(new_order=new_order))
                            elif price_change <= curr_rate_applied:
                                new_order = bot.createOrder(
                                    symbol=orders_info[order]['order_pair'],
                                    recvWindow=5000,
                                    side='SELL',
                                    type='MARKET',
                                    quantity="{quantity:0.{precision}f}".format(
                                        quantity=got_qty, precision=CURR_LIMITS['baseAssetPrecision']
                                    ),
                                    newOrderRespType='FULL'
                                )
                                # Если ордер создался без ошибок, записываем данные в базу данных
                                if 'orderId' in new_order:
                                    log.info("Создан ордер на продажу по рынку {new_order}".format(new_order=new_order))
                                    store_sell_order(cursor, conn, order, new_order['orderId'], got_qty, 0)

                                    order_trades = get_order_trades(
                                        order_id=new_order['orderId'],
                                        pair=pair_name, bot=bot
                                    )

                                    avg_rate = calc_sell_avg_rate(order_trades, log)
                                    if avg_rate > 0:
                                        update_sell_rate(cursor, conn, new_order['orderId'], avg_rate)
                                    else:
                                        log.debug("Не удается вычислить цену покупки, пропуск")
                                        continue
                            else:
                                log.debug("Цена не изменилась до take-profit")

                    # Если это ордер на продажу, и он исполнен
                    if orders_info[order]['order_type'] == 'sell' and not orders_info[order]['sell_verified']:
                        multiplier = 1
                        order_trades = get_order_trades(
                            order_id=orders_info[order]['order_id'],
                            pair=orders_info[order]['order_pair'], bot=bot
                        )

                        avg_rate = calc_sell_avg_rate(order_trades, log)
                        if avg_rate > 0:
                            update_sell_rate(cursor, conn, orders_info[order]['order_id'], avg_rate)
                        else:
                            log.debug("Не удается вычислить цену покупки, пропуск")
                            continue

                    if all_pairs[orders_info[order]['order_pair']]['use_stop_loss']:
                        if order_status == 'FILLED' and orders_info[order]['order_type'] == 'buy':
                            curr_rate = float(bot.tickerPrice(symbol=orders_info[order]['order_pair'])['price'])
                            price_change = (curr_rate/orders_info[order]['buy_price'] - 1)*100

                        if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop2']:
                            curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss2']*multiplier+100)
                            if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop3']:
                                curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss3']*multiplier+100)
                                if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop4']:
                                    curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss4']*multiplier+100)
                                    if price_change >= all_pairs[stock_order_data['symbol']]['percent_for_stop5']:
                                        curr_rate_applied = (curr_rate/100) * (all_pairs[stock_order_data['symbol']]['stop_loss5']*multiplier+100)


                        if price_change >= all_pairs[stock_order_data['symbol']]['profit_markup'] or ema_5[-1] > ema_100[-1] and sma_5[-1] > sma_100[-1]:
                            # Отправляем команду на создание ордера с рассчитанными параметрами
                            new_order = bot.createOrder(
                                symbol=orders_info[order]['order_pair'],
                                recvWindow=5000,
                                side='BUY',
                                type='MARKET',
                                quantity="{quantity:0.{precision}f}".format(
                                    quantity=got_qty, precision=CURR_LIMITS['baseAssetPrecision']
                                ),
                                newOrderRespType='FULL'
                            )
                        elif price_change >= curr_rate_applied:
                            log.debug("{pair} Цена упала до стоплосс (покупали по {b:0.8f}, сейчас {s:0.8f}), пора продавать".format(
                               pair=orders_info[order]['order_pair'],
                               b=orders_info[order]['buy_price'],
                               s=curr_rate
                            ))

                            # Получаем лимиты пары с биржи
                            for elem in limits['symbols']:
                                if elem['symbol'] == orders_info[order]['order_pair']:
                                    CURR_LIMITS = elem
                                    break
                            else:
                                raise Exception("Не удалось найти настройки выбранной пары " + orders_info[order]['order_pair'])

                            new_order = bot.createOrder(
                                      symbol=orders_info[order]['order_pair'],
                                      recvWindow=15000,
                                      side='SELL',
                                      type='MARKET',
                                      quantity="{quantity:0.{precision}f}".format(
                                            quantity=orders_info[order]['buy_amount'], precision=CURR_LIMITS['baseAssetPrecision']
                                      ),
                                )

                            if 'orderId' in new_order:
                                log.info("Создан ордер на продажу по рынку {new_order}".format(new_order=new_order))
                                store_sell_order(cursor, conn, order, new_order['orderId'], got_qty, 0)

                                order_trades = get_order_trades(
                                    order_id=new_order['orderId'],
                                    pair=orders_info[order]['order_pair'], bot=bot
                                )

                                avg_rate = calc_sell_avg_rate(order_trades, log)
                                if avg_rate > 0:
                                    update_sell_rate(cursor, conn, new_order['orderId'], avg_rate)
                                else:
                                    log.debug("Не удается вычислить цену покупки, пропуск")
                                    continue
            else:
                log.debug("Неисполненных ордеров в БД нет")

            log.debug('Получаем из настроек все пары, по которым нет неисполненных ордеров')

            # Получаем из базы все ордера, по которым есть торги, и исключаем их из списка, по которому будем создавать новые ордера
            for row in get_db_running_pairs(cursor):
                del all_pairs[row]

            # Если остались пары, по которым нет текущих торгов
            if all_pairs:
                log.debug('Найдены пары, по которым нет неисполненных ордеров: {pairs}'.format(pairs=list(all_pairs.keys())))
                for pair_name, pair_obj in all_pairs.items():
                    try:
                        log.debug("Работаем с парой {pair}".format(pair=pair_name))

                        # Получаем лимиты пары с биржи
                        for elem in limits['symbols']:
                            if elem['symbol'] == pair_name:
                                CURR_LIMITS = elem
                                break
                        else:
                            raise Exception("Не удалось найти настройки выбранной пары " + pair_name)

                        log.debug("Проверяем индикаторы")
                        # Получаем свечи и берем цены закрытия, high, low
                        klines = bot.klines(
                            symbol=pair_name.upper(),
                            interval=TIMEFRAME,
                            limit=KLINES_LIMITS
                        )
                        klines = klines[:len(klines)-int(not USE_OPEN_CANDLES)]

                        closes = [float(x[4]) for x in klines]
                        high = [float(x[2]) for x in klines]
                        low = [float(x[3]) for x in klines]

                        # Скользящая средняя
                        sma_5 = ta.SMA(closes, 5)
                        sma_100 = ta.SMA(closes, 100)
                        ema_5 = ta.EMA(closes, 5)
                        ema_100 = ta.EMA(closes, 100)
                        rsi_9 = ta.RSI(closes, 9)
                        rsi_14 = ta.RSI(closes, 14)
                        rsi_21 = ta.RSI(closes, 21)
                        fast, slow = ta.STOCH(high, low, closes, 5, 3, 3)
                        fast, slow = ta.STOCHRSI(closes, 14, 3, 3)
                        upper, middle, lower = ta.BBANDS(closes, ma_period=21)
                        macd, macdsignal, macdhist = ta.MACD(closes, 12, 26, 9)

                        enter_points = 0
                        
                        time_window = "".join(c for c in WINDOWS['bbands'] if  c.isdecimal())
                        timeframe = "".join(c for c in TIMEFRAME if  c.isdecimal())
                        amount_of_repeat = int(time_window) / int(timeframe)

                        # Быстрая EMA выше медленной и быстрая SMA выше медленной, считаем, что можно входить
                        # идет проверка всех остальных индикаторов в заданом промежутке времени
                        # при совпадении всех будет открыта сделка
                        if ema_5[-1] > ema_100[-1] and sma_5[-1] > sma_100[-1]:
                            c = 0
                            while c < amount_of_repeat:
                                time.sleep()

                                log.debug("Проверяем индикаторы")
                                # Получаем свечи и берем цены закрытия, high, low
                                klines = bot.klines(
                                    symbol=pair_name.upper(),
                                    interval=TIMEFRAME,
                                    limit=KLINES_LIMITS
                                )
                                klines = klines[:len(klines)-int(not USE_OPEN_CANDLES)]

                                closes = [float(x[4]) for x in klines]
                                high = [float(x[2]) for x in klines]
                                low = [float(x[3]) for x in klines]

                                sma_5 = ta.SMA(closes, 5)
                                sma_100 = ta.SMA(closes, 100)
                                ema_5 = ta.EMA(closes, 5)
                                ema_100 = ta.EMA(closes, 100)
                                rsi_9 = ta.RSI(closes, 9)
                                rsi_14 = ta.RSI(closes, 14)
                                rsi_21 = ta.RSI(closes, 21)
                                fast, slow = ta.STOCH(high, low, closes, 5, 3, 3)
                                fast, slow = ta.STOCHRSI(closes, 14, 3, 3)
                                upper, middle, lower = ta.BBANDS(closes, ma_period=21)
                                macd, macdsignal, macdhist = ta.MACD(closes, 12, 26, 9)

                                if macd[-1] > macdsignal[-1] and macdhist[-1] > 0 and rsi_9[-1] < 70 and rsi_14[-1] < 70 and rsi_21[-1] < 70 and fast[-1] > slow[-1] and high[-1] > upper[-1]:
                                    enter_points = 1
                                    c = amount_of_repeat

                        # Линия макд выше сигнальной и на гистограмме они выше нуля
                        # идет проверка всех остальных индикаторов в заданом промежутке времени
                        # при совпадении всех будет открыта сделка
                        if macd[-1] > macdsignal[-1] and macdhist[-1] > 0:
                            c = 0
                            while c < amount_of_repeat:
                                time.sleep()

                                log.debug("Проверяем индикаторы")
                                # Получаем свечи и берем цены закрытия, high, low
                                klines = bot.klines(
                                    symbol=pair_name.upper(),
                                    interval=TIMEFRAME,
                                    limit=KLINES_LIMITS
                                )
                                klines = klines[:len(klines)-int(not USE_OPEN_CANDLES)]

                                closes = [float(x[4]) for x in klines]
                                high = [float(x[2]) for x in klines]
                                low = [float(x[3]) for x in klines]

                                sma_5 = ta.SMA(closes, 5)
                                sma_100 = ta.SMA(closes, 100)
                                ema_5 = ta.EMA(closes, 5)
                                ema_100 = ta.EMA(closes, 100)
                                rsi_9 = ta.RSI(closes, 9)
                                rsi_14 = ta.RSI(closes, 14)
                                rsi_21 = ta.RSI(closes, 21)
                                fast, slow = ta.STOCH(high, low, closes, 5, 3, 3)
                                fast, slow = ta.STOCHRSI(closes, 14, 3, 3)
                                upper, middle, lower = ta.BBANDS(closes, ma_period=21)
                                macd, macdsignal, macdhist = ta.MACD(closes, 12, 26, 9)

                                if ema_5[-1] > ema_100[-1] and sma_5[-1] > sma_100[-1] and rsi_9[-1] < 70 and rsi_14[-1] < 70 and rsi_21[-1] < 70 and fast[-1] > slow[-1] and high[-1] > upper[-1]:
                                    enter_points = 1
                                    c = amount_of_repeat

                        # RSI не показывает перекупленности
                        # идет проверка всех остальных индикаторов в заданом промежутке времени
                        # при совпадении всех будет открыта сделка
                        if rsi_9[-1] < 70 and rsi_14[-1] < 70 and rsi_21[-1] < 70:
                            c = 0
                            while c < amount_of_repeat:
                                time.sleep()

                                log.debug("Проверяем индикаторы")
                                # Получаем свечи и берем цены закрытия, high, low
                                klines = bot.klines(
                                    symbol=pair_name.upper(),
                                    interval=TIMEFRAME,
                                    limit=KLINES_LIMITS
                                )
                                klines = klines[:len(klines)-int(not USE_OPEN_CANDLES)]

                                closes = [float(x[4]) for x in klines]
                                high = [float(x[2]) for x in klines]
                                low = [float(x[3]) for x in klines]

                                sma_5 = ta.SMA(closes, 5)
                                sma_100 = ta.SMA(closes, 100)
                                ema_5 = ta.EMA(closes, 5)
                                ema_100 = ta.EMA(closes, 100)
                                rsi_9 = ta.RSI(closes, 9)
                                rsi_14 = ta.RSI(closes, 14)
                                rsi_21 = ta.RSI(closes, 21)
                                fast, slow = ta.STOCH(high, low, closes, 5, 3, 3)
                                fast, slow = ta.STOCHRSI(closes, 14, 3, 3)
                                upper, middle, lower = ta.BBANDS(closes, ma_period=21)
                                macd, macdsignal, macdhist = ta.MACD(closes, 12, 26, 9)

                                if ema_5[-1] > ema_100[-1] and sma_5[-1] > sma_100[-1] and macd[-1] > macdsignal[-1] and macdhist[-1] > 0  and fast[-1] > slow[-1] and high[-1] > upper[-1]:
                                    enter_points = 1
                                    c = amount_of_repeat

                        # Быстрая линия стохастика выше медленной, вход
                        # идет проверка всех остальных индикаторов в заданом промежутке времени
                        # при совпадении всех будет открыта сделка
                        if fast[-1] > slow[-1]:
                            c = 0
                            while c < amount_of_repeat:
                                time.sleep()

                                log.debug("Проверяем индикаторы")
                                # Получаем свечи и берем цены закрытия, high, low
                                klines = bot.klines(
                                    symbol=pair_name.upper(),
                                    interval=TIMEFRAME,
                                    limit=KLINES_LIMITS
                                )
                                klines = klines[:len(klines)-int(not USE_OPEN_CANDLES)]

                                closes = [float(x[4]) for x in klines]
                                high = [float(x[2]) for x in klines]
                                low = [float(x[3]) for x in klines]

                                sma_5 = ta.SMA(closes, 5)
                                sma_100 = ta.SMA(closes, 100)
                                ema_5 = ta.EMA(closes, 5)
                                ema_100 = ta.EMA(closes, 100)
                                rsi_9 = ta.RSI(closes, 9)
                                rsi_14 = ta.RSI(closes, 14)
                                rsi_21 = ta.RSI(closes, 21)
                                fast, slow = ta.STOCH(high, low, closes, 5, 3, 3)
                                fast, slow = ta.STOCHRSI(closes, 14, 3, 3)
                                upper, middle, lower = ta.BBANDS(closes, ma_period=21)
                                macd, macdsignal, macdhist = ta.MACD(closes, 12, 26, 9)

                                if ema_5[-1] > ema_100[-1] and sma_5[-1] > sma_100[-1] and macd[-1] > macdsignal[-1] and macdhist[-1] > 0  and rsi_9[-1] < 70 and rsi_14[-1] < 70 and rsi_21[-1] < 70 and high[-1] > upper[-1]:
                                    enter_points = 1
                                    c = amount_of_repeat

                        # Свеча пробила верхнюю полосу Боллинджера
                        # идет проверка всех остальных индикаторов в заданом промежутке времени
                        # при совпадении всех будет открыта сделка
                        if high[-1] > upper[-1]:
                            c = 0
                            while c < amount_of_repeat:
                                time.sleep()

                                log.debug("Проверяем индикаторы")
                                # Получаем свечи и берем цены закрытия, high, low
                                klines = bot.klines(
                                    symbol=pair_name.upper(),
                                    interval=TIMEFRAME,
                                    limit=KLINES_LIMITS
                                )
                                klines = klines[:len(klines)-int(not USE_OPEN_CANDLES)]

                                closes = [float(x[4]) for x in klines]
                                high = [float(x[2]) for x in klines]
                                low = [float(x[3]) for x in klines]

                                sma_5 = ta.SMA(closes, 5)
                                sma_100 = ta.SMA(closes, 100)
                                ema_5 = ta.EMA(closes, 5)
                                ema_100 = ta.EMA(closes, 100)
                                rsi_9 = ta.RSI(closes, 9)
                                rsi_14 = ta.RSI(closes, 14)
                                rsi_21 = ta.RSI(closes, 21)
                                fast, slow = ta.STOCH(high, low, closes, 5, 3, 3)
                                fast, slow = ta.STOCHRSI(closes, 14, 3, 3)
                                upper, middle, lower = ta.BBANDS(closes, ma_period=21)
                                macd, macdsignal, macdhist = ta.MACD(closes, 12, 26, 9)

                                if ema_5[-1] > ema_100[-1] and sma_5[-1] > sma_100[-1] and macd[-1] > macdsignal[-1] and macdhist[-1] > 0  and rsi_9[-1] < 70 and rsi_14[-1] < 70 and rsi_21[-1] < 70 and fast[-1] > slow[-1]:
                                    enter_points = 1
                                    c = amount_of_repeat



                        log.debug("Свеча набрала {b} баллов".format(b=enter_points))
                        if enter_points ==  POINTS_TO_ENTER:
                            log.debug("Минимальный проходной балл {b}. Пропуск пары".format(b=POINTS_TO_ENTER))
                            continue

                        # Получаем балансы с биржи по указанным валютам
                        balances = {
                            balance['asset']: float(balance['free']) for balance in bot.account()['balances']
                            if balance['asset'] in [pair_obj['base'], pair_obj['quote']]
                        }
                        log.debug("Баланс {balance}".format(balance=["{k}:{bal:0.8f}".format(k=k, bal=balances[k]) for k in balances]))
                        # Если баланс позволяет торговать - выше лимитов биржи и выше указанной суммы в настройках
                        if balances[pair_obj['base']] >= pair_obj['spend_sum']:

                            prices = bot.tickerBookTicker(
                                symbol=pair_name
                            )

                            # Берем цены продаж (продажа будет по рынку)
                            top_price = float(prices['askPrice'])

                            # Рассчитываем кол-во, которое можно купить на заданную сумму, и приводим его к кратному значению
                            my_amount = adjust_to_step(pair_obj['spend_sum']/top_price, CURR_LIMITS['filters'][2]['stepSize'])
                            # Если в итоге получается объем торгов меньше минимально разрешенного, то ругаемся и не создаем ордер
                            if my_amount < float(CURR_LIMITS['filters'][2]['stepSize']) or my_amount < float(CURR_LIMITS['filters'][2]['minQty']):
                                log.warning("""
                                    Минимальная сумма лота: {min_lot:0.8f}
                                    Минимальный шаг лота: {min_lot_step:0.8f}
                                    На свои деньги мы могли бы купить {wanted_amount:0.8f}
                                    После приведения к минимальному шагу мы можем купить {my_amount:0.8f}
                                    Покупка невозможна, выход. Увеличьте размер ставки
                                """.format(
                                    wanted_amount=pair_obj['spend_sum']/ top_price,
                                    my_amount=my_amount,
                                    min_lot=float(CURR_LIMITS['filters'][2]['minQty']),
                                    min_lot_step=float(CURR_LIMITS['filters'][2]['stepSize'])
                                ))
                                continue

                            # Итоговый размер лота

                            trade_am = top_price*my_amount
                            # Если итоговый размер лота меньше минимального разрешенного, то ругаемся и не создаем ордер
                            if trade_am < float(CURR_LIMITS['filters'][3]['minNotional']):
                                raise Exception("""
                                    Итоговый размер сделки {trade_am:0.8f} меньше допустимого по паре {min_am:0.8f}. 
                                    Увеличьте сумму торгов (в {incr} раз(а))""".format(
                                    trade_am=trade_am, min_am=float(CURR_LIMITS['filters'][3]['minNotional']),
                                    incr=float(CURR_LIMITS['filters'][3]['minNotional'])/trade_am
                                ))
                            log.debug(
                                'Рассчитан ордер на покупку по рынку: кол-во {amount:0.8f}, примерный курс: {rate:0.8f}'.format(amount=my_amount, rate=top_price)
                            )
                            # Отправляем команду на бирже о создании ордера на покупку с рассчитанными параметрами
                            new_order = bot.createOrder(
                                symbol=pair_name,
                                recvWindow=5000,
                                side='BUY',
                                type='MARKET',
                                quantity="{quantity:0.{precision}f}".format(
                                    quantity=my_amount, precision=CURR_LIMITS['baseAssetPrecision']
                                ),
                                newOrderRespType='FULL'
                            )
                            # Если удалось создать ордер на покупку, записываем информацию в БД
                            if 'orderId' in new_order:
                                log.info("Создан ордер на покупку {new_order}".format(new_order=new_order))

                                add_db_new_order(cursor,conn,pair_name,new_order['orderId'], my_amount, top_price)
                                # Получить итоговую цену ордера

                                log.debug('Получаем сделки и вычисляем комиссию')

                                order_trades = get_order_trades(
                                    order_id=new_order['orderId'],
                                    pair=pair_name,
                                    bot=bot
                                )

                                avg_rate = calc_buy_avg_rate(order_trades, log)
                                if avg_rate > 0:
                                    update_buy_rate(cursor, conn, new_order['orderId'], avg_rate)

                            else:
                                log.warning("Не удалось создать ордер на покупку! {new_order}".format(new_order=str(new_order)))

                        else:
                            log.warning('Для создания ордера на покупку нужно минимум {min_qty:0.8f} {curr}, выход'.format(
                                min_qty=pair_obj['spend_sum'], curr=pair_obj['base']
                            ))
                    except:
                        log.exception("Пропускаем пару " + pair_name)
            else:
                log.debug('По всем парам есть неисполненные ордера')

        except Exception as e:
            log.exception(e)
        finally:
            conn.close()

if __name__ == "__main__":

    sync_time(bot, log, False,)

    t1 = threading.Thread(target=main_flow)
    t2 = threading.Thread(target=sync_time, args=(bot, log, True,))

    threads = [t1, t2]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

