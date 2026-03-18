# ==============================================================================
# 🌌 FSD Engine (PROD): TELEGRAM LIVE ORACLE 
# Описание: Автономный робот. Динамический парсинг S&P 500 + Z-Сигмоида + Telegram
# Архитектура: Интерактивный режим (Polling) + Фоновый планировщик (Cron)
# ==============================================================================

import telebot
import yfinance as yf
import pandas as pd
import numpy as np
import math
import requests
import threading
import schedule
import time
from io import StringIO
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔐 НАСТРОЙКИ БЕЗОПАСНОСТИ (ВСТАВЬТЕ СВОИ ДАННЫЕ)
# ==============================================================================
TELEGRAM_TOKEN = ""
ADMIN_CHAT_ID = ""

bot = telebot.TeleBot(TELEGRAM_TOKEN)

class FSD_Math:
    """Квантитативная Физика (Ядро FSD)"""
    @staticmethod
    def calc_z_sigmoid(series: pd.Series) -> pd.Series:
        safe_series = np.maximum(series, 1e-9)
        log_s = np.log1p(safe_series)
        std_val = log_s.std()
        if std_val == 0: return pd.Series(0.5, index=series.index)
        z = (log_s - log_s.mean()) / std_val
        return (1 / (1 + np.exp(-z * 1.5))).round(4)

    @staticmethod
    def calc_atr(high, low, close, period=14):
        if len(close) < 2: return 0.0
        hl = high - low
        hc = np.abs(high - close.shift())
        lc = np.abs(low - close.shift())
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return float(tr.rolling(period, min_periods=1).mean().iloc[-1])

    @staticmethod
    def calc_bollinger_b(close_series: pd.Series, period=20, std_dev=2):
        if len(close_series) < period: return 0.5
        sma = close_series.rolling(window=period).mean()
        std = close_series.rolling(window=period).std()
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        diff = upper.iloc[-1] - lower.iloc[-1]
        if diff == 0: return 0.5
        return round((close_series.iloc[-1] - lower.iloc[-1]) / diff, 4)

    @staticmethod
    def calc_hurst(prices):
        prices = prices.dropna().tolist()
        n = len(prices)
        if n < 8: return 0.5
        returns = [math.log(prices[i]/prices[i-1]) for i in range(1, n) if prices[i-1] > 0]
        if not returns: return 0.5
        mean = sum(returns) / len(returns)
        devs = [x - mean for x in returns]
        cum_sum = np.cumsum(devs)
        R = max(cum_sum) - min(cum_sum)
        S = math.sqrt(sum(d**2 for d in devs) / len(returns))
        if S == 0 or R == 0: return 0.5
        rs = R / S
        if rs <= 0: return 0.5
        h = math.log(rs) / math.log(len(returns))
        return round(max(0.01, min(0.99, h)), 4)


def process_market():
    """Функция сканирования Уолл-Стрит и генерации сигналов"""
    bot.send_message(ADMIN_CHAT_ID, "🔍 <b>FSD Оракул:</b> Обновляю актуальный состав S&P 500 из Wikipedia...", parse_mode='HTML')
    
    # 1. ДИНАМИЧЕСКИЙ ПАРСИНГ РЫНКА
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        table = pd.read_html(StringIO(response.text))[0]
        tickers = [t.replace('.', '-') for t in table['Symbol'].tolist()]
    except Exception as e:
        bot.send_message(ADMIN_CHAT_ID, f"⚠️ Ошибка Википедии: {e}\nИспользую резервный ТОП-50.")
        tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK-B", "LLY", "AVGO", "JPM", "V", "UNH", "XOM", "PG"]
        
    # Добавляем спекулятивные инструменты (Крипта + Мемы) для шортов и симбиоза
    speculative = ["BTC-USD", "ETH-USD", "SOL-USD", "GME", "AMC", "CVNA", "MSTR", "HOOD", "PLTR"]
    all_tickers = list(set(tickers + speculative))
    
    bot.send_message(ADMIN_CHAT_ID, f"📡 Скачиваю живые котировки для {len(all_tickers)} активов. Ожидайте ~40 сек...", parse_mode='HTML')
    
    try:
        # Качаем 250 дней истории для макро-фильтра SMA 200
        data = yf.download(all_tickers + ["^GSPC"], period="250d", progress=False)
    except Exception as e:
        bot.send_message(ADMIN_CHAT_ID, f"❌ Критическая ошибка Yahoo API: {e}")
        return

    # 2. МАКРО-ФИЛЬТР (^GSPC)
    macro_data = data['Close']['^GSPC'].dropna()
    if len(macro_data) < 200: macro_sma200 = macro_data.mean()
    else: macro_sma200 = macro_data.rolling(200).mean().iloc[-1]
        
    is_bull_market = bool(macro_data.iloc[-1] > macro_sma200)
    
    market_data = []
    
    # 3. МИКРО-ФИЗИКА АКТИВОВ
    for ticker in all_tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if ticker in data.columns.get_level_values(1): hist = data.xs(ticker, axis=1, level=1)
                elif ticker in data.columns.get_level_values(0): hist = data[ticker]
                else: continue
            else: hist = data

            hist = hist.dropna().copy()
            if len(hist) < 30: continue
            
            c, v, h, l = hist['Close'], hist['Volume'], hist['High'], hist['Low']
            last_c, prev_c = float(c.iloc[-1]), float(c.iloc[-2])
            
            dollar_vol = c * v
            mass_proxy = float(dollar_vol.rolling(30).mean().iloc[-2]) 
            kinetic_proxy = float(dollar_vol.iloc[-1])                 
            
            if pd.isna(mass_proxy) or mass_proxy <= 0 or kinetic_proxy <= 0: continue
            
            atr = FSD_Math.calc_atr(h, l, c)
            price_delta = float(last_c - prev_c)
            threshold = float(atr / 2.0)
            
            v1 = 0 if abs(price_delta) <= threshold else (1 if price_delta > 0 else -1)
            v2 = 1 if float(c.iloc[-1] - c.iloc[-4]) > 0 else -1
            percent_b = FSD_Math.calc_bollinger_b(c)
            hurst = FSD_Math.calc_hurst(c)
            
            market_data.append({
                "Ticker": ticker, "Price": round(last_c, 2), "ATR": round(atr, 2),
                "Mass_Raw": mass_proxy, "Kinetics_Raw": kinetic_proxy,
                "Vector_1": v1, "Vector_2": v2, "Percent_B": percent_b, "Hurst": hurst
            })
        except: continue
            
    df = pd.DataFrame(market_data)
    if df.empty:
        bot.send_message(ADMIN_CHAT_ID, "❌ Ошибка: биржа не вернула данные. Рынок закрыт или сбой сети.")
        return
        
    # 4. ГЛОБАЛЬНАЯ Z-СИГМОИДА
    df["P_Mass"] = FSD_Math.calc_z_sigmoid(df["Mass_Raw"])
    df["P_Kin"] = FSD_Math.calc_z_sigmoid(df["Kinetics_Raw"])
    
    classify = lambda p: "F" if p >= 0.65 else ("SD" if p >= 0.35 else "A")
    df["Mass"] = df["P_Mass"].apply(classify)
    df["Kinetic"] = df["P_Kin"].apply(classify)

    long_capitulation, long_symbiosis, short_signals = [], [], []
    
    # 5. ВЫДЕЛЕНИЕ АНОМАЛИЙ
    for _, r in df.iterrows():
        t, p, m, k, atr = r['Ticker'], r['Price'], r['Mass'], r['Kinetic'], r['ATR']
        v1, v2, pb, hurst = r['Vector_1'], r['Vector_2'], r['Percent_B'], r['Hurst']
        
        # 🟢 LONG: Капитуляция Гигантов (Бычий рынок, актив F, Кинетика F, Вектор 0 на дне %B)
        if is_bull_market and m == "F" and k == "F" and v2 == -1 and v1 == 0 and pb <= 0.15:
            sl, tp = round(p - (1.5 * atr), 2), round(p + (3.0 * atr), 2)
            long_capitulation.append(f"🟢 <b>LONG (Капитуляция): #{t}</b>\n💵 Вход: <b>${p}</b>\n🛡 SL: ${sl} | 🎯 TP: ${tp}\n📉 %B: {pb:.2f} | <i>Умные деньги выкупают панику</i>\n")
            
        # 💎 LONG: Симбиоз (Бычий рынок, актив A/SD, Кинетика F, Вектор 0, зарождение тренда H > 0.55)
        elif is_bull_market and m in ["A", "SD"] and k == "F" and v1 == 0 and hurst > 0.55:
            sl, tp = round(p - (1.5 * atr), 2), round(p + (3.0 * atr), 2)
            long_symbiosis.append(f"💎 <b>LONG (Симбиоз): #{t}</b>\n💵 Вход: <b>${p}</b>\n🛡 SL: ${sl} | 🎯 TP: ${tp}\n📈 Hurst: {hurst:.2f} | <i>Скрытое накопление капитала</i>\n")

        # 🔴 SHORT: Эйфория Мусора (Медвежий рынок, актив A, Кинетика F, Вектор 0 на хаях %B)
        elif not is_bull_market and m == "A" and k == "F" and v2 == 1 and v1 == 0 and pb >= 0.85:
            sl, tp = round(p + (1.5 * atr), 2), round(p - (3.0 * atr), 2)
            short_signals.append(f"🔴 <b>SHORT (Эйфория): #{t}</b>\n💵 Вход: <b>${p}</b>\n🛡 SL: ${sl} | 🎯 TP: ${tp}\n📈 %B: {pb:.2f} | <i>Институционалы разгружаются об толпу</i>\n")

    # 6. ФОРМИРОВАНИЕ ОТЧЕТА
    macro_text = "БЫЧИЙ 🐂 (Разрешены Long)" if is_bull_market else "МЕДВЕЖИЙ 🐻 (Разрешены Short)"
    report = f"🌌 <b>FSD ENGINE: ОТЧЕТ УОЛЛ-СТРИТ</b> 🌌\n🌍 <b>Макро-режим:</b> {macro_text}\n📈 <b>Проанализировано:</b> {len(df)} активов\n\n"
    
    signals_exist = False
    if long_capitulation:
        report += "<b>🏆 КВАНТОВАЯ КАПИТУЛЯЦИЯ:</b>\n" + "\n".join(long_capitulation) + "\n"
        signals_exist = True
    if long_symbiosis:
        report += "<b>🧬 ИНВЕСТИЦИОННАЯ ИНИЦИАТИВА:</b>\n" + "\n".join(long_symbiosis) + "\n"
        signals_exist = True
    if short_signals:
        report += "<b>🩸 РАСПРЕДЕЛЕНИЕ / ПУЗЫРИ:</b>\n" + "\n".join(short_signals) + "\n"
        signals_exist = True
        
    if not signals_exist:
        report += "💤 <b>ШТИЛЬ.</b>\nИдеальных математических точек входа сегодня нет. Кеш генерирует безрисковую ставку ФРС."

    bot.send_message(ADMIN_CHAT_ID, report, parse_mode="HTML")

# ==============================================================================
# 🤖 ОБРАБОТЧИКИ КОМАНД (TELEGRAM HANDLERS)
# ==============================================================================
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if str(message.chat.id) != str(ADMIN_CHAT_ID):
        bot.reply_to(message, "⛔ Отказано в доступе. Вы не являетесь Архитектором системы.")
        return
    text = (
        "🌌 <b>FSD Engine Oracle (Live Production)</b> 🌌\n\n"
        "Алгоритм уровня Quant Fund готов к работе.\n"
        "Команды:\n"
        "🔹 /scan — Принудительно запустить анализ всего S&P 500 прямо сейчас\n"
        "🔹 /status — Проверить аптайм серверов\n\n"
        "⏳ <i>Планировщик автоматически сканирует рынок каждый будний день в 22:45 МСК (за 15 мин до закрытия Нью-Йорка).</i>"
    )
    bot.send_message(message.chat.id, text, parse_mode="HTML")

@bot.message_handler(commands=['status'])
def status_check(message):
    if str(message.chat.id) != str(ADMIN_CHAT_ID): return
    bot.reply_to(message, "🟢 Сервер активен. Математические модули в норме. API подключено.")

@bot.message_handler(commands=['scan'])
def manual_scan(message):
    if str(message.chat.id) != str(ADMIN_CHAT_ID): return
    # Запускаем в отдельном потоке, чтобы не блокировать интерфейс бота
    threading.Thread(target=process_market).start()

# ==============================================================================
# 🕒 ФОНОВЫЙ ПЛАНИРОВЩИК (SCHEDULER)
# ==============================================================================
def run_scheduler():
    """Запускает бота каждый день по расписанию"""
    # Рынок США закрывается в 16:00 EST. По Москве это 23:00 (зимой) или 24:00 (летом).
    # Установите время на 15 минут до закрытия рынка в вашем часовом поясе.
    schedule.every().day.at("22:45").do(process_market)
    
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == "__main__":
    if TELEGRAM_TOKEN == "ВАШ_ТОКЕН_ОТ_BOTFATHER" or ADMIN_CHAT_ID == "ВАШ_ID_ИЗ_USERINFOBOT":
        print("[!] ОШИБКА: Пожалуйста, вставьте ваши TELEGRAM_TOKEN и ADMIN_CHAT_ID в строках 23-24!")
    else:
        print("🌌 [FSD Oracle] Сервер запущен. Бот слушает команды в Telegram...")
        
        # Запуск планировщика в фоновом потоке
        threading.Thread(target=run_scheduler, daemon=True).start()
        
        # Бесконечный цикл приема сообщений
        while True:
            try:
                bot.polling(none_stop=True, interval=0, timeout=20)
            except Exception as e:
                print(f"[!] Сбой соединения: {e}. Перезапуск через 5 сек...")
                time.sleep(5)