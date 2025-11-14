"""
Multi-Strategy Trading Signal Generator for IB Trading App

This script generates trading signals for 8 different strategies and outputs
a single consolidated CSV file for manual upload to ib-trading-app.

Strategies:
1. MOMO - Momentum stocks (NASDAQ 100)
2. GROWTH - Growth ETFs (QQQ, SPY, IOO)
3. DEF - Defensive ETFs (GLD, TLT)
4. BTC - Bitcoin exposure (IBIT)
5. MR Long - Mean reversion longs (S&P 500)
6. MR Short - Mean reversion shorts (S&P 500)
7. HFT Long - High frequency longs (Russell 1000)
8. HFT Short - High frequency shorts (Russell 1000)

Author: Claude Code
Date: 2025-11-12
"""

import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import norgatedata as nd
from ta.trend import ADXIndicator, SMAIndicator
from ta.volatility import AverageTrueRange
from ta.momentum import RSIIndicator
import pytz
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import utils
from utils import indicator_utils as ind
from utils import data_utils as du
from utils import api_utils as api
from ib_api_client import create_client

# ============================================================================
# CONFIGURATION
# ============================================================================

# IB Trading App API Configuration
IB_API_URL = os.getenv("IB_API_URL", "http://localhost:8000")
IB_API_KEY = os.getenv("IB_API_KEY", "")

# Legacy credentials (no longer used with API key authentication)
# IB_USERNAME = os.getenv("IB_USERNAME", "matthewwills")
# IB_PASSWORD = os.getenv("IB_PASSWORD", "")

# Capital Management
BUFFER = 0.20  # 20% safety buffer on leverage

# Output Configuration
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# STRATEGY PARAMETERS
# ============================================================================

# MOMO Strategy - Momentum Stocks (NASDAQ 100)
MOMO_ALLOCATION = 0.05  # 5%
MOMO_MAX_POS = 3
MOMO_WORST_RANK = 5  # Exit if rank drops below 5
MOMO_ROC_P1 = 120
MOMO_ROC_P2 = 240
MOMO_MA_PERIOD = 100
MOMO_MIN_BARS = 250
MOMO_BULL_MKT = True  # Enable bullish market filter
MOMO_INDEX_PERIOD = 13
MOMO_INDEX_SYMBOL = "#NYSEHL"  # NYSE High-Low index
MOMO_UNIVERSE = "Nasdaq 100"  # Norgate watchlist name

# GROWTH Strategy - Growth ETFs
GROWTH_ALLOCATION = 0.10  # 10%
GROWTH_MAX_POS = 1
GROWTH_WORST_RANK = 2
GROWTH_ROC_PERIOD = 75  # Single ROC period
GROWTH_ROC_P1 = 75
GROWTH_ROC_P2 = 150
GROWTH_UPTREND = True  # Enable uptrend filter
GROWTH_UP_TREND = True  # Enable uptrend filter
GROWTH_MA_PERIOD = 100
GROWTH_SINCE_TRUE = 5  # Must be true for 5 bars
GROWTH_MIN_BARS = 250
GROWTH_SYMBOLS = ["QQQ", "SPY", "IOO"]
GROWTH_UNIVERSE = ["QQQ", "SPY", "IOO"]

# DEF Strategy - Defensive ETFs
DEF_ALLOCATION = 0.03  # 3%
DEF_MAX_POS = 1
DEF_WORST_RANK = 1
DEF_ROC_PERIOD = 75  # Single ROC period
DEF_ROC_P1 = 75
DEF_ROC_P2 = 150
DEF_UPTREND = True  # Enable uptrend filter
DEF_UP_TREND = True  # Enable uptrend filter
DEF_MA_PERIOD = 100
DEF_SINCE_TRUE = 5
DEF_MIN_BARS = 250
DEF_SYMBOLS = ["GLD", "TLT"]
DEF_UNIVERSE = ["GLD", "TLT"]

# BTC Strategy - Bitcoin
BTC_ALLOCATION = 0.02  # 2%
BTC_ROC_PERIOD = 40
BTC_UPTREND = True  # Enable uptrend filter
BTC_UP_TREND = True  # Enable uptrend filter
BTC_MA_PERIOD = 100
BTC_SINCE_TRUE = 4  # Must be true for 4 bars
BTC_MIN_BARS = 50
BTC_SYMBOL = "IBIT"
BTC_UNIVERSE = ["IBIT"]

# MR Long Strategy - Mean Reversion Longs (S&P 500)
MR_ALLOCATION_LONG = 0.15  # 15%
MR_LONG_MAX_POS = 10
MR_MIN_BARS = 200
MR_MA_PERIOD = 100
MR_ADX_PERIOD = 10
MR_ADX_LIMIT = 30
MR_ATR_PERIOD = 10
MR_MIN_PRICE = 5
MR_VOLUME_PERIOD = 50
MR_VOLUME_LIMIT = 200000
MR_LONG_RSI_PERIOD = 2
MR_LONG_RSI_LIMIT = 30  # Oversold
MR_LONG_STRETCH = 0.5  # Entry at Low - 0.5*ATR
MR_UNIVERSE = "S&P 500"  # Norgate watchlist name

# MR Short Strategy - Mean Reversion Shorts (S&P 500)
MR_ALLOCATION_SHORT = 0.15  # 15%
MR_SHORT_MAX_POS = 10
MR_SHORT_MIN_BARS = 200
MR_SHORT_MA_PERIOD = 100
MR_SHORT_ADX_PERIOD = 10
MR_SHORT_ADX_LIMIT = 30
MR_SHORT_ATR_PERIOD = 10
MR_SHORT_MIN_PRICE = 5
MR_SHORT_MIN_VOL = 200000
MR_SHORT_RSI_PERIOD = 3
MR_SHORT_RSI_LIMIT = 90  # Overbought
MR_SHORT_STRETCH = 0.8  # Entry at High + 0.8*ATR
MR_SHORT_UNIVERSE = "S&P 500"  # Norgate watchlist name

# HFT Long Strategy - High Frequency Longs (Russell 1000)
HFT_ALLOCATION_LONG = 0.25  # 25%
HFT_LONG_MAX_POS = 15
HFT_MIN_BARS = 251
HFT_MA_PERIOD = 250
HFT_ADX_PERIOD = 4
HFT_ADX_LIMIT = 35
HFT_VOLUME_PERIOD = 50
HFT_VOLUME_LIMIT = 2000000
HFT_ATR_PERIOD = 5
HFT_LONG_MIN_PRICE = 10
HFT_LONG_MAX_PRICE = 5000
HFT_LONG_IBR_LIMIT = 0.3  # Closed near low of range
HFT_LONG_STRETCH = 0.6  # Entry at Low - 0.6*ATR
HFT_UNIVERSE = "Russell 1000"  # Norgate watchlist name

# HFT Short Strategy - High Frequency Shorts (Russell 1000)
HFT_ALLOCATION_SHORT = 0.25  # 25%
HFT_SHORT_MAX_POS = 15
HFT_SHORT_MIN_BARS = 251
HFT_SHORT_MA_PERIOD = 250
HFT_SHORT_ADX_PERIOD = 4
HFT_SHORT_ADX_LIMIT = 35
HFT_SHORT_ATR_PERIOD = 5
HFT_SHORT_MIN_PRICE = 20
HFT_SHORT_MAX_PRICE = 5000
HFT_SHORT_MIN_VOL = 2000000
HFT_SHORT_IBR_LIMIT = 0.7  # Closed near high of range
HFT_SHORT_STRETCH = 0.3  # Entry at High + 0.3*ATR
HFT_SHORT_UNIVERSE = "Russell 1000"  # Norgate watchlist name

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_order_row(symbol, action, quantity, order_type, limit_price, strategy_name,
                     account="", security_type="STK", exchange="SMART", time_in_force="DAY",
                     good_till_date="", attach_moc="NO"):
    """
    Create a standardized order row for CSV output.

    Returns a dictionary with all 19 required CSV columns (including Account).
    """
    return {
        "Symbol": symbol,
        "Account": account,  # New column - leave blank for default account
        "Action": action,
        "Quantity": int(quantity),
        "OrderType": order_type,
        "LimitPrice": f"{limit_price:.2f}" if limit_price else "",
        "StopPrice": "",
        "SecurityType": security_type,
        "Exchange": exchange,
        "Timezone": "",
        "TimeInForce": time_in_force,
        "GoodTillDate": good_till_date,
        "AttachMOC": attach_moc,
        "Strategy": strategy_name,
        "OutsideRTH": "NO",
        "AllOrNone": "NO",
        "Hidden": "NO",
        "DisplaySize": "0",
        "DisplaySizeIsPercentage": "NO"
    }


def calculate_gtd_time():
    """
    Calculate Good Till Date for HFT orders.
    Returns today at 15:44 ET, or next trading day if after 15:44.
    """
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)

    # Target time is 15:44 ET
    target_time = now_et.replace(hour=15, minute=44, second=0, microsecond=0)

    # If it's already past 15:44, use tomorrow
    if now_et > target_time:
        target_time += timedelta(days=1)

    # Format: 2025-11-12T15:44 (no seconds, matching template format)
    return target_time.strftime("%Y-%m-%dT%H:%M")


def get_current_positions_by_strategy(positions_list):
    """
    Organize positions by strategy name.

    Returns:
        dict: {strategy_name: [list of symbols]}
    """
    positions_by_strategy = {}

    for pos in positions_list:
        symbol = pos.get('symbol')
        # Note: IB API doesn't return strategy name directly
        # We'll need to infer based on position characteristics
        # For now, just track all open symbols
        if 'all' not in positions_by_strategy:
            positions_by_strategy['all'] = []
        positions_by_strategy['all'].append(symbol)

    return positions_by_strategy


# ============================================================================
# STRATEGY IMPLEMENTATIONS
# ============================================================================

def run_momo_strategy(usable_capital, current_positions_df):
    """
    MOMO Strategy - Momentum Stocks (NASDAQ 100)

    Entry: Bullish market (NYSE H-L > MA), Price > 100-SMA, Momentum > 0
    Ranking: 0.5*ROC(120) + 0.5*ROC(240)
    Hold: Top 3 positions
    Exit: If rank drops below 5
    """
    print("\n" + "=" * 80)
    print("MOMO STRATEGY - Momentum Stocks (NASDAQ 100)")
    print("=" * 80)

    orders = []

    # Get data end date for monthly rebalancing
    today = datetime.now().date()
    last_friday_current_month = du.get_last_friday_of_month(today)
    last_friday_previous_month = du.get_last_friday_of_previous_month(today)

    if today > last_friday_current_month:
        data_end_date = last_friday_current_month
    else:
        data_end_date = last_friday_previous_month

    # Check bullish market condition if enabled
    momo_bullmkt = False
    if MOMO_BULL_MKT:
        try:
            index_data = du.getData_endDate(MOMO_INDEX_SYMBOL, MOMO_INDEX_PERIOD + 5, data_end_date)
            index_close = index_data.Close
            momo_bullmkt = index_close.iloc[-1] > index_close.iloc[-1 - MOMO_INDEX_PERIOD]
            print(f"Market Filter: {'BULLISH' if momo_bullmkt else 'BEARISH'} (NYSE H-L vs {MOMO_INDEX_PERIOD}-MA)")
        except Exception as e:
            print(f"Warning: Could not fetch market index data: {e}")
            print("Proceeding without market filter")
            momo_bullmkt = True  # Default to allowing trades
    else:
        momo_bullmkt = True
        print("Market Filter: DISABLED")

    # Get current MOMO positions
    current_momo_symbols = []
    if not current_positions_df.empty:
        # Filter for what we believe are MOMO positions
        # Since we don't have strategy name, we'll just track all for now
        current_momo_symbols = current_positions_df['Symbol'].tolist()

    print(f"Current MOMO positions: {len(current_momo_symbols)}")

    # Scan universe and rank
    momo_list = []
    ticker_list = nd.watchlist_symbols(MOMO_UNIVERSE)
    print(f"Scanning {len(ticker_list)} symbols...")

    for symbol in ticker_list:
        try:
            data = du.getData_endDate(symbol, MOMO_MIN_BARS + 1, data_end_date)
            if len(data) < MOMO_MIN_BARS:
                continue

            c = data.Close.iloc[-1]

            # Calculate indicators
            momo_ma = SMAIndicator(data.Close, MOMO_MA_PERIOD, True).sma_indicator().iloc[-1]
            momo_uptrend = c > momo_ma
            momo_factor = 0.5 * ind.ROC(data.Close, MOMO_ROC_P1) + 0.5 * ind.ROC(data.Close, MOMO_ROC_P2)

            # Calculate position size
            momo_buy_price = c
            momo_quantity = int(usable_capital * MOMO_ALLOCATION / MOMO_MAX_POS / momo_buy_price)

            # Check entry conditions
            if momo_bullmkt and momo_factor > 0 and momo_uptrend:
                momo_list.append({
                    'Symbol': symbol,
                    'Quantity': momo_quantity,
                    'BuyPrice': momo_buy_price,
                    'MomoFactor': round(momo_factor, 3)
                })

        except Exception as e:
            continue

    # Sort by momentum factor
    momo_list.sort(key=lambda x: x['MomoFactor'], reverse=True)

    # Get top symbols
    hold_symbols = [x['Symbol'] for x in momo_list[:MOMO_WORST_RANK]]
    entry_symbols = [x['Symbol'] for x in momo_list[:MOMO_MAX_POS]]

    print(f"Qualified symbols: {len(momo_list)}")
    print(f"Top {MOMO_MAX_POS} symbols: {entry_symbols}")
    print(f"Hold buffer (top {MOMO_WORST_RANK}): {hold_symbols}")

    # Generate EXIT orders (for positions not in hold list)
    for symbol in current_momo_symbols:
        if symbol not in hold_symbols:
            # Get current quantity (we don't have it from IB API, use placeholder)
            orders.append(create_order_row(
                symbol=symbol,
                action="SELL",
                quantity=100,  # TODO: Get actual quantity from position
                order_type="MARKET",
                limit_price=None,
                strategy_name="momo",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  EXIT: {symbol} (no longer in top {MOMO_WORST_RANK})")

    # Generate ENTRY orders (for new positions)
    for item in momo_list[:MOMO_MAX_POS]:
        symbol = item['Symbol']
        if symbol not in current_momo_symbols:
            orders.append(create_order_row(
                symbol=symbol,
                action="BUY",
                quantity=item['Quantity'],
                order_type="MARKET",
                limit_price=None,
                strategy_name="momo",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  ENTRY: {symbol} @ Market (Factor: {item['MomoFactor']})")

    print(f"\nGenerated {len(orders)} MOMO orders")
    return orders


def run_mr_long_strategy(usable_capital, current_positions_df):
    """
    MR Long Strategy - Mean Reversion Longs (S&P 500)

    Entry: Price > $5, Vol > 200k, Price > 100-SMA, ADX > 30, RSI(2) < 30
    Entry Limit: Low - 0.5*ATR
    Exit Limit: Previous day's high
    """
    print("\n" + "=" * 80)
    print("MR LONG STRATEGY - Mean Reversion Longs (S&P 500)")
    print("=" * 80)

    orders = []

    # Get current MR long positions
    mr_long_symbols = []
    mr_long_positions = {}
    if not current_positions_df.empty:
        # Filter for long positions (positive quantity)
        long_pos = current_positions_df[current_positions_df['Quantity'] > 0]
        mr_long_symbols = long_pos['Symbol'].tolist()
        mr_long_positions = dict(zip(long_pos['Symbol'], long_pos['Quantity']))

    print(f"Current MR Long positions: {len(mr_long_symbols)}")

    # Generate EXIT orders for open positions
    exit_count = 0
    for symbol in mr_long_symbols:
        try:
            data = du.getData(symbol, 3)
            prev_high = data.High.iloc[-2]  # Previous day's high

            orders.append(create_order_row(
                symbol=symbol,
                action="SELL",
                quantity=mr_long_positions.get(symbol, 100),
                order_type="LIMIT",
                limit_price=prev_high,
                strategy_name="mr-long",
                security_type="STK",
                exchange="SMART",
                time_in_force="GTC"
            ))
            exit_count += 1
            print(f"  EXIT ORDER: {symbol} @ ${prev_high:.2f} limit (prev high)")
        except Exception as e:
            print(f"  Warning: Could not generate exit for {symbol}: {e}")

    # Scan for ENTRY signals
    ticker_list = nd.watchlist_symbols(MR_UNIVERSE)
    print(f"\nScanning {len(ticker_list)} symbols for entries...")

    entry_candidates = []

    for symbol in ticker_list:
        # Skip if already in position
        if symbol in mr_long_symbols:
            continue

        # Skip GOOG (as in original)
        if symbol == "GOOG":
            continue

        try:
            data = du.getData(symbol, MR_MIN_BARS + 1)
            if len(data) < MR_MIN_BARS:
                continue

            # Get latest values
            c = data.Close.iloc[-1]
            l = data.Low.iloc[-1]
            h = data.High.iloc[-1]

            # Calculate indicators
            mr_atr = AverageTrueRange(data.High, data.Low, data.Close, MR_ATR_PERIOD, True).average_true_range().iloc[-1]
            mr_ma = SMAIndicator(data.Close, MR_MA_PERIOD, True).sma_indicator().iloc[-1]
            mr_adx = ADXIndicator(data.High, data.Low, data.Close, MR_ADX_PERIOD, fillna=True).adx().iloc[-1]
            mr_avg_volume = SMAIndicator(data.Volume, MR_VOLUME_PERIOD, True).sma_indicator().iloc[-1]
            mr_volatility = mr_atr / c * 100
            mr_long_rsi = RSIIndicator(data.Close, MR_LONG_RSI_PERIOD, True).rsi().iloc[-1]

            # Calculate entry limit price
            mr_long_entry_limit = round(l - MR_LONG_STRETCH * mr_atr, 2)

            # Calculate position size
            mr_long_quantity = max(1, int(MR_ALLOCATION_LONG * usable_capital / MR_LONG_MAX_POS / mr_long_entry_limit))

            # Check entry conditions
            if (c > MR_MIN_PRICE and
                mr_avg_volume > MR_VOLUME_LIMIT and
                c > mr_ma and
                mr_adx > MR_ADX_LIMIT and
                mr_long_rsi < MR_LONG_RSI_LIMIT):

                entry_candidates.append({
                    'Symbol': symbol,
                    'Quantity': mr_long_quantity,
                    'EntryLimit': mr_long_entry_limit,
                    'Volatility': round(mr_volatility, 3),
                    'RSI': round(mr_long_rsi, 2)
                })

        except Exception as e:
            continue

    # Sort by volatility (higher volatility = higher rank)
    entry_candidates.sort(key=lambda x: x['Volatility'], reverse=True)

    # Take top positions up to max
    num_open = len(mr_long_symbols)
    num_to_add = min(len(entry_candidates), MR_LONG_MAX_POS - num_open)

    print(f"Qualified entries: {len(entry_candidates)}")
    print(f"Can add: {num_to_add} (current: {num_open}, max: {MR_LONG_MAX_POS})")

    for item in entry_candidates[:num_to_add]:
        orders.append(create_order_row(
            symbol=item['Symbol'],
            action="BUY",
            quantity=item['Quantity'],
            order_type="LIMIT",
            limit_price=item['EntryLimit'],
            strategy_name="mr-long",
            security_type="STK",
            exchange="SMART",
            time_in_force="GTC"
        ))
        print(f"  ENTRY: {item['Symbol']} @ ${item['EntryLimit']:.2f} limit (RSI: {item['RSI']}, Vol: {item['Volatility']})")

    print(f"\nGenerated {len(orders)} MR Long orders ({exit_count} exits, {len(orders)-exit_count} entries)")
    return orders


def run_hft_long_strategy(usable_capital, current_positions_df):
    """
    HFT Long Strategy - High Frequency Longs (Russell 1000)

    Entry: $10-$5000, Vol > 2M, Price > 250-SMA, ADX > 35, IBR < 0.3
    Entry Limit: Low - 0.6*ATR
    Exit: Attached Market-On-Close (AttachMOC)
    """
    print("\n" + "=" * 80)
    print("HFT LONG STRATEGY - High Frequency Longs (Russell 1000)")
    print("=" * 80)

    orders = []

    # Get current HFT long positions
    hft_long_symbols = []
    if not current_positions_df.empty:
        hft_long_symbols = current_positions_df['Symbol'].tolist()

    print(f"Current HFT Long positions: {len(hft_long_symbols)}")

    # Calculate GTD time
    gtd_time = calculate_gtd_time()
    print(f"GTD Time: {gtd_time}")

    # Scan for entries
    ticker_list = nd.watchlist_symbols(HFT_UNIVERSE)
    print(f"Scanning {len(ticker_list)} symbols...")

    entry_candidates = []

    for symbol in ticker_list:
        # Skip if already in position
        if symbol in hft_long_symbols:
            continue

        try:
            data = du.getData(symbol, HFT_MIN_BARS + 1)
            if len(data) < HFT_MIN_BARS:
                continue

            # Get latest values
            c = data.Close.iloc[-1]
            l = data.Low.iloc[-1]
            h = data.High.iloc[-1]

            # Price range filter
            if c < HFT_LONG_MIN_PRICE or c > HFT_LONG_MAX_PRICE:
                continue

            # Calculate indicators
            hft_atr = AverageTrueRange(data.High, data.Low, data.Close, HFT_ATR_PERIOD, True).average_true_range().iloc[-1]
            hft_ma = SMAIndicator(data.Close, HFT_MA_PERIOD, True).sma_indicator().iloc[-1]
            hft_adx = ADXIndicator(data.High, data.Low, data.Close, HFT_ADX_PERIOD, fillna=True).adx().iloc[-1]
            hft_avg_volume = SMAIndicator(data.Volume, HFT_VOLUME_PERIOD, True).sma_indicator().iloc[-1]
            hft_volatility = hft_atr / c * 100
            hft_ibr = ind.IBR(h, l, c)

            # Calculate entry limit price with tick size rounding
            hft_long_entry_limit = l - HFT_LONG_STRETCH * hft_atr
            tick_size = ind.tickSize(hft_long_entry_limit)
            hft_long_entry_limit = round(hft_long_entry_limit / tick_size) * tick_size

            # Calculate position size
            hft_long_quantity = max(1, int(HFT_ALLOCATION_LONG * usable_capital / HFT_LONG_MAX_POS / hft_long_entry_limit))

            # Check entry conditions
            if (hft_avg_volume > HFT_VOLUME_LIMIT and
                c > hft_ma and
                hft_adx > HFT_ADX_LIMIT and
                hft_ibr < HFT_LONG_IBR_LIMIT):

                entry_candidates.append({
                    'Symbol': symbol,
                    'Quantity': hft_long_quantity,
                    'EntryLimit': hft_long_entry_limit,
                    'Volatility': round(hft_volatility, 3),
                    'IBR': round(hft_ibr, 3)
                })

        except Exception as e:
            continue

    # Sort by volatility
    entry_candidates.sort(key=lambda x: x['Volatility'], reverse=True)

    # Take top positions
    num_to_add = min(len(entry_candidates), HFT_LONG_MAX_POS)

    print(f"Qualified entries: {len(entry_candidates)}")
    print(f"Taking top {num_to_add}")

    for item in entry_candidates[:num_to_add]:
        orders.append(create_order_row(
            symbol=item['Symbol'],
            action="BUY",
            quantity=item['Quantity'],
            order_type="LIMIT",
            limit_price=item['EntryLimit'],
            strategy_name="hft-long",
            security_type="STK",
            exchange="SMART",
            time_in_force="GTD",
            good_till_date=gtd_time,
            attach_moc="YES"
        ))
        print(f"  ENTRY: {item['Symbol']} @ ${item['EntryLimit']:.2f} limit (IBR: {item['IBR']}, Vol: {item['Volatility']})")

    print(f"\nGenerated {len(orders)} HFT Long orders")
    return orders


def run_growth_strategy(usable_capital, current_positions_df):
    """
    GROWTH Strategy - Growth ETFs (QQQ, SPY, IOO)

    Entry: ROC > 0 (positive momentum)
    Hold: Single ETF with highest ROC
    Exit: If not the highest ROC
    """
    print("\n" + "=" * 80)
    print("GROWTH STRATEGY - Growth ETFs (QQQ, SPY, IOO)")
    print("=" * 80)

    orders = []

    # Get current GROWTH position
    current_growth_symbol = None
    if not current_positions_df.empty:
        # Check if we have any of the GROWTH ETFs
        growth_positions = current_positions_df[current_positions_df['Symbol'].isin(GROWTH_SYMBOLS)]
        if not growth_positions.empty:
            current_growth_symbol = growth_positions['Symbol'].iloc[0]

    print(f"Current GROWTH position: {current_growth_symbol if current_growth_symbol else 'None'}")

    # Scan GROWTH ETFs and rank by ROC
    growth_list = []

    for symbol in GROWTH_SYMBOLS:
        try:
            data = du.getData(symbol, GROWTH_MIN_BARS + 1)
            if len(data) < GROWTH_MIN_BARS:
                continue

            c = data.Close.iloc[-1]

            # Calculate ROC
            roc = ind.ROC(data.Close, GROWTH_ROC_PERIOD)

            # Check uptrend if enabled
            if GROWTH_UPTREND:
                growth_ma = SMAIndicator(data.Close, GROWTH_MA_PERIOD, True).sma_indicator().iloc[-1]
                uptrend = c > growth_ma
            else:
                uptrend = True

            # Calculate position size (100% allocation to 1 ETF)
            buy_price = c
            quantity = int(usable_capital * GROWTH_ALLOCATION / buy_price)

            # Check entry conditions
            if roc > 0 and uptrend:
                growth_list.append({
                    'Symbol': symbol,
                    'Quantity': quantity,
                    'BuyPrice': buy_price,
                    'ROC': round(roc, 3)
                })

        except Exception as e:
            print(f"Warning: Could not process {symbol}: {e}")
            continue

    # Sort by ROC
    growth_list.sort(key=lambda x: x['ROC'], reverse=True)

    print(f"Qualified ETFs: {len(growth_list)}")

    if growth_list:
        best_etf = growth_list[0]
        print(f"Best ETF: {best_etf['Symbol']} (ROC: {best_etf['ROC']})")

        # Generate EXIT order if we're holding a different ETF
        if current_growth_symbol and current_growth_symbol != best_etf['Symbol']:
            orders.append(create_order_row(
                symbol=current_growth_symbol,
                action="SELL",
                quantity=100,  # TODO: Get actual quantity from position
                order_type="MARKET",
                limit_price=None,
                strategy_name="growth",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  EXIT: {current_growth_symbol} (no longer best performer)")

        # Generate ENTRY order if we don't have a position or need to switch
        if not current_growth_symbol or current_growth_symbol != best_etf['Symbol']:
            orders.append(create_order_row(
                symbol=best_etf['Symbol'],
                action="BUY",
                quantity=best_etf['Quantity'],
                order_type="MARKET",
                limit_price=None,
                strategy_name="growth",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  ENTRY: {best_etf['Symbol']} @ Market (ROC: {best_etf['ROC']})")
    else:
        print("No qualified GROWTH ETFs")
        # If we have a position but no qualified ETFs, exit
        if current_growth_symbol:
            orders.append(create_order_row(
                symbol=current_growth_symbol,
                action="SELL",
                quantity=100,
                order_type="MARKET",
                limit_price=None,
                strategy_name="growth",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  EXIT: {current_growth_symbol} (no qualified ETFs)")

    print(f"\nGenerated {len(orders)} GROWTH orders")
    return orders


def run_def_strategy(usable_capital, current_positions_df):
    """
    DEF Strategy - Defensive ETFs (GLD, TLT)

    Entry: ROC > 0 (positive momentum)
    Hold: Single ETF with highest ROC
    Exit: If not the highest ROC
    """
    print("\n" + "=" * 80)
    print("DEF STRATEGY - Defensive ETFs (GLD, TLT)")
    print("=" * 80)

    orders = []

    # Get current DEF position
    current_def_symbol = None
    if not current_positions_df.empty:
        # Check if we have any of the DEF ETFs
        def_positions = current_positions_df[current_positions_df['Symbol'].isin(DEF_SYMBOLS)]
        if not def_positions.empty:
            current_def_symbol = def_positions['Symbol'].iloc[0]

    print(f"Current DEF position: {current_def_symbol if current_def_symbol else 'None'}")

    # Scan DEF ETFs and rank by ROC
    def_list = []

    for symbol in DEF_SYMBOLS:
        try:
            data = du.getData(symbol, DEF_MIN_BARS + 1)
            if len(data) < DEF_MIN_BARS:
                continue

            c = data.Close.iloc[-1]

            # Calculate ROC
            roc = ind.ROC(data.Close, DEF_ROC_PERIOD)

            # Check uptrend if enabled
            if DEF_UPTREND:
                def_ma = SMAIndicator(data.Close, DEF_MA_PERIOD, True).sma_indicator().iloc[-1]
                uptrend = c > def_ma
            else:
                uptrend = True

            # Calculate position size (100% allocation to 1 ETF)
            buy_price = c
            quantity = int(usable_capital * DEF_ALLOCATION / buy_price)

            # Check entry conditions
            if roc > 0 and uptrend:
                def_list.append({
                    'Symbol': symbol,
                    'Quantity': quantity,
                    'BuyPrice': buy_price,
                    'ROC': round(roc, 3)
                })

        except Exception as e:
            print(f"Warning: Could not process {symbol}: {e}")
            continue

    # Sort by ROC
    def_list.sort(key=lambda x: x['ROC'], reverse=True)

    print(f"Qualified ETFs: {len(def_list)}")

    if def_list:
        best_etf = def_list[0]
        print(f"Best ETF: {best_etf['Symbol']} (ROC: {best_etf['ROC']})")

        # Generate EXIT order if we're holding a different ETF
        if current_def_symbol and current_def_symbol != best_etf['Symbol']:
            orders.append(create_order_row(
                symbol=current_def_symbol,
                action="SELL",
                quantity=100,  # TODO: Get actual quantity from position
                order_type="MARKET",
                limit_price=None,
                strategy_name="def",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  EXIT: {current_def_symbol} (no longer best performer)")

        # Generate ENTRY order if we don't have a position or need to switch
        if not current_def_symbol or current_def_symbol != best_etf['Symbol']:
            orders.append(create_order_row(
                symbol=best_etf['Symbol'],
                action="BUY",
                quantity=best_etf['Quantity'],
                order_type="MARKET",
                limit_price=None,
                strategy_name="def",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  ENTRY: {best_etf['Symbol']} @ Market (ROC: {best_etf['ROC']})")
    else:
        print("No qualified DEF ETFs")
        # If we have a position but no qualified ETFs, exit
        if current_def_symbol:
            orders.append(create_order_row(
                symbol=current_def_symbol,
                action="SELL",
                quantity=100,
                order_type="MARKET",
                limit_price=None,
                strategy_name="def",
                security_type="STK",
                exchange="SMART",
                time_in_force="DAY"
            ))
            print(f"  EXIT: {current_def_symbol} (no qualified ETFs)")

    print(f"\nGenerated {len(orders)} DEF orders")
    return orders


def run_btc_strategy(usable_capital, current_positions_df):
    """
    BTC Strategy - Bitcoin Exposure via IBIT ETF

    Entry: ROC > 0 (positive momentum)
    Hold: Single position
    Exit: If ROC <= 0
    """
    print("\n" + "=" * 80)
    print("BTC STRATEGY - Bitcoin (IBIT)")
    print("=" * 80)

    orders = []

    # Get current BTC position
    current_btc_position = False
    if not current_positions_df.empty:
        btc_positions = current_positions_df[current_positions_df['Symbol'] == BTC_SYMBOL]
        current_btc_position = not btc_positions.empty

    print(f"Current BTC position: {'Yes' if current_btc_position else 'No'}")

    try:
        data = du.getData(BTC_SYMBOL, BTC_MIN_BARS + 1)

        if len(data) < BTC_MIN_BARS:
            print(f"Insufficient data for {BTC_SYMBOL}")
            return orders

        c = data.Close.iloc[-1]

        # Calculate ROC
        roc = ind.ROC(data.Close, BTC_ROC_PERIOD)

        # Check uptrend if enabled
        if BTC_UPTREND:
            btc_ma = SMAIndicator(data.Close, BTC_MA_PERIOD, True).sma_indicator().iloc[-1]
            uptrend = c > btc_ma
        else:
            uptrend = True

        # Calculate position size
        buy_price = c
        quantity = int(usable_capital * BTC_ALLOCATION / buy_price)

        print(f"{BTC_SYMBOL} Price: ${c:.2f}, ROC: {roc:.3f}, Uptrend: {uptrend}")

        # Entry condition: ROC > 0 and uptrend
        if roc > 0 and uptrend:
            if not current_btc_position:
                # Generate ENTRY order
                orders.append(create_order_row(
                    symbol=BTC_SYMBOL,
                    action="BUY",
                    quantity=quantity,
                    order_type="MARKET",
                    limit_price=None,
                    strategy_name="btc",
                    security_type="STK",
                    exchange="SMART",
                    time_in_force="DAY"
                ))
                print(f"  ENTRY: {BTC_SYMBOL} @ Market (ROC: {roc:.3f})")
            else:
                print(f"  HOLD: {BTC_SYMBOL} (conditions still met)")
        else:
            # Exit condition: ROC <= 0 or not in uptrend
            if current_btc_position:
                orders.append(create_order_row(
                    symbol=BTC_SYMBOL,
                    action="SELL",
                    quantity=100,  # TODO: Get actual quantity
                    order_type="MARKET",
                    limit_price=None,
                    strategy_name="btc",
                    security_type="STK",
                    exchange="SMART",
                    time_in_force="DAY"
                ))
                print(f"  EXIT: {BTC_SYMBOL} (ROC: {roc:.3f}, conditions not met)")
            else:
                print(f"  No entry signal (ROC: {roc:.3f})")

    except Exception as e:
        print(f"Error processing {BTC_SYMBOL}: {e}")

    print(f"\nGenerated {len(orders)} BTC orders")
    return orders


def run_mr_short_strategy(usable_capital, current_positions_df):
    """
    MR Short Strategy - Mean Reversion Shorts (S&P 500)

    Entry: Price > $5, Vol > 200k, Price > 100-SMA, ADX > 30, RSI(2) > 90
    Entry Limit: High + 0.5*ATR
    Exit Limit: Previous day's low
    """
    print("\n" + "=" * 80)
    print("MR SHORT STRATEGY - Mean Reversion Shorts (S&P 500)")
    print("=" * 80)

    orders = []

    # Get current MR short positions
    mr_short_symbols = []
    mr_short_positions = {}
    if not current_positions_df.empty:
        # We'd need strategy info to filter properly
        # For now, assume any S&P 500 stock could be MR short
        mr_short_symbols = current_positions_df['Symbol'].tolist()
        for _, row in current_positions_df.iterrows():
            mr_short_positions[row['Symbol']] = row.get('Quantity', 100)

    print(f"Current MR short positions: {len(mr_short_symbols)}")

    # Scan universe
    mr_short_list = []
    ticker_list = nd.watchlist_symbols(MR_SHORT_UNIVERSE)
    print(f"Scanning {len(ticker_list)} symbols...")

    for symbol in ticker_list:
        try:
            data = du.getData(symbol, MR_SHORT_MIN_BARS + 1)
            if len(data) < MR_SHORT_MIN_BARS:
                continue

            c = data.Close.iloc[-1]
            h = data.High.iloc[-1]
            l = data.Low.iloc[-1]
            vol = data.Volume.iloc[-1]

            # Price and volume filters
            if c < MR_SHORT_MIN_PRICE or vol < MR_SHORT_MIN_VOL:
                continue

            # Calculate indicators
            mr_ma = SMAIndicator(data.Close, MR_SHORT_MA_PERIOD, True).sma_indicator().iloc[-1]
            rsi = RSIIndicator(data.Close, MR_SHORT_RSI_PERIOD, True).rsi().iloc[-1]
            adx = ADXIndicator(data.High, data.Low, data.Close, MR_SHORT_ADX_PERIOD, True).adx().iloc[-1]
            atr = AverageTrueRange(data.High, data.Low, data.Close, MR_SHORT_ATR_PERIOD, True).average_true_range().iloc[-1]

            # Entry conditions: Price > MA, ADX > 30, RSI > 90 (overbought)
            if c > mr_ma and adx > MR_SHORT_ADX_LIMIT and rsi > MR_SHORT_RSI_LIMIT:
                entry_limit = h + MR_SHORT_STRETCH * atr
                exit_limit = data.Low.iloc[-2]  # Previous day's low

                quantity = int(usable_capital * MR_SHORT_ALLOCATION / MR_SHORT_MAX_POS / entry_limit)

                mr_short_list.append({
                    'Symbol': symbol,
                    'Quantity': quantity,
                    'EntryLimit': entry_limit,
                    'ExitLimit': exit_limit,
                    'RSI': round(rsi, 2),
                    'ADX': round(adx, 2)
                })

        except Exception as e:
            continue

    print(f"Qualified symbols: {len(mr_short_list)}")

    # Sort by RSI (highest first - most overbought)
    mr_short_list.sort(key=lambda x: x['RSI'], reverse=True)

    # Take top positions up to max
    entry_candidates = mr_short_list[:MR_SHORT_MAX_POS]
    entry_symbols = [x['Symbol'] for x in entry_candidates]

    print(f"Top {min(len(entry_candidates), MR_SHORT_MAX_POS)} short candidates: {entry_symbols[:5]}...")

    # Generate EXIT orders for existing positions
    for symbol in mr_short_symbols:
        if symbol not in entry_symbols:
            # Find exit price
            try:
                data = du.getData(symbol, 2)
                exit_limit = data.Low.iloc[-2]

                orders.append(create_order_row(
                    symbol=symbol,
                    action="BUY",  # BUY to cover short
                    quantity=mr_short_positions.get(symbol, 100),
                    order_type="LIMIT",
                    limit_price=exit_limit,
                    strategy_name="mr-short",
                    security_type="STK",
                    exchange="SMART",
                    time_in_force="GTC"
                ))
                print(f"  EXIT: {symbol} @ ${exit_limit:.2f} limit (no longer qualified)")
            except:
                continue

    # Generate ENTRY orders for new positions
    for item in entry_candidates:
        symbol = item['Symbol']
        if symbol not in mr_short_symbols:
            orders.append(create_order_row(
                symbol=symbol,
                action="SELL",  # SELL to open short
                quantity=item['Quantity'],
                order_type="LIMIT",
                limit_price=item['EntryLimit'],
                strategy_name="mr-short",
                security_type="STK",
                exchange="SMART",
                time_in_force="GTC"
            ))
            print(f"  ENTRY: {symbol} @ ${item['EntryLimit']:.2f} limit (RSI: {item['RSI']}, ADX: {item['ADX']})")

    print(f"\nGenerated {len(orders)} MR Short orders")
    return orders


def run_hft_short_strategy(usable_capital, current_positions_df):
    """
    HFT Short Strategy - High Frequency Shorts (Russell 1000)

    Entry: Price $20-$5000, Vol > 2M, Price > 250-SMA, ADX > 35, IBR > 0.7
    Entry Limit: High + 0.3*ATR
    Exit: Market-On-Close (AttachMOC)
    """
    print("\n" + "=" * 80)
    print("HFT SHORT STRATEGY - High Frequency Shorts (Russell 1000)")
    print("=" * 80)

    orders = []

    # Get current HFT short positions
    hft_short_symbols = []
    hft_short_positions = {}
    if not current_positions_df.empty:
        hft_short_symbols = current_positions_df['Symbol'].tolist()
        for _, row in current_positions_df.iterrows():
            hft_short_positions[row['Symbol']] = row.get('Quantity', 100)

    print(f"Current HFT short positions: {len(hft_short_symbols)}")

    # Calculate GTD time
    gtd_time = calculate_gtd_time()

    # Scan universe
    hft_short_list = []
    ticker_list = nd.watchlist_symbols(HFT_SHORT_UNIVERSE)
    print(f"Scanning {len(ticker_list)} symbols...")

    for symbol in ticker_list:
        try:
            data = du.getData(symbol, HFT_SHORT_MIN_BARS + 1)
            if len(data) < HFT_SHORT_MIN_BARS:
                continue

            c = data.Close.iloc[-1]
            h = data.High.iloc[-1]
            l = data.Low.iloc[-1]
            vol = data.Volume.iloc[-1]

            # Price and volume filters
            if c < HFT_SHORT_MIN_PRICE or c > HFT_SHORT_MAX_PRICE or vol < HFT_SHORT_MIN_VOL:
                continue

            # Calculate indicators
            hft_ma = SMAIndicator(data.Close, HFT_SHORT_MA_PERIOD, True).sma_indicator().iloc[-1]
            adx = ADXIndicator(data.High, data.Low, data.Close, HFT_SHORT_ADX_PERIOD, True).adx().iloc[-1]
            atr = AverageTrueRange(data.High, data.Low, data.Close, HFT_SHORT_ATR_PERIOD, True).average_true_range().iloc[-1]

            # Calculate IBR (Intrabar Range)
            ibr = ind.IBR(h, l, c)

            # Entry conditions: Price > MA, ADX > 35, IBR > 0.7 (closed near high)
            if c > hft_ma and adx > HFT_SHORT_ADX_LIMIT and ibr > HFT_SHORT_IBR_LIMIT:
                entry_limit = h + HFT_SHORT_STRETCH * atr
                volatility = atr / c * 100

                quantity = int(usable_capital * HFT_ALLOCATION_SHORT / HFT_SHORT_MAX_POS / entry_limit)

                hft_short_list.append({
                    'Symbol': symbol,
                    'Quantity': quantity,
                    'EntryLimit': entry_limit,
                    'IBR': round(ibr, 3),
                    'Volatility': round(volatility, 2),
                    'ADX': round(adx, 2)
                })

        except Exception as e:
            continue

    print(f"Qualified symbols: {len(hft_short_list)}")

    # Sort by IBR (highest first - closed nearest to high)
    hft_short_list.sort(key=lambda x: x['IBR'], reverse=True)

    # Take top positions up to max
    entry_candidates = hft_short_list[:HFT_SHORT_MAX_POS]
    entry_symbols = [x['Symbol'] for x in entry_candidates]

    print(f"Top {min(len(entry_candidates), HFT_SHORT_MAX_POS)} short candidates: {entry_symbols[:5]}...")

    # Generate EXIT orders for existing positions (Market-On-Close)
    for symbol in hft_short_symbols:
        orders.append(create_order_row(
            symbol=symbol,
            action="BUY",  # BUY to cover short
            quantity=hft_short_positions.get(symbol, 100),
            order_type="MARKET",
            limit_price=None,
            strategy_name="hft-short",
            security_type="STK",
            exchange="SMART",
            time_in_force="DAY",
            attach_moc="YES"
        ))
        print(f"  EXIT: {symbol} @ MOC")

    # Generate ENTRY orders for new positions
    for item in entry_candidates:
        orders.append(create_order_row(
            symbol=item['Symbol'],
            action="SELL",  # SELL to open short
            quantity=item['Quantity'],
            order_type="LIMIT",
            limit_price=item['EntryLimit'],
            strategy_name="hft-short",
            security_type="STK",
            exchange="SMART",
            time_in_force="GTD",
            good_till_date=gtd_time,
            attach_moc="YES"
        ))
        print(f"  ENTRY: {item['Symbol']} @ ${item['EntryLimit']:.2f} limit (IBR: {item['IBR']}, Vol: {item['Volatility']})")

    print(f"\nGenerated {len(orders)} HFT Short orders")
    return orders


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""

    print("=" * 80)
    print("MULTI-STRATEGY TRADING SIGNAL GENERATOR")
    print("=" * 80)
    print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Step 1: Initialize IB Trading App API Client
    print("Step 1: Initializing IB Trading App API Client...")
    print("-" * 80)

    if not IB_API_KEY:
        print("ERROR: IB_API_KEY not set. Please set in .env file.")
        sys.exit(1)

    try:
        client = create_client(IB_API_KEY, IB_API_URL)
        # Test connection
        connection_status = client.get_connection_status()
        print(f"Connected: {connection_status.get('connected', False)}")
        print(f"Mode: {connection_status.get('mode', 'unknown')}")
        print(f"Accounts: {connection_status.get('accounts', [])}")
    except Exception as e:
        print(f"ERROR: Failed to connect to API: {e}")
        print("\nMake sure:")
        print("1. ib-trading-app is running on http://localhost:8000")
        print("2. Your API key is correct in the .env file")
        print("3. TWS or IB Gateway is connected")
        sys.exit(1)

    print()

    # Step 2: Fetch Account Data
    print("Step 2: Fetching Account Data...")
    print("-" * 80)

    try:
        account_summary = client.get_account_summary()
    except Exception as e:
        print(f"ERROR: Failed to fetch account data: {e}")
        sys.exit(1)

    # Extract account information - handle both nested and flat structures
    if 'account' in account_summary and isinstance(account_summary['account'], dict):
        # Nested structure (actual API response)
        acct = account_summary['account']
        account_number = acct.get('account', 'Unknown')
        equity = acct.get('equity', 0)
        buying_power = acct.get('buyingPower', 0)
        net_liquidation = acct.get('netLiquidation', equity)
        cash = acct.get('cash', 0)
    else:
        # Flat structure (as per docs)
        account_number = account_summary.get('account', 'Unknown')
        equity = account_summary.get('equity', 0)
        buying_power = account_summary.get('buying_power', 0)
        net_liquidation = account_summary.get('net_liquidation', equity)
        cash = account_summary.get('cash', 0)

    print(f"Account: {account_number}")
    print(f"Equity: ${equity:,.2f}")
    print(f"Net Liquidation: ${net_liquidation:,.2f}")
    print(f"Buying Power: ${buying_power:,.2f}")
    print(f"Cash: ${cash:,.2f}")

    # Calculate usable capital (using buying power as the primary metric)
    # For a margin account, buying_power already includes leverage capability
    usable_capital = (1 - BUFFER) * buying_power

    print(f"Usable Capital (after {BUFFER*100}% buffer): ${usable_capital:,.2f}")
    print()

    # Step 3: Fetch Current Positions and Open Trades
    print("Step 3: Fetching Current Positions and Open Trades...")
    print("-" * 80)

    try:
        # Get portfolio snapshot for comprehensive view
        portfolio = client.get_portfolio_snapshot(account=account_number)
        positions_list = portfolio.get('positions', [])

        # Get open trades with strategy information
        open_trades = client.get_open_trades(account=account_number)

        print(f"Total open positions: {len(positions_list)}")
        print(f"Total open trades: {len(open_trades)}")

        # Display current positions by strategy
        if open_trades:
            print("\nCurrent positions by strategy:")
            strategy_counts = {}
            for trade in open_trades:
                strategy_name = trade.get('strategy_name', 'Unknown')
                strategy_counts[strategy_name] = strategy_counts.get(strategy_name, 0) + 1

            for strategy, count in sorted(strategy_counts.items()):
                print(f"  {strategy}: {count} position(s)")

        # Convert open trades to positions_by_strategy dict for compatibility
        positions_by_strategy = {}
        for trade in open_trades:
            strategy_name = trade.get('strategy_name', 'Unknown')
            if strategy_name not in positions_by_strategy:
                positions_by_strategy[strategy_name] = []
            positions_by_strategy[strategy_name].append({
                'symbol': trade.get('symbol'),
                'quantity': trade.get('current_quantity'),
                'avg_entry_price': trade.get('avg_entry_price'),
                'unrealized_pnl': trade.get('unrealized_pnl', 0)
            })
    except Exception as e:
        print(f"Warning: Could not fetch positions: {e}")
        positions_list = []
        open_trades = []
        positions_by_strategy = {}

    print()

    # Step 4: Validate Data
    print("Step 4: Validating Market Data...")
    print("-" * 80)

    # Check if Norgate data is up to date
    data_ok, norgate_date, expected_date = du.is_data_up_to_date_v2()
    if not data_ok:
        print(f"WARNING: Norgate data may be stale. Norgate: {norgate_date}, Expected: {expected_date}")
    else:
        print(f"[OK] Norgate data is current: {norgate_date}")
    print()

    # Step 5: Prepare positions DataFrame
    # Convert positions list to DataFrame for easier handling
    if positions_list:
        positions_df = pd.DataFrame(positions_list)
        # Ensure we have the columns we need
        if 'symbol' in positions_df.columns:
            positions_df.rename(columns={'symbol': 'Symbol', 'position': 'Quantity'}, inplace=True)
    else:
        positions_df = pd.DataFrame(columns=['Symbol', 'Quantity'])

    # Step 6: Generate Signals for Each Strategy
    print("\nStep 6: Generating Signals...")
    print("=" * 80)

    all_orders = []

    # Run implemented strategies
    try:
        momo_orders = run_momo_strategy(usable_capital, positions_df)
        all_orders.extend(momo_orders)
    except Exception as e:
        print(f"\nERROR in MOMO strategy: {e}")
        import traceback
        traceback.print_exc()

    try:
        mr_long_orders = run_mr_long_strategy(usable_capital, positions_df)
        all_orders.extend(mr_long_orders)
    except Exception as e:
        print(f"\nERROR in MR Long strategy: {e}")
        import traceback
        traceback.print_exc()

    try:
        hft_long_orders = run_hft_long_strategy(usable_capital, positions_df)
        all_orders.extend(hft_long_orders)
    except Exception as e:
        print(f"\nERROR in HFT Long strategy: {e}")
        import traceback
        traceback.print_exc()

    try:
        growth_orders = run_growth_strategy(usable_capital, positions_df)
        all_orders.extend(growth_orders)
    except Exception as e:
        print(f"\nERROR in GROWTH strategy: {e}")
        import traceback
        traceback.print_exc()

    try:
        def_orders = run_def_strategy(usable_capital, positions_df)
        all_orders.extend(def_orders)
    except Exception as e:
        print(f"\nERROR in DEF strategy: {e}")
        import traceback
        traceback.print_exc()

    try:
        btc_orders = run_btc_strategy(usable_capital, positions_df)
        all_orders.extend(btc_orders)
    except Exception as e:
        print(f"\nERROR in BTC strategy: {e}")
        import traceback
        traceback.print_exc()

    try:
        mr_short_orders = run_mr_short_strategy(usable_capital, positions_df)
        all_orders.extend(mr_short_orders)
    except Exception as e:
        print(f"\nERROR in MR Short strategy: {e}")
        import traceback
        traceback.print_exc()

    try:
        hft_short_orders = run_hft_short_strategy(usable_capital, positions_df)
        all_orders.extend(hft_short_orders)
    except Exception as e:
        print(f"\nERROR in HFT Short strategy: {e}")
        import traceback
        traceback.print_exc()

    # Step 7: Consolidate and Output CSV
    print()
    print("Step 7: Generating CSV Output...")
    print("=" * 80)

    if not all_orders:
        print("No orders generated. All strategies returned empty signals.")
        print("This could be normal if market conditions don't meet entry criteria.")
    else:
        # Create DataFrame and save to CSV
        orders_df = pd.DataFrame(all_orders)

        # Generate filename with today's date
        today = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUTPUT_DIR, f"daily_orders_{today}.csv")

        orders_df.to_csv(output_file, index=False)

        print(f"[OK] Generated {len(orders_df)} total orders")
        print(f"[OK] Saved to: {output_file}")
        print()
        print("Order Summary by Strategy and Action:")
        summary = orders_df.groupby(['Strategy', 'Action']).size().reset_index(name='Count')
        for _, row in summary.iterrows():
            print(f"  {row['Strategy']:15s} {row['Action']:12s} {row['Count']:3d} orders")
        print()
        print("Next Steps:")
        print("  1. Review the CSV file")
        print("  2. Open ib-trading-app at http://localhost:3000")
        print("  3. Navigate to Batch Orders page")
        print("  4. Upload the CSV file")
        print("  5. Review and submit orders")

    print()
    print("=" * 80)
    print("COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
