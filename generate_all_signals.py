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

# Import utils
from utils import indicator_utils as ind
from utils import data_utils as du
from utils import api_utils as api
from ib_api_client import create_client

# ============================================================================
# CONFIGURATION
# ============================================================================

# IB Trading App API Configuration
IB_API_URL = "http://localhost:8000"
IB_USERNAME = os.getenv("IB_USERNAME", "matthewwills")  # Set via environment or change here
IB_PASSWORD = os.getenv("IB_PASSWORD", "")  # Set via environment variable for security

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

# GROWTH Strategy - Growth ETFs
GROWTH_ALLOCATION = 0.10  # 10%
GROWTH_MAX_POS = 1
GROWTH_WORST_RANK = 2
GROWTH_ROC_P1 = 75
GROWTH_ROC_P2 = 150
GROWTH_UP_TREND = True  # Enable uptrend filter
GROWTH_SINCE_TRUE = 5  # Must be true for 5 bars
GROWTH_MIN_BARS = 250
GROWTH_UNIVERSE = ["QQQ", "SPY", "IOO"]

# DEF Strategy - Defensive ETFs
DEF_ALLOCATION = 0.03  # 3%
DEF_MAX_POS = 1
DEF_WORST_RANK = 1
DEF_ROC_P1 = 75
DEF_ROC_P2 = 150
DEF_UP_TREND = True  # Enable uptrend filter
DEF_SINCE_TRUE = 5
DEF_MIN_BARS = 250
DEF_UNIVERSE = ["GLD", "TLT"]

# BTC Strategy - Bitcoin
BTC_ALLOCATION = 0.02  # 2%
BTC_ROC_PERIOD = 40
BTC_UP_TREND = True  # Enable uptrend filter
BTC_SINCE_TRUE = 4  # Must be true for 4 bars
BTC_MIN_BARS = 50
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

# MR Short Strategy - Mean Reversion Shorts (S&P 500)
MR_ALLOCATION_SHORT = 0.15  # 15%
MR_SHORT_MAX_POS = 10
MR_SHORT_RSI_PERIOD = 3
MR_SHORT_RSI_LIMIT = 90  # Overbought
MR_SHORT_STRETCH = 0.8  # Entry at High + 0.8*ATR

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

# HFT Short Strategy - High Frequency Shorts (Russell 1000)
HFT_ALLOCATION_SHORT = 0.25  # 25%
HFT_SHORT_MAX_POS = 15
HFT_SHORT_MIN_PRICE = 20
HFT_SHORT_MAX_PRICE = 5000
HFT_SHORT_IBR_LIMIT = 0.7  # Closed near high of range
HFT_SHORT_STRETCH = 0.3  # Entry at High + 0.3*ATR

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_order_row(symbol, action, quantity, order_type, limit_price, strategy_name,
                     security_type="STK", exchange="SMART", time_in_force="DAY",
                     good_till_date="", attach_moc="NO"):
    """
    Create a standardized order row for CSV output.

    Returns a dictionary with all 17 required CSV columns.
    """
    return {
        "Symbol": symbol,
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

    # Format: 2025-11-12T15:44:00
    return target_time.strftime("%Y-%m-%dT%H:%M:%S")


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
            security_type="CFD",
            exchange="SMART",
            time_in_force="GTD",
            good_till_date=gtd_time,
            attach_moc="YES"
        ))
        print(f"  ENTRY: {item['Symbol']} @ ${item['EntryLimit']:.2f} limit (IBR: {item['IBR']}, Vol: {item['Volatility']})")

    print(f"\nGenerated {len(orders)} HFT Long orders")
    return orders


# TODO: Implement remaining strategies following the patterns above:
# - run_growth_strategy()
# - run_def_strategy()
# - run_btc_strategy()
# - run_mr_short_strategy()
# - run_hft_short_strategy()


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

    # Step 1: Authenticate with IB Trading App API
    print("Step 1: Authenticating with IB Trading App...")
    print("-" * 80)

    if not IB_PASSWORD:
        print("ERROR: IB_PASSWORD not set. Please set environment variable or update script.")
        sys.exit(1)

    try:
        client = create_client(IB_USERNAME, IB_PASSWORD, IB_API_URL)
    except Exception as e:
        print(f"ERROR: Failed to authenticate: {e}")
        sys.exit(1)

    print()

    # Step 2: Fetch Account Data
    print("Step 2: Fetching Account Data...")
    print("-" * 80)

    account_data = api.fetch_ib_account_summary(client)
    if not account_data:
        print("ERROR: Failed to fetch account data")
        sys.exit(1)

    account_info = account_data.get('account', {})
    net_liquidation = account_info.get('netLiquidation', 0)
    buying_power = account_info.get('buyingPower', 0)
    gross_position_value = account_info.get('grossPositionValue', 0)

    print(f"Net Liquidation: ${net_liquidation:,.2f}")
    print(f"Buying Power: ${buying_power:,.2f}")
    print(f"Gross Position Value: ${gross_position_value:,.2f}")

    # Calculate usable capital
    total_capital = buying_power + gross_position_value
    usable_capital = (1 - BUFFER) * total_capital

    print(f"Total Capital: ${total_capital:,.2f}")
    print(f"Usable Capital (after {BUFFER*100}% buffer): ${usable_capital:,.2f}")
    print()

    # Step 3: Fetch Current Positions
    print("Step 3: Fetching Current Positions...")
    print("-" * 80)

    positions_list = api.fetch_ib_positions(client)
    positions_by_strategy = get_current_positions_by_strategy(positions_list)

    print(f"Total open positions: {len(positions_list)}")
    print()

    # Step 4: Validate Data
    print("Step 4: Validating Market Data...")
    print("-" * 80)

    # Check if Norgate data is up to date
    data_ok, norgate_date, expected_date = du.is_data_up_to_date_v2()
    if not data_ok:
        print(f"WARNING: Norgate data may be stale. Norgate: {norgate_date}, Expected: {expected_date}")
    else:
        print(f"✓ Norgate data is current: {norgate_date}")
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

    # TODO: Implement and call remaining strategies:
    print("\n[GROWTH Strategy - TODO: Implement run_growth_strategy()]")
    print("[DEF Strategy - TODO: Implement run_def_strategy()]")
    print("[BTC Strategy - TODO: Implement run_btc_strategy()]")
    print("[MR Short Strategy - TODO: Implement run_mr_short_strategy()]")
    print("[HFT Short Strategy - TODO: Implement run_hft_short_strategy()]")

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

        print(f"✓ Generated {len(orders_df)} total orders")
        print(f"✓ Saved to: {output_file}")
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
