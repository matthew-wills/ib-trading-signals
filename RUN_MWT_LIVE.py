# %% Import Packages
# Import native packages
import os
import pandas as pd
import datetime as dt

# Import 3rd party packages
import norgatedata
import ta

# Import my local files
import utils.indicator_utils as ind

from utils.api_utils import (
    fetch_account_info,
    fetch_open_positions,
    send_signals_to_mongo,
)

from utils.data_utils import (
    getData,
    getData_endDate,
    get_last_friday_of_month,
    get_last_friday_of_previous_month,
)

# Import email utilities
from utils.email_utils import (
    create_status_table,
    create_account_balance_table,
    create_open_positions_table,
    create_orders_table,
    generate_email_header,
    generate_email_footer,
    send_email,
    send_error_email,
)

from dotenv import load_dotenv

load_dotenv()

# %% Set Global Variables
# Initialize Norgate Data API settings
timeseriesformat = "pandas-dataframe"
priceadjust = norgatedata.StockPriceAdjustmentType.CAPITAL
padding_setting = norgatedata.PaddingType.NONE

# API Access Variables
ACCOUNT_API_KEY = os.environ.get("API_KEY_MWT")
accountID = os.environ.get("TS_ACCOUNT_ID_MWT_LIVE")
headers = {"x-api-key": ACCOUNT_API_KEY}
isLive = True

# UTM user name for Open Positions List
utm_userName = "Matthew Wills"

# Email Address
strategy_package_name = "MWT Live Signals"
recipient_list = ["matthewswills1@gmail.com"]
admin_list = ["matthewswills1@gmail.com"]

# Set Strategy Name Variables
mr_strategy_name = "MWT-LIVE-MR-SP500-v1"
mr_strategy_type = "MR"

rot_strategy_name = "MWT-LIVE-MOMO-v1"
rot_strategy_type = "ROT"

hft_strategy_name = "MWT-LIVE-HFT-R1000-v1"
hft_strategy_type = "HFT"

dataOK = True

# %% Set Strategy Variables
##################################################### Strategy Allocations ################################################
# Account Leverage Safety Buffer 0.1 -> 10%
buffer = 0.2

# Rotation Strategy Allocations
momo_allocation = 0.05
growth_allocaiton = 0.10
def_allocation = 0.03
btc_allocation = 0.02

# Mean Reversion Strategy Allocations
mr_allocationLong = 0.15
mr_allocationShort = 0.15

# High Frequency Strategy Allocations
hft_allocationLong = 0.25
hft_allocationShort = 0.25

##################################################### HFT Strategies #####################################################
hft_long_entry_allowed = False
hft_short_entry_allowed = False

# hft common variables
hft_universe = "Russell 1000"
hft_minBars = 251
hft_maPeriod = 250
hft_adxPeriod = 4
hft_adxLimit = 35
hft_volumePeriod = 50
hft_volumeLimit = 2000000
hft_atrPeriod = 5

# hft long variables
hft_long_maxPos = 15
hft_long_minPrice = 10
hft_long_maxPrice = 5000
hft_long_ibrLimit = 0.3
hft_long_stretch = 0.6

# hft short variables
hft_short_maxPos = 15
hft_short_minPrice = 20
hft_short_maxPrice = 5000
hft_short_ibrLimit = 0.7
hft_short_stretch = 0.3

##################################################### MR Strategies #####################################################
mr_long_entry_allowed = False
mr_short_entry_allowed = False

# Common Variables
mr_universe = "S&P 500"
mr_minBars = 200
mr_maPeriod = 100
mr_adxPeriod = 10
mr_adxLimit = 30
mr_atrPeriod = 10
mr_minPrice = 5
mr_volumePeriod = 50
mr_volumeLimit = 200000

# Variables for long
mr_long_maxPos = 10
mr_long_rsiPeriod = 2
mr_long_rsiLimit = 30
mr_long_stretch = 0.5

# Variable for short
mr_short_maxPos = 10
mr_short_rsiPeriod = 3
mr_short_rsiLimit = 90
mr_short_stretch = 0.8

##################################################### Rotational Strategies ###############################################
momo_entry_allowed = True
momo_universe = "NASDAQ 100"
momo_index_symbol = "#NYSEHL"

momo_indexPeriod = 13
momo_maxPos = 3
momo_worstRank = 5
momo_rocP1 = 120
momo_rocP2 = 240
momo_maPeriod = 100
momo_minBars = 250

growth_entry_allowed = True
growth_universe = ["QQQ", "SPY", "IOO"]
growth_maxPos = 1
growth_worstRank = 2
growth_rocP1 = 75
growth_rocP2 = 150
growth_sinceTrue = 5
growth_minBars = 250

def_entry_allowed = True
def_universe = ["GLD", "TLT"]
def_maxPos = 1
def_worstRank = 1
def_rocP1 = 75
def_rocP2 = 150
def_sinceTrue = 5
def_minBars = 250

btc_entry_allowed = True
btc_universe = ["IBIT"]
btc_rocPeriod = 40
btc_sinceTrue = 4
btc_min_bars = 50

# %% Import User Account Informaiton
account_data = fetch_account_info(accountID, isLive, headers)
equity = account_data["Equity"]

# %% Fetch Open Positions
trade_records = fetch_open_positions(isLive, headers)

# Check if trades were successfully fetched
if trade_records:
    print(f"Fetched {len(trade_records)} open positions.")
else:
    print("No open positions found or an error occurred.")

# %% Establish open positions for rotation type strategies
df = pd.DataFrame(trade_records)

#%%

# Filter for the current user
df = df[df["User"] == utm_userName].copy()

totalOpenPosCost = 0
if not df.empty:
    # Convert Quantity to a numeric value from a string so the filtering will work
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Execution Price"] = pd.to_numeric(df["Execution Price"], errors="coerce")

    totalOpenPosCost = (df["Quantity"] * df["Execution Price"]).sum()

    # Extract open momo-based positions
    momoPositions = df.loc[
        (df["Trade Action"] == "BUY")
        & (df["Strategy"] == rot_strategy_name)
        & (df["Quantity"] != 0),
        ["Symbol", "Quantity"],
    ]

    # Filter growth trades
    growthTrades = momoPositions.loc[
        momoPositions["Symbol"].isin(growth_universe)
    ].copy()

    # Filter Def trades
    defTrades = momoPositions.loc[momoPositions["Symbol"].isin(def_universe)].copy()

    # Filter BTC trades
    btcTrades = momoPositions.loc[momoPositions["Symbol"].isin(btc_universe)].copy()

    # Filter momo trades (all remaining trades not in growth or DEF or BTC)
    momoTrades = momoPositions.loc[
        ~momoPositions["Symbol"].isin(growth_universe + btc_universe + def_universe)
    ].copy()

# Extract open positions for MOMO and mr --> for the mr and hft Strategies
momo_positions = set(df[df["Strategy"] == rot_strategy_name]["Symbol"].tolist())
mr_positions = set(df[df["Strategy"] == mr_strategy_name]["Symbol"].tolist())


# %% Determine total size of the capital pool
buyingPower = account_data["BuyingPower"]
totalBuyingPower = buyingPower + totalOpenPosCost
usableCapital = (1 - buffer) * totalBuyingPower

# %% Find dataEndDate of monthly systems
today = dt.date.today()
yesterday = today - dt.timedelta(days=1)

last_friday_current_month = get_last_friday_of_month(today)
last_friday_previous_month = get_last_friday_of_previous_month(today)

if today > last_friday_current_month:
    data_end_date = last_friday_current_month
else:
    data_end_date = last_friday_previous_month
dataEndDate = data_end_date

# %% Run Momo (Stocks) Strategy
# Helper to guarantee a trades DF exists with required cols
def ensure_trades_df(var_name, cols=('Symbol', 'Quantity')):
    g = globals()
    if var_name not in g or not isinstance(g[var_name], pd.DataFrame):
        g[var_name] = pd.DataFrame(columns=list(cols))
    else:
        # ensure required columns exist
        for c in cols:
            if c not in g[var_name].columns:
                g[var_name][c] = pd.Series(dtype='object')
    return g[var_name]

# Make sure all trades DFs exist before any strategy logic runs
momoTrades   = ensure_trades_df('momoTrades',   cols=('Symbol','Quantity'))
growthTrades = ensure_trades_df('growthTrades', cols=('Symbol','Quantity'))
defTrades    = ensure_trades_df('defTrades',    cols=('Symbol','Quantity'))
btcTrades    = ensure_trades_df('btcTrades',    cols=('Symbol','Quantity'))

momo_list = []
momo_tickerList = norgatedata.watchlist_symbols(momo_universe)
indexData = getData_endDate(momo_index_symbol, momo_indexPeriod+5, dataEndDate).Close
momo_bullmkt = False #indexData.iloc[-1] > indexData.iloc[-1 - momo_indexPeriod]

for symbol in momo_tickerList:
    try:
        data = getData_endDate(symbol, momo_minBars + 1, dataEndDate)
        lastDate = data.index[-1]
    except:
        continue

    if len(data) < momo_minBars:
        print('MOMO_Stocks ----------->>    not enough bars for ' + symbol)
        continue
    print('MOMO_Stocks - Scanning   ' + symbol + '   ' + str(data.index[-1]))

    date = data.index[-1].strftime("%d/%m/%Y")
    c = data.Close.iloc[-1]
    momo_ma = ta.trend.SMAIndicator(data.Close, momo_maPeriod, True).sma_indicator().iloc[-1]
    momo_upTrend = c > momo_ma
    momo_factor = 0.5*ind.ROC(data.Close, momo_rocP1) + 0.5*ind.ROC(data.Close, int(momo_rocP2))

    momo_buyPrice = c
    momo_quantity = int(usableCapital * momo_allocation / momo_maxPos / momo_buyPrice)

    rankingEntry = [date, symbol, momo_quantity, momo_buyPrice, round(momo_factor, 3)]
    if (
        momo_bullmkt 
        and momo_factor > 0
        and momo_upTrend
        ):
        momo_list.append(rankingEntry)

sorted_momo_list = sorted(momo_list, key=lambda x: x[4], reverse=True)[:momo_worstRank]

# Generate Entries and exits for momo System

# Create a DataFrame from the sorted momo list
columns = ['Date', 'Symbol', 'Quantity', 'BuyPrice', 'momoFactor']
momo_df = pd.DataFrame(sorted_momo_list, columns=columns)


# Get currently held momo positions
current_positions = momoTrades['Symbol'].tolist()

# Identify symbols in the top 5
hold_symbols = momo_df['Symbol'].head(momo_worstRank).tolist()

# Identify symbols in the top 3
entry_symbols = momo_df['Symbol'].head(momo_maxPos).tolist()

# Determine Sell Orders
momo_sell_orders = []
for _, row in momoTrades.iterrows():
    symbol = row['Symbol']
    current_quantity = int(row['Quantity'])  # Fetch current holding quantity
    if symbol not in hold_symbols and current_quantity > 0:  # Sell if the symbol is not in the top 5
        momo_sell_orders.append([
            symbol,                   # symbol
            "SELL",                   # tradeAction
            current_quantity,         # quantity matches current holdings
            None,                     # limitPrice
            "OPG",                    # duration
            "Market",                 # orderType
            'false'                   # allOrNone
        ])

# Determine Buy Orders
momo_buy_orders = []
if momo_entry_allowed:
    for _, row in momo_df.iterrows():
        symbol = row['Symbol']
        if symbol in entry_symbols and symbol not in current_positions:  # Buy only if in top 3 and not already held
            momo_buy_orders.append([
                symbol,                   # symbol
                "BUY",                    # tradeAction
                int(row['Quantity']),     # quantity
                None,                     # limitPrice
                "OPG",                    # duration
                "Market",                 # orderType
                'false'                   # allOrNone
            ])

# Combine sell and buy orders into a single DataFrame
columns = ['symbol', 'tradeAction', 'quantity', 'limitPrice', 'duration', 'orderType', 'allOrNone']
momo_orders = pd.DataFrame(momo_sell_orders + momo_buy_orders, columns=columns)

#%% Run Growth (ETFs) Strategy
growth_list = []  # Initialize as an empty list

# Fetch and process data for each symbol in the growth universe
for symbol in growth_universe:
    try:
        data = getData(symbol,growth_minBars)#getData_endDate(symbol, growth_minBars, dataEndDate)  # Fetch growth data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        continue

    if len(data) < growth_minBars:
        # Not enough bars
        print(f"Not enough data for {symbol}")
        continue

    try:
        date = data.index[-1].strftime("%d/%m/%Y")
        c = data.Close.iloc[-1]
        data['growth_factor'] = data['Close'].pct_change(periods=growth_rocP1)*100 + data['Close'].pct_change(periods=growth_rocP2)*100
        growth_upTrend = False#(data['growth_factor'].iloc[-growth_sinceTrue:] > 0).all()

        growth_buyPrice = c
        growth_quantity = int(usableCapital * growth_allocaiton / growth_maxPos / growth_buyPrice)

        # print(f"\nSymbol: {symbol}")
        # print(f"  Date: {date}")
        # print(f"  Close Price (c): {c}")
        # print(f"  Growth Factor (latest): {round(data['growth_factor'].iloc[-1], 3)}")
        # print(f"  Growth UpTrend: {growth_upTrend}")
        # print(f"  Quantity to Buy: {growth_quantity}")
        # last_5_growth_factors = data['growth_factor'].iloc[-5:].round(2).tolist()
        # print(f"  Last 5 Growth Factors: {last_5_growth_factors}")

        # Ensure all values are valid before appending
        if c > 0 and growth_quantity > 0 and growth_upTrend:
            growth_list.append([date, symbol, growth_quantity, growth_buyPrice, round(data.growth_factor.iloc[-1], 3)])
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

# Sort the processed growth list
sorted_growth_list = sorted(growth_list, key=lambda x: x[4], reverse=True)[:growth_worstRank]

## Generate Entry and Exit Signals for growth Strategy
# Create a DataFrame from the sorted growth list
columns = ['Date', 'Symbol', 'Quantity', 'BuyPrice', 'growthFactor']
growth_df = pd.DataFrame(sorted_growth_list, columns=columns)

# Get currently held growth positions
current_positions = growthTrades['Symbol'].tolist()

# Identify symbols in the top (growth_maxPos + growth_worstRank - 1)
hold_symbols = growth_df['Symbol'].head(growth_worstRank).tolist()

# Identify symbols in the top growth_maxPos
entry_symbols = growth_df['Symbol'].head(growth_maxPos).tolist()

# Determine Sell Orders
growth_sell_orders = []
for _, row in growthTrades.iterrows():
    symbol = row['Symbol']
    current_quantity = int(row['Quantity'])  # Fetch current holding quantity
    if symbol not in hold_symbols and current_quantity > 0:  # Sell if the symbol is not in the top acceptable
        growth_sell_orders.append([
            symbol,                   # symbol
            "SELL",                   # tradeAction
            current_quantity,         # quantity matches current holdings
            None,                     # limitPrice
            "OPG",                    # duration
            "Market",                 # orderType
            'false'                   # allOrNone
        ])

# Determine Buy Orders
growth_buy_orders = []
if growth_entry_allowed:
    for _, row in growth_df.iterrows():
        symbol = row['Symbol']
        if symbol in entry_symbols and symbol not in current_positions:  # Buy only if in top positions and not already held
            growth_buy_orders.append([
                symbol,                   # symbol
                "BUY",                    # tradeAction
                int(row['Quantity']),     # quantity
                None,                     # limitPrice
                "OPG",                    # duration
                "Market",                 # orderType
                'false'                   # allOrNone
            ])

# Combine sell and buy orders into a single DataFrame
columns = ['symbol', 'tradeAction', 'quantity', 'limitPrice', 'duration', 'orderType', 'allOrNone']
growth_orders = pd.DataFrame(growth_sell_orders + growth_buy_orders, columns=columns)

#%% Run Def Strategy
def_list = []  # Initialize as an empty list

# Fetch and process data for each symbol in the growth universe
for symbol in def_universe:
    try:
        data = getData_endDate(symbol, def_minBars, dataEndDate)  # Fetch def data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        continue

    if len(data) < def_minBars:
        # Not enough bars
        print(f"Not enough data for {symbol}")
        continue

    try:
        date = data.index[-1].strftime("%d/%m/%Y")
        c = data.Close.iloc[-1]
        data['def_factor'] = data['Close'].pct_change(periods=def_rocP1)*100 + data['Close'].pct_change(periods=def_rocP2)*100 
        def_upTrend = False#(data['def_factor'].iloc[-def_sinceTrue:] > 0).all()

        def_buyPrice = c
        def_quantity = int(usableCapital * def_allocation / def_maxPos / def_buyPrice)

        # Ensure all values are valid before appending
        if c > 0 and def_quantity > 0 and def_upTrend:
            def_list.append([date, symbol, def_quantity, def_buyPrice, round(data.def_factor.iloc[-1], 3)])
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

# Sort the processed def list
sorted_def_list = sorted(def_list, key=lambda x: x[4], reverse=True)[:def_worstRank]

## Generate Entry and Exit Signals for def Strategy
# Create a DataFrame from the sorted def list
columns = ['Date', 'Symbol', 'Quantity', 'BuyPrice', 'defFactor']
def_df = pd.DataFrame(sorted_def_list, columns=columns)

# Get currently held def positions

current_positions = defTrades['Symbol'].tolist()

# Identify symbols in the top (def_maxPos + def_worstRank - 1)
hold_def_symbols = def_df['Symbol'].head(def_worstRank).tolist()

# Identify symbols in the top def_maxPos
entry_def_symbols = def_df['Symbol'].head(def_maxPos).tolist()

# Determine Sell Orders
def_sell_orders = []
for _, row in defTrades.iterrows():
    symbol = row['Symbol']
    current_quantity = int(row['Quantity'])  # Fetch current holding quantity
    if symbol not in hold_def_symbols and current_quantity > 0:  # Sell if the symbol is not in the top acceptable
        def_sell_orders.append([
            symbol,                   # symbol
            "SELL",                   # tradeAction
            current_quantity,         # quantity matches current holdings
            None,                     # limitPrice
            "OPG",                    # duration
            "Market",                 # orderType
            'false'                   # allOrNone
        ])

# Determine Buy Orders
def_buy_orders = []
if def_entry_allowed:
    for _, row in def_df.iterrows():
        symbol = row['Symbol']
        if symbol in entry_def_symbols and symbol not in current_positions:  # Buy only if in top positions and not already held
            def_buy_orders.append([
                symbol,                   # symbol
                "BUY",                    # tradeAction
                int(row['Quantity']),     # quantity
                None,                     # limitPrice
                "OPG",                    # duration
                "Market",                 # orderType
                'false'                   # allOrNone
            ])

# Combine sell and buy orders into a single DataFrame
columns = ['symbol', 'tradeAction', 'quantity', 'limitPrice', 'duration', 'orderType', 'allOrNone']
def_orders = pd.DataFrame(def_sell_orders + def_buy_orders, columns=columns)

#%% Run BTC Strategy
btc_list = []
# Fetch BTC data
try:
    btc_data = getData_endDate(btc_universe[0], btc_min_bars, dt.date.today())  # Fetch enough data to evaluate conditions
except Exception as e:
    print(f"Error fetching BTC data: {e}")
    btc_data = pd.DataFrame()  # Default to an empty DataFrame if data cannot be fetched

if not data.empty:
    # Calculate the 100-day ROC
    btc_data['btc_factor'] = btc_data['Close'].pct_change(periods=btc_rocPeriod) * 100  # ROC as a percentage

    # Check if ROC(C,100) > 0 for all of the last x days
    btc_upTrend = False#(btc_data['btc_factor'].iloc[-btc_sinceTrue:] > 0).all()

    # Determine the number of shares for BUY orders
    btc_quantity = int(usableCapital * btc_allocation / btc_data['Close'].iloc[-1])

    # Determine if we should currently be in a position
    btc_trade_action = None
    if btc_upTrend:  # Uptrend: Should be in a position
        if btcTrades.empty and btc_entry_allowed:  # Not currently in a position
            btc_trade_action = "BUY"
            btc_list.append([
                btc_universe[0],   # symbol
                btc_trade_action,
                btc_quantity,      # number of shares to buy
                None,           # limitPrice
                "OPG",          # duration
                "Market",       # orderType
                'false'         # allOrNone
            ])
    else:  # No Uptrend: Should be flatbtc
        if not btcTrades.empty:  # Currently in a position
            # Get the quantity of the open position from btcTrades
            current_quantity = int(btcTrades['Quantity'].sum())  # Sum in case of multiple positions
            if current_quantity > 0:
                btc_trade_action = "SELL"
                btc_list.append([
                    btc_universe[0],   # symbol
                    btc_trade_action,
                    current_quantity,  # number of shares to sell
                    None,              # limitPrice
                    "OPG",             # duration
                    "Market",          # orderType
                    'false'            # allOrNone
                ])

# Create a DataFrame for BTC orders
columns = ['symbol', 'tradeAction', 'quantity', 'limitPrice', 'duration', 'orderType', 'allOrNone']
btc_orders = pd.DataFrame(btc_list, columns=columns)

# %% Combine rotatation strategy orders into a single DataFrame
ROT_dataFrame = pd.concat(
    [momo_orders, growth_orders, def_orders, btc_orders], ignore_index=True
)

if len(ROT_dataFrame) > 0:
    todaysDate = str(dt.date.today().strftime("%d-%m-%Y"))
    ROT_dataFrame.to_csv(
        "history/" + rot_strategy_name + " - " + todaysDate + ".csv", index=False
    )

# %% Run MR Strategy
# Generate Exit Signals from open positions
# Create a DataFrame from the trade records
df = pd.DataFrame(trade_records)

if not df.empty:
    # Convert Quantity to a numeric value from a string so the filtering will work
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")

    # Filter SHORT positions
    shortPositions = df[
        (df["Trade Action"] == "SELLSHORT")
        & (df["Strategy"] == mr_strategy_name)
        & (df["Quantity"] != 0)
    ][["Symbol", "Quantity"]]

    # Filter LONG positions
    longPositions = df[
        (df["Trade Action"] == "BUY")
        & (df["Strategy"] == mr_strategy_name)
        & (df["Quantity"] != 0)
    ][["Symbol", "Quantity"]]

    # Rename 'Quantity' to 'Position'
    longPositions.rename({"Quantity": "Position"}, axis=1, inplace=True)
    shortPositions.rename({"Quantity": "Position"}, axis=1, inplace=True)

    # Exit Signals Long
    exitOrderListLong = []
    for i in range(len(longPositions)):
        symbol = longPositions.iloc[i].Symbol
        prevHigh = str(round(getData(symbol, 3).High.iloc[-1], 2))
        exitOrderListLong.append(
            [
                symbol,
                "SELL",
                longPositions.iloc[i].Position,
                prevHigh,
                "GTC",
                "Limit",
                "false",
            ]
        )
    numOpenPosLong = len(exitOrderListLong)

    # Exit Signals Short
    exitOrderListShort = []
    for i in range(len(shortPositions)):
        symbol = shortPositions.iloc[i].Symbol
        prevLow = str(round(getData(symbol, 3).Low.iloc[-1], 2))
        exitOrderListShort.append(
            [
                symbol,
                "BUYTOCOVER",
                int(shortPositions.iloc[i].Position),
                prevLow,
                "GTC",
                "Limit",
                "false",
            ]
        )
    numOpenPosShort = len(exitOrderListShort)
else:
    # Handle the case where there are no open positions
    exitOrderListLong = []
    exitOrderListShort = []
    numOpenPosLong = 0
    numOpenPosShort = 0

# Generate Entry Signals
# Scan for Long signals - Long mr positions and Orders will not conflict with MOMO therefore no addtional checks required
mr_tickerList = norgatedata.watchlist_symbols(mr_universe)
tradeListLong = []
if mr_long_entry_allowed:
    for symbol in mr_tickerList:
        # check if symbol is allowed
        if symbol == "GOOG":
            continue
        # Check if already in a long_mr_position
        inPosFlag = False
        for i in range(len(exitOrderListLong)):
            if symbol == exitOrderListLong[i][0]:
                inPosFlag = True
                break
        if inPosFlag:
            continue
        # attempt to call data from norgate
        try:
            data = getData(symbol, mr_minBars + 1)
        except:
            continue
        # are there enough bars
        if len(data) < mr_minBars:
            print("MR_SP500 ----------->>    not enough bars for " + symbol)
            continue
        print("MR_SP500 - Scanning   " + symbol + "   " + str(data.index[-1]))

        try:
            date = data.index[-1]
            c = data.Close.iloc[-1]
            l = data.Low.iloc[-1]
            h = data.High.iloc[-1]

            mr_atr = (
                ta.volatility.AverageTrueRange(
                    data.High, data.Low, data.Close, mr_atrPeriod, True
                )
                .average_true_range()
                .iloc[-1]
            )
            mr_ma = (
                ta.trend.SMAIndicator(data.Close, mr_maPeriod, True)
                .sma_indicator()
                .iloc[-1]
            )
            mr_adx = (
                ta.trend.ADXIndicator(
                    data.High, data.Low, data.Close, mr_adxPeriod, fillna=True
                )
                .adx()
                .iloc[-1]
            )
            mr_avgVolume = (
                ta.trend.SMAIndicator(data.Volume, mr_volumePeriod, True)
                .sma_indicator()
                .iloc[-1]
            )
            mr_volatility = mr_atr / c * 100

            mr_long_rsi = (
                ta.momentum.RSIIndicator(data.Close, mr_long_rsiPeriod, True)
                .rsi()
                .iloc[-1]
            )
            mr_long_entryLimit = round(l - mr_long_stretch * mr_atr, 2)

            mr_long_quantity = max(
                1,
                int(
                    mr_allocationLong
                    * usableCapital
                    / mr_long_maxPos
                    / mr_long_entryLimit
                ),
            )

        except:
            print("----------->>    unable to process indicators for " + symbol)
            continue

        # Check for Long Setup
        if (
            c > mr_minPrice
            and mr_avgVolume > mr_volumeLimit
            and c > mr_ma
            and mr_adx > mr_adxLimit
            and mr_long_rsi < mr_long_rsiLimit
        ):
            mr_long_rank = round(mr_volatility, 3)
            entryLong = [
                symbol,
                "BUY",
                mr_long_quantity,
                mr_long_entryLimit,
                0,
                0,
                mr_long_rank,
                date,
            ]
            tradeListLong.append(entryLong)

# Scan for Short signals - Short mr Orders will conflict with MOMO long positions or orders - a check is required here
tradeListShort = []
if mr_short_entry_allowed:
    for symbol in mr_tickerList:
        # Check if the symbol is not allowed (e.g., special exclusions)
        if symbol == "GOOG":
            continue

        # Skip if the symbol is already in an open mr_short_position
        inPosFlag = False
        for i in range(len(exitOrderListShort)):
            if symbol == exitOrderListShort[i][0]:
                inPosFlag = True
                break
        if inPosFlag:
            continue

        # Skip if the symbol is in MOMO positions or has a BUY order in MOMO orders
        if symbol in momo_positions or any(
            momo_order[0] == symbol and momo_order[1] == "BUY"
            for momo_order in ROT_dataFrame.values.tolist()
        ):
            print(f"{symbol} --- MOMO conflict (LONG POSITION or ORDER)")
            continue

        # Fetch data and process indicators
        try:
            data = getData(symbol, mr_minBars + 1)
        except:
            continue

        # Ensure there are enough bars
        if len(data) < mr_minBars:
            print(f"----------->>    not enough bars for {symbol}")
            continue

        print(f"MR_SP500 - scanning   {symbol}   {str(data.index[-1])}")

        try:
            date = data.index[-1]
            c = data.Close.iloc[-1]
            l = data.Low.iloc[-1]
            h = data.High.iloc[-1]

            mr_atr = (
                ta.volatility.AverageTrueRange(
                    data.High, data.Low, data.Close, mr_atrPeriod, True
                )
                .average_true_range()
                .iloc[-1]
            )
            mr_ma = (
                ta.trend.SMAIndicator(data.Close, mr_maPeriod, True)
                .sma_indicator()
                .iloc[-1]
            )
            mr_adx = (
                ta.trend.ADXIndicator(
                    data.High, data.Low, data.Close, mr_adxPeriod, fillna=True
                )
                .adx()
                .iloc[-1]
            )
            mr_avgVolume = (
                ta.trend.SMAIndicator(data.Volume, mr_volumePeriod, True)
                .sma_indicator()
                .iloc[-1]
            )
            mr_volatility = mr_atr / c * 100

            mr_short_rsi = (
                ta.momentum.RSIIndicator(data.Close, mr_short_rsiPeriod, True)
                .rsi()
                .iloc[-1]
            )
            mr_short_entryLimit = round(h + mr_short_stretch * mr_atr, 2)

            mr_short_quantity = max(
                1,
                int(
                    mr_allocationShort
                    * usableCapital
                    / mr_short_maxPos
                    / mr_short_entryLimit
                ),
            )
        except:
            print(f"----------->>    unable to process indicators for {symbol}")
            continue

        # Check for Short Setup
        if (
            c > mr_minPrice
            and mr_avgVolume > mr_volumeLimit
            and c > mr_ma
            and mr_adx > mr_adxLimit
            and mr_short_rsi > mr_short_rsiLimit
        ):
            mr_short_rank = round(mr_volatility, 3)
            entryShort = [
                symbol,
                "SELLSHORT",
                mr_short_quantity,
                mr_short_entryLimit,
                0,
                0,
                mr_short_rank,
                date,
            ]
            tradeListShort.append(entryShort)

# Sort Long Orders
sortedListLong = sorted(tradeListLong, key=lambda x: (x[6]), reverse=True)[
    0 : max(0, mr_long_maxPos - numOpenPosLong)
]
tradeListLong = []
for i in range(len(sortedListLong)):
    tradeListLong.append(sortedListLong[i][:4] + ["GTC", "Limit", "false"])

# Sort Short Orders
sortedListShort = sorted(tradeListShort, key=lambda x: (x[6]), reverse=True)[
    0 : max(0, mr_short_maxPos - numOpenPosShort)
]
tradeListShort = []
for i in range(len(sortedListShort)):
    tradeListShort.append(sortedListShort[i][:4] + ["GTC", "Limit", "false"])

# Combine long and short orders into dataframe
mr_ordersList = exitOrderListLong + tradeListLong + exitOrderListShort + tradeListShort
mr_dataFrame = pd.DataFrame(mr_ordersList, columns=columns)
if len(mr_ordersList) > 0:
    todaysDate = str(dt.date.today().strftime("%d-%m-%Y"))
    mr_dataFrame.to_csv(
        "history/" + mr_strategy_name + " - " + todaysDate + ".csv", index=False
    )

# %% Run HFT System
hft_tickerList = norgatedata.watchlist_symbols(hft_universe)
hft_tradeListLong = []
hft_tradeListShort = []

for symbol in hft_tickerList:
    if symbol == "GOOG":
        continue

    try:
        data = getData(symbol, hft_minBars + 1)
    except:
        continue

    if len(data) < hft_minBars:
        print(f"----------->> Not enough bars for {symbol}")
        continue

    print(f"HFT_R1000 - Scanning {symbol} {data.index[-1]}")

    try:
        close = data.Close.iloc[-1]
        low = data.Low.iloc[-1]
        high = data.High.iloc[-1]

        hft_avgVolume = (
            ta.trend.EMAIndicator(data.Volume, hft_volumePeriod, True)
            .ema_indicator()
            .iloc[-1]
        )
        hft_ma = (
            ta.trend.SMAIndicator(data.Close, hft_maPeriod, True)
            .sma_indicator()
            .iloc[-1]
        )
        hft_adx = (
            ta.trend.ADXIndicator(
                data.High, data.Low, data.Close, hft_adxPeriod, fillna=True
            )
            .adx()
            .iloc[-1]
        )
        hft_atr = (
            ta.volatility.AverageTrueRange(
                data.High, data.Low, data.Close, hft_atrPeriod, True
            )
            .average_true_range()
            .iloc[-1]
        )
        hft_ibr = ind.IBR(data.High.iloc[-1], data.Low.iloc[-1], data.Close.iloc[-1])
        hft_volatility = hft_atr / close * 100
    except:
        print(f"----------->>HFT - Unable to process indicators for {symbol}")
        continue

    rank = hft_volatility
    # Scanning for Long Setups
    if (
        close > hft_long_minPrice
        and close < hft_long_maxPrice
        and hft_avgVolume > hft_volumeLimit
        and close > hft_ma
        and hft_adx > hft_adxLimit
        and hft_ibr < hft_long_ibrLimit
    ):
        hft_tradeListLong.append([rank, symbol, hft_atr, low])

    # Scanning for Short Setups
    if (
        close > hft_short_minPrice
        and close < hft_short_maxPrice
        and hft_avgVolume > hft_volumeLimit
        and close > hft_ma
        and hft_adx > hft_adxLimit
        and hft_ibr > hft_short_ibrLimit
    ):
        hft_tradeListShort.append([rank, symbol, hft_atr, high])

hft_sortedListLong = sorted(hft_tradeListLong, key=lambda x: x[0], reverse=True)
hft_sortedListShort = sorted(hft_tradeListShort, key=lambda x: x[0], reverse=True)

# Generate the hft_long Tradelist and check against mr positions and orders
hft_orderListLong = []
if hft_long_entry_allowed:
    placedOrderCount = 0
    for trade in hft_sortedListLong:
        rank, symbol, atr, low = trade

        # Skip if there's a conflict with mr short positions or orders
        skipFlag = False
        for order in mr_ordersList:
            if symbol == order[0] and (
                order[1] == "BUYTOCOVER" or order[1] == "SELLSHORT"
            ):
                print(f"{symbol} -------- MR-SHORT POS or ORDER conflict")
                skipFlag = True
                break
        if skipFlag:
            continue

        tick = ind.tickSize(low)
        hft_long_entryLimit = round(
            round((low - hft_long_stretch * atr) / tick, 0) * tick, 3
        )
        hft_long_quantity = max(
            1,
            int(
                (
                    usableCapital
                    * hft_allocationLong
                    / hft_long_maxPos
                    / hft_long_entryLimit
                )
                - 1
            ),
        )
        if placedOrderCount == hft_long_maxPos:
            break
        hft_orderListLong.append(
            [
                symbol,
                "BUY",
                hft_long_quantity,
                hft_long_entryLimit,
                "GTC",
                "Limit",
                "false",
            ]
        )
        placedOrderCount += 1

# Generate the hft_Short Tradelist and check against MR and MOMO conflicts
hft_orderListShort = []
if hft_short_entry_allowed:
    placedOrderCount = 0
    for trade in hft_sortedListShort:
        rank, symbol, atr, high = trade

        # Skip if there's a conflict with MR long positions or orders
        skipFlag = False
        for order in mr_ordersList:
            if symbol == order[0] and (order[1] == "SELL" or order[1] == "BUY"):
                print(f"{symbol} -------- MR-LONG POS or ORDER conflict")
                skipFlag = True
                break
        # Skip if there's a conflict with MOMO long positions or orders
        if symbol in momo_positions or any(
            order[0] == symbol and order[1] == "BUY"
            for order in ROT_dataFrame.values.tolist()
        ):
            print(f"{symbol} -------- MOMO LONG POS or ORDER conflict")
            skipFlag = True

        if skipFlag:
            continue

        tick = ind.tickSize(high)
        hft_short_entryLimit = round(
            round((high + hft_short_stretch * atr) / tick, 0) * tick, 3
        )
        hft_short_quantity = max(
            1,
            int(
                (
                    usableCapital
                    * hft_allocationShort
                    / hft_short_maxPos
                    / hft_short_entryLimit
                )
                - 1
            ),
        )
        if placedOrderCount == hft_short_maxPos:
            break
        hft_orderListShort.append(
            [
                symbol,
                "SELLSHORT",
                hft_short_quantity,
                hft_short_entryLimit,
                "GTC",
                "Limit",
                "false",
            ]
        )
        placedOrderCount += 1

# Combine long and short hft orders into dataframe
hft_ordersList = hft_orderListLong + hft_orderListShort
hft_dataFrame = pd.DataFrame(hft_ordersList, columns=columns)
if hft_ordersList:
    todaysDate = str(dt.date.today().strftime("%d-%m-%Y"))
    hft_dataFrame.to_csv(f"history/{hft_strategy_name} - {todaysDate}.csv", index=False)

# %% Send Signals to Mongo Database
# Fetch the last 2 days of data for SPY from Norgate Data
spy_data = getData("SPY", 2)
spyDt = spy_data.index[-1].date()

# Initialize MongoDB success flag
mongoOK = False

# MOMO signals
momo_sent = send_signals_to_mongo(
    strategy_name=rot_strategy_name,
    strategy_type=rot_strategy_type,
    signals_df=ROT_dataFrame,
    strategy_id=os.environ.get(rot_strategy_name),
    is_live=isLive,
    api_key=ACCOUNT_API_KEY,
    spy_date=spyDt,
    spy_date_name=spyDt.strftime("%A"),
)
mongoOK = mongoOK or momo_sent

# Send MR signals
mr_sent = send_signals_to_mongo(
    strategy_name=mr_strategy_name,
    strategy_type=mr_strategy_type,
    signals_df=mr_dataFrame,
    strategy_id=os.environ.get(mr_strategy_name),
    is_live=isLive,
    api_key=ACCOUNT_API_KEY,
    spy_date=spyDt,
    spy_date_name=spyDt.strftime("%A"),
)
mongoOK = mongoOK or mr_sent

# Send hft signals
hft_sent = send_signals_to_mongo(
    strategy_name=hft_strategy_name,
    strategy_type=hft_strategy_type,
    signals_df=hft_dataFrame,
    strategy_id=os.environ.get(hft_strategy_name),
    is_live=isLive,
    api_key=ACCOUNT_API_KEY,
    spy_date=spyDt,
    spy_date_name=spyDt.strftime("%A"),
)
mongoOK = mongoOK or hft_sent

mongoOK = True

# %% Build Table for Open Positions in Email
# Convert to DataFrame
df = pd.DataFrame(trade_records)

# Filter only this user's positions and known strategy names
my_strategies = [mr_strategy_name, rot_strategy_name, hft_strategy_name]
df = df[(df["User"] == utm_userName) & (df["Strategy"].isin(my_strategies))].copy()

# Ensure correct types and format
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df["OpenedDateTime"] = pd.to_datetime(df["Opened DateTime"], errors="coerce")

# Build the table of open positions
open_positions_table_data = []

for _, row in df.iterrows():
    symbol = row["Symbol"]
    strategy = row["Strategy"]
    action = row["Trade Action"]
    qty = row["Quantity"]
    entry_price = row["Execution Price"]
    entry_date = (
        row["OpenedDateTime"].date().strftime("%Y-%m-%d")
        if pd.notnull(row["OpenedDateTime"])
        else "Unknown"
    )

    if not entry_price or qty == 0:
        continue

    try:
        data = getData(symbol, 2)
        last_close = float(data.Close.iloc[-1])
    except:
        last_close = entry_price  # fallback if price data fails

    if action == "BUY":
        open_pnl = (last_close - entry_price) * qty
        pnl_pct = ((last_close - entry_price) / entry_price) * 100
    elif action == "SELLSHORT":
        open_pnl = (entry_price - last_close) * qty
        pnl_pct = ((entry_price - last_close) / entry_price) * 100
    else:
        open_pnl = 0
        pnl_pct = 0

    open_positions_table_data.append(
        [
            symbol,
            strategy,
            action,
            int(qty),
            entry_price,
            last_close,
            open_pnl,
            pnl_pct,
            entry_date,
        ]
    )

strategy_name_map = {
    mr_strategy_name: mr_strategy_type,
    rot_strategy_name: rot_strategy_type,
    hft_strategy_name: hft_strategy_type,
}

# %% Send Email
# Set recipient list and email subject
email_subject = f"{strategy_package_name} - {dt.date.today().strftime('%d/%m/%Y')}"

# Generate Email Content
header_html = generate_email_header("Strategy: " + strategy_package_name)
footer_html = generate_email_footer()

# Generate Account Balance and Orders Tables
account_balance_html = create_account_balance_table(
    {
        "AccountID": account_data["AccountID"],
        "AccountType": account_data["AccountType"],
        "Equity": equity,
        "CashBalance": account_data["CashBalance"],
        "MarketValue": account_data["MarketValue"],
        "TotalCost": totalOpenPosCost,
        "BuyingPower": account_data["BuyingPower"],
        "UsableCapital": totalBuyingPower,
        "RequiredMargin": account_data["BalanceDetail"]["RequiredMargin"],
    }
)

open_positions_html = create_open_positions_table(
    open_positions_table_data, "OPEN POSITIONS LIST", strategy_name_map
)

momo_order_list_html = create_orders_table(
    ROT_dataFrame.values.tolist(), rot_strategy_type
)
mr_order_list_html = create_orders_table(mr_ordersList, mr_strategy_type)
hft_order_list_html = create_orders_table(hft_ordersList, hft_strategy_type)

# Generate System Checks Table
system_checks_html = create_status_table(True, mongoOK)

# Build Email Body
email_body = f"""
{header_html}
{system_checks_html}
{account_balance_html}
{open_positions_html}
{momo_order_list_html}
{mr_order_list_html}
{hft_order_list_html}
{footer_html}
"""

# Send Email
if not mongoOK:
    # Format detailed error message
    error_message = f"""
    <p style='color: #dc3545;'>System checks indicate a problem:</p>
    <p><strong>MongoDB OK:</strong> {"Yes" if mongoOK else "No"}</p>
    <p><strong>Last SPY Date:</strong> {spyDt.strftime("%A, %B %d, %Y")}</p>
    """
    send_error_email(
        name=f"Strategy: {strategy_package_name} - Error Report",
        error_message=error_message,
        recipients=admin_list,
    )
else:
    try:
        send_email(
            subject=email_subject,
            body=email_body,
            recipients=recipient_list,
            attachments=[],
        )
        print(f"Email sent successfully for {strategy_package_name}.")
    except Exception as e:
        error_message = f"""
        <p style='color: #dc3545;'>Failed to send the email:</p>
        <pre style='background-color: #f8d7da; color: #721c24; padding: 10px; border: 1px solid #f5c6cb; border-radius: 5px; white-space: pre-wrap;'>{e}</pre>
        """
        send_error_email(
            name=f"Email Sending Error - {strategy_package_name}",
            error_message=error_message,
            recipients=recipient_list,
        )

print(f"{strategy_package_name} Scan Completed...")
