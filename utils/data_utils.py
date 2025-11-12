# data_utils.py
import datetime as dt
import pytz
import exchange_calendars as mcal
import norgatedata
import yfinance as yf

# Initialize Norgate Data API
timeseriesformat = 'pandas-dataframe'
priceadjust = norgatedata.StockPriceAdjustmentType.CAPITAL
padding_setting = norgatedata.PaddingType.NONE


def is_data_up_to_date(spy_data):
    """
    Checks if the provided SPY data is up-to-date based on the NYSE trading calendar and market hours.
    
    Parameters:
    spy_data (pd.DataFrame): DataFrame containing the SPY data with a datetime index.
    
    Returns:
    bool: True if the data is up-to-date, False otherwise.
    """
    
    lastTradeDayDate = mcal.get_calendar('XNYS').previous_close(dt.datetime.now(pytz.timezone('US/Eastern')).date()).date()
    spyDate = getData('SPY', 2).index[-1].date()
    
    return spyDate == lastTradeDayDate


def is_data_up_to_date_v2():
    """
    Checks if the latest Norgate SPY data is up-to-date based on the NYSE trading calendar.

    Returns:
    tuple: (bool indicating if up-to-date, Norgate's last data date, expected last trade date)
    """
    # Load latest SPY data (2 days to be safe)
    spy_data = getData('SPY', 2)
    norgate_last_trade_date = spy_data.index[-1].date()

    # Get today's date in adelaide time
    today_adl_time = dt.datetime.now(pytz.timezone('Australia/Adelaide')).date()

    # Get the most recent completed trading day according to NYSE calendar
    nyse = mcal.get_calendar('XNYS')
    expected_last_trade_date = nyse.previous_close(today_adl_time).date()

    # Compare and return consistent tuple
    is_up_to_date = norgate_last_trade_date == expected_last_trade_date
    return is_up_to_date, norgate_last_trade_date, expected_last_trade_date


def check_norgate_against_yahoo():
    """
    Checks if the SPY data is up-to-date by comparing the last traded day of SPY from Norgate Data with Yahoo Finance.

    Returns:
    bool: True if the data is up-to-date, False otherwise.
    """
    # Fetch the last 2 days of data for SPY from Norgate Data
    spy_data = getData('SPY', 2)
    norgate_last_trade_date = spy_data.index[-1].date()
    # print(f"Norgate last trade date: {norgate_last_trade_date}")

    # Fetch the last 5 days of data for SPY from Yahoo Finance
    yahoo_data = yf.download('SPY', period='5d', interval='1d',progress=False,auto_adjust=True)
    yahoo_last_trade_date = yahoo_data.index[-1].date()
    # print(f"Yahoo last trade date: {yahoo_last_trade_date}")

    # Compare the dates
    return norgate_last_trade_date == yahoo_last_trade_date, norgate_last_trade_date, yahoo_last_trade_date

def getData(symbol, bars=250):
    """Fetches price data for a given symbol and number of bars."""
    return norgatedata.price_timeseries(
        symbol,
        stock_price_adjustment_setting=priceadjust,
        padding_setting=padding_setting,
        limit=bars,
        timeseriesformat=timeseriesformat,
    )

def getData_endDate(symbol, bars, end_date):
    """Fetches price data for a given symbol, bars, and an end date."""
    return norgatedata.price_timeseries(
        symbol,
        stock_price_adjustment_setting=priceadjust,
        padding_setting=padding_setting,
        end_date=end_date,
        limit=bars,
        timeseriesformat=timeseriesformat,
    )

def is_last_friday_of_month(date):
    """Checks if the given date is the last Friday of the month."""
    last_friday = get_last_friday_of_month(date)
    return date == last_friday

def get_last_friday_of_month(date):
    """Finds the last Friday of the month for a given date."""
    next_month = date.replace(day=28) + dt.timedelta(days=4)  # this will never fail
    last_day_of_month = next_month - dt.timedelta(days=next_month.day)
    last_friday = last_day_of_month
    while last_friday.weekday() != 4:  # 4 is Friday
        last_friday -= dt.timedelta(days=1)
    return last_friday

def get_last_friday_of_previous_month(date):
    """Finds the last Friday of the previous month for a given date."""
    first_day_of_current_month = date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - dt.timedelta(days=1)
    last_friday = last_day_of_previous_month
    while last_friday.weekday() != 4:  # 4 is Friday
        last_friday -= dt.timedelta(days=1)
    return last_friday


#%% OLD CODE DURING UPDATE
# def is_data_up_to_date(spy_data):
#     """
#     Checks if the provided SPY data is up-to-date based on the NYSE trading calendar and market hours.
    
#     Parameters:
#     spy_data (pd.DataFrame): DataFrame containing the SPY data with a datetime index.
    
#     Returns:
#     bool: True if the data is up-to-date, False otherwise.
#     """
    
#     # Define NYSE timezone and market hours
#     nyse_tz = pytz.timezone('US/Eastern')
#     market_open_time = dt.time(9, 30)  # 9:30 AM ET
#     #market_close_time = dt.time(16, 0)  # 4:00 PM ET

#     # Get the current date and time in NYSE timezone
#     nyse_now = dt.datetime.now(nyse_tz)
#     nyse_today = nyse_now.date()

#     # Get the last date in the provided SPY data
#     spyDt = spy_data.index[-1]

#     # Get the last NYSE trading day
#     nyse = mcal.get_calendar('XNYS')
#     lastTradeDay = nyse.previous_close(nyse_today)  # Get the last trading day up to today
#     lastTradeDayDate = lastTradeDay.date()

#     # Check if the market has opened today
#     if nyse_now.time() < market_open_time:
#         # If the market hasn't opened yet, check against the previous trading day
#         comparison_date = lastTradeDayDate
#     else:
#         # If the market is open or after close, check against today
#         comparison_date = nyse_today

#     # Convert spyDt to a date for comparison
#     spyDate = spyDt.date()

#     # Check if data is up-to-date
#     return spyDate == comparison_date