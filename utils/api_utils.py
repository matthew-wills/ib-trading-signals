import requests
import json
import sys
import os

# Add parent directory to path to import ib_api_client
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ib_api_client import IBApiClient

# ============================================================================
# IB Trading App API Functions (Active)
# ============================================================================

def fetch_ib_account_summary(client: IBApiClient, account=None):
    """
    Fetch account summary from IB Trading App API.

    Args:
        client: Authenticated IBApiClient instance
        account: Optional account ID to filter by

    Returns:
        dict: Account summary with fields:
            - netLiquidation: Total account value
            - buyingPower: Available buying power
            - grossPositionValue: Total cost of positions
            - totalCashValue: Cash balance
            - availableFunds, excessLiquidity
            - unrealizedPnl, realizedPnl
            - Plus 15 other account fields
    """
    try:
        return client.get_account_summary(account)
    except Exception as e:
        print(f"Error fetching IB account summary: {e}")
        return {}


def fetch_ib_positions(client: IBApiClient, account=None):
    """
    Fetch open positions from IB Trading App API.

    Args:
        client: Authenticated IBApiClient instance
        account: Optional account ID to filter by

    Returns:
        list: List of position dictionaries with fields:
            - symbol: Stock ticker
            - secType: Security type
            - position: Quantity (positive=long, negative=short)
            - avgCost: Average cost per share
            - marketPrice: Current market price
            - marketValue: Total market value
            - unrealizedPnl: Unrealized P&L
            - account: Account number
    """
    try:
        return client.get_positions(account)
    except Exception as e:
        print(f"Error fetching IB positions: {e}")
        return []


# ============================================================================
# Legacy UTM API Functions (For Reference Only - Not Used)
# ============================================================================

def fetch_account_info(accountID, isLive, headers):
    """
    Fetch account information from the UTM API.

    Args:
        accountID (str): The account ID to fetch information for.
        isLive (bool): Whether the account is live or demo.
        headers (dict): Headers to include in the API request.

    Returns:
        dict: A dictionary containing account and balance details.
    """
    # Define the API URL based on account ID and isLive flag

    if isLive == False:  # Hardcoding workaround
        accountInfoAddress = 'https://utm-staging.herokuapp.com/api/v1/accounts/' + accountID + '/balances?live=false'
    else:
        accountInfoAddress = 'https://api.universaltrademanager.com/api/v1/accounts/' + accountID + '/balances?live=true'
        
    # accountInfoAddress = 'https://utm-staging.herokuapp.com/api/v1/accounts/' + accountID + '/balances?live=false'    

    # Make the API request
    response = requests.get(accountInfoAddress, headers=headers)
    accountInfo = json.loads(response.text)

    # Check if the response status is 'success'
    if accountInfo['status'] != 'success':
        return {"error": "Error in Account Info API response"}

    # Extracting balances
    balances = accountInfo['data']['Balances'][0]
    balance_detail = balances['BalanceDetail']

    # Compile all data into a dictionary
    account_data = {
        "AccountID": balances['AccountID'],
        "AccountType": balances['AccountType'],
        "CashBalance": float(balances['CashBalance']),
        "BuyingPower": float(balances['BuyingPower']),
        "Equity": float(balances['Equity']),
        "MarketValue": float(balances['MarketValue']),
        "TodaysProfitLoss": float(balances['TodaysProfitLoss']),
        "UnclearedDeposit": float(balances['UnclearedDeposit']),
        "Commission": float(balances['Commission']),
        "BalanceDetail": {
            "CostOfPositions": float(balance_detail['CostOfPositions']),
            "DayTrades": int(balance_detail['DayTrades']),
            "MaintenanceRate": float(balance_detail['MaintenanceRate']),
            "OptionBuyingPower": float(balance_detail['OptionBuyingPower']),
            "OptionsMarketValue": float(balance_detail['OptionsMarketValue']),
            "OvernightBuyingPower": float(balance_detail['OvernightBuyingPower']),
            "RequiredMargin": float(balance_detail['RequiredMargin']),
            "UnsettledFunds": float(balance_detail['UnsettledFunds']),
            "DayTradeExcess": float(balance_detail['DayTradeExcess']),
            "RealizedProfitLoss": float(balance_detail['RealizedProfitLoss']),
            "UnrealizedProfitLoss": float(balance_detail['UnrealizedProfitLoss']),
        },
    }

    return account_data

def fetch_open_positions(isLive, headers):
    """
    Fetch open positions from the API and process them into a list of trade records.

    Parameters:
        isLive (bool): Whether to fetch live positions or not.
        headers (dict): The headers for the API request, including authentication.

    Returns:
        list: A list of processed trade records.
    """
 
    if isLive == False:  # Hardcoding workaround
        api_url = 'https://utm-staging.herokuapp.com/api/v1/trades/all?archived=false?live=false'
    else:
        api_url = 'https://api.universaltrademanager.com/api/v1/trades/all?archived=false?live=true'

    try:
        # Fetch data from the API
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        
        # Parse JSON response
        data = response.json()
        trades_list = data.get("trades", [])
        
        # Process trades into a list of dictionaries
        trade_records = []
        for trade in trades_list:
            entry_order = trade.get('entryOrder', {})
            trade_info = {
                "User": f"{trade['user']['firstName']} {trade['user']['lastName']}",
                "Account": trade['account']['accountName'],
                "Strategy": trade['strategy']['name'],
                # "Signal Date": trade['signalDate'],
                "Symbol": trade['symbol'],
                "Trade Action": trade['tradeAction'],
                "Order Quantity": trade['quantity'],
                "Limit Price": trade.get('limitPrice', None),
                "Commission Fee": float(entry_order.get('commissionFee', 0)),  # Default to 0 if missing
                "Quantity": entry_order.get('execQuantity', 0),  # Default to 0 if missing
                "Execution Price": float(entry_order.get('executionPrice', 0)),  # Default to 0 if missing
                "Opened DateTime": entry_order.get('openedDateTime', None),  # Default to None if missing
            }
            trade_records.append(trade_info)
                
        return trade_records
    except requests.exceptions.RequestException as e:
        print(f"Error fetching open positions: {e}")
        return []
    
def send_signals_to_mongo(strategy_name, strategy_type, signals_df, strategy_id, is_live, api_key, spy_date, spy_date_name):
    """
    Send trading signals to MongoDB.

    Parameters:
        strategy_name (str): The name of the strategy.
        strategy_type (str): The type of the strategy (e.g., 'ROT', 'MR', 'HFT').
        signals_df (pd.DataFrame): The signals to be sent.
        strategy_id (str): The MongoDB strategy ID.
        is_live(bool): Is the strategy a demo or live.
        api_key (str): The API key for authentication.
        spy_date (datetime): The trade date.
        spy_date_name (str): The name of the trade day (e.g., 'Monday').
        
    Returns:
        bool: True if the signals were sent successfully, False otherwise.
    """
    headers = {'x-api-key': api_key}
    spy_date_num = spy_date.year * 10000 + spy_date.month * 100 + spy_date.day
    
    # Prepare JSON payload
    json_data = {
        'name': strategy_name,
        'type': strategy_type,
        'lastTradeDay': spy_date_name,
        'lastTradeDate': str(spy_date_num),
        'signals': signals_df.to_dict(orient='records'),
    }
    
    # API URL
    if is_live:
        url = f'https://api.universaltrademanager.com/api/v1/strategies/{strategy_id}'
    else:
        url = f'https://utm-staging.herokuapp.com/api/v1/strategies/{strategy_id}'
    
    try:
        # Send the API request
        response = requests.put(url, headers=headers, json=json_data)
        
        if response.status_code in [200, 201]:
            print(f"Successfully updated MongoDB for {strategy_name}.")
            return True
        else:
            print(f"Failed to update MongoDB for {strategy_name}. Response: {response.text}")
            return False
    except Exception as e:
        print(f"Error while sending signals for {strategy_name}: {e}")
        return False
