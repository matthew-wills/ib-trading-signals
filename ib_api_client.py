"""
IB Trading App API Client

This module provides a Python client for interacting with the ib-trading-app API.
Uses X-API-Key authentication for secure access to trading endpoints.

Based on API documentation from ib-trading-app v2.0
"""

import requests
from typing import Dict, List, Optional


class IBTradingAPI:
    """
    Client for IB Trading Platform API

    This client provides methods to:
    - Query account balances and buying power
    - Get current positions and open trades
    - View portfolio snapshots with strategy breakdown
    - Check strategy P&L and performance
    - Submit new orders programmatically
    """

    def __init__(self, api_key: str, base_url: str = "http://localhost:8000"):
        """
        Initialize the API client with API key authentication.

        Args:
            api_key: API key for authentication (X-API-Key header)
            base_url: Base URL for the API (default: http://localhost:8000)
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.headers = {"X-API-Key": api_key}

    def get_account_summary(self, account: Optional[str] = None) -> Dict:
        """
        Get account balance, buying power, and equity information.

        Args:
            account: Optional account number to filter by

        Returns:
            Dict containing account summary with keys:
            - account: Account number
            - equity: Total equity
            - cash: Available cash
            - buying_power: Available buying power
            - net_liquidation: Net liquidation value
            - is_live_data: Whether data is live
            - connection_status: Connection status
            - account_type: Account type (MARGIN, CASH)
            - leverage: Current leverage ratio
        """
        url = f"{self.base_url}/api/account-summary"
        params = {"account": account} if account else {}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_positions(self, account: Optional[str] = None) -> List[Dict]:
        """
        Get all open positions.

        Args:
            account: Optional account number to filter by

        Returns:
            List of position dicts with keys:
            - symbol: Ticker symbol
            - quantity: Number of shares
            - avg_cost: Average cost per share
            - account: Account number
            - sec_type: Security type (STK, OPT, etc.)
        """
        url = f"{self.base_url}/api/positions"
        params = {"account": account} if account else {}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["positions"]

    def get_open_trades(self, account: Optional[str] = None) -> List[Dict]:
        """
        Get all open trades with strategy information.

        This is crucial for understanding which positions belong to which strategy.

        Args:
            account: Optional account number to filter by

        Returns:
            List of trade dicts with keys:
            - symbol: Ticker symbol
            - strategy_id: ID of the strategy
            - strategy_name: Name of the strategy (e.g., "MOMO", "MR Long")
            - action: BUY or SELL
            - current_quantity: Current position size
            - avg_entry_price: Average entry price
            - unrealized_pnl: Unrealized P&L
        """
        url = f"{self.base_url}/api/trades/open"
        params = {"account": account} if account else {}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["trades"]

    def get_strategies(self, account: Optional[str] = None) -> List[Dict]:
        """
        Get all trading strategies.

        Args:
            account: Optional account number to filter by

        Returns:
            List of strategy dicts with keys:
            - id: Strategy ID
            - name: Strategy name
            - description: Strategy description
            - active: Whether strategy is active
        """
        url = f"{self.base_url}/api/strategies"
        params = {"account": account} if account else {}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_strategy_pnl(self, strategy_id: int) -> Dict:
        """
        Get detailed P&L for a specific strategy.

        Args:
            strategy_id: ID of the strategy

        Returns:
            Dict containing P&L metrics:
            - realized_pnl: Realized profit/loss
            - unrealized_pnl: Unrealized profit/loss
            - total_pnl: Total P&L
            - open_trades_count: Number of open trades
            - closed_trades_count: Number of closed trades
            - win_rate: Win rate percentage
            - avg_win: Average winning trade
            - avg_loss: Average losing trade
        """
        url = f"{self.base_url}/api/strategies/{strategy_id}/pnl"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def get_portfolio_snapshot(self, account: Optional[str] = None) -> Dict:
        """
        Get comprehensive portfolio snapshot with positions and strategy breakdown.

        This is the most comprehensive endpoint for understanding current portfolio state.

        Args:
            account: Optional account number to filter by

        Returns:
            Dict containing:
            - positions: List of all positions with unrealized P&L
            - strategies: List of strategies with open position counts
            - total_positions: Total number of positions
            - total_market_value: Total market value
            - total_unrealized_pnl: Total unrealized P&L
        """
        url = f"{self.base_url}/api/portfolio-snapshot"
        params = {"account": account} if account else {}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_orders(self, account: Optional[str] = None) -> List[Dict]:
        """
        Get all orders (pending, filled, cancelled).

        Args:
            account: Optional account number to filter by

        Returns:
            List of order dicts
        """
        url = f"{self.base_url}/api/orders"
        params = {"account": account} if account else {}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()["orders"]

    def get_connection_status(self) -> Dict:
        """
        Check TWS/IB Gateway connection status.

        Returns:
            Dict containing:
            - connected: Whether connected to TWS/Gateway
            - mode: 'paper' or 'live'
            - accounts: List of available account numbers
        """
        url = f"{self.base_url}/api/connection"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def submit_order(self,
                     strategy_id: int,
                     symbol: str,
                     action: str,  # "BUY" or "SELL"
                     quantity: float,
                     order_type: str = "MKT",
                     limit_price: Optional[float] = None,
                     stop_price: Optional[float] = None,
                     time_in_force: str = "DAY",
                     account_number: Optional[str] = None,
                     sec_type: str = "STK") -> Dict:
        """
        Submit a new order to the trading platform.

        Args:
            strategy_id: ID of the strategy placing the order
            symbol: Ticker symbol
            action: "BUY" or "SELL"
            quantity: Number of shares
            order_type: Order type (MKT, LMT, STP, MOC, LOC)
            limit_price: Limit price for LMT orders
            stop_price: Stop price for STP orders
            time_in_force: Time in force (DAY, GTC, GTD)
            account_number: Account number (optional)
            sec_type: Security type (STK, OPT, FUT, etc.)

        Returns:
            Dict containing order confirmation:
            - order_id: Order ID from IB
            - status: Order status
            - symbol: Symbol
            - action: Action (BUY/SELL)
            - quantity: Quantity
        """
        url = f"{self.base_url}/api/orders"

        order_data = {
            "strategy_id": strategy_id,
            "symbol": symbol,
            "sec_type": sec_type,
            "action": action,
            "quantity": quantity,
            "order_type": order_type,
            "time_in_force": time_in_force,
        }

        if limit_price is not None:
            order_data["limit_price"] = limit_price

        if stop_price is not None:
            order_data["stop_price"] = stop_price

        if account_number:
            order_data["account_number"] = account_number

        response = requests.post(url, headers=self.headers, json=order_data)
        response.raise_for_status()
        return response.json()


def create_client(api_key: str, base_url: str = "http://localhost:8000") -> IBTradingAPI:
    """
    Create an API client with API key authentication.

    Args:
        api_key: API key for X-API-Key header authentication
        base_url: Base URL for the API

    Returns:
        Authenticated IBTradingAPI instance

    Example:
        api_key = os.getenv("IB_API_KEY")
        client = create_client(api_key)
        account = client.get_account_summary()
    """
    return IBTradingAPI(api_key, base_url)


# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()

    # Initialize client with API key from environment
    api_key = os.getenv("IB_API_KEY")
    if not api_key:
        print("Error: IB_API_KEY not found in environment variables")
        print("Please set IB_API_KEY in your .env file")
        exit(1)

    api = create_client(api_key)

    try:
        # Get account information
        print("Fetching account summary...")
        account_summary = api.get_account_summary()

        # Handle nested account structure
        if 'account' in account_summary and isinstance(account_summary['account'], dict):
            acct = account_summary['account']
            account_num = acct.get('account', 'Unknown')
            equity = acct.get('equity', 0)
            buying_power = acct.get('buyingPower', 0)
        else:
            # Flat structure (as per docs)
            account_num = account_summary.get('account', 'Unknown')
            equity = account_summary.get('equity', 0)
            buying_power = account_summary.get('buying_power', 0)

        print(f"Account: {account_num}")
        print(f"Equity: ${equity:,.2f}")
        print(f"Buying Power: ${buying_power:,.2f}")

        # Get current positions
        print("\nFetching current positions...")
        positions = api.get_positions()
        print(f"Total Positions: {len(positions)}")
        for pos in positions[:5]:  # Show first 5
            print(f"  {pos['symbol']}: {pos['quantity']} shares @ ${pos['avg_cost']:.2f}")

        # Get open trades by strategy (may not be available without proper auth)
        print("\nFetching open trades...")
        try:
            open_trades = api.get_open_trades()
            print(f"Open Trades: {len(open_trades)}")
            for trade in open_trades[:5]:  # Show first 5
                print(f"  {trade['strategy_name']}: {trade['symbol']} {trade['action']} "
                      f"{trade['current_quantity']} @ ${trade['avg_entry_price']:.2f} "
                      f"(P&L: ${trade.get('unrealized_pnl', 0):.2f})")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("  (Endpoint requires authentication - skipping)")
            else:
                raise

        # Get portfolio snapshot
        print("\nFetching portfolio snapshot...")
        try:
            portfolio = api.get_portfolio_snapshot()
            print(f"Total Positions: {portfolio.get('total_positions', 0)}")
            print(f"Market Value: ${portfolio.get('total_market_value', 0):,.2f}")
            print(f"Unrealized P&L: ${portfolio.get('total_unrealized_pnl', 0):,.2f}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("  (Endpoint requires authentication - skipping)")
            else:
                raise

        print("\n[SUCCESS] API client is working correctly!")

    except requests.exceptions.RequestException as e:
        print(f"\n[ERROR] Error connecting to API: {e}")
        print("\nMake sure:")
        print("1. ib-trading-app is running on http://localhost:8000")
        print("2. Your API key is correct in the .env file")
        print("3. TWS or IB Gateway is connected")
