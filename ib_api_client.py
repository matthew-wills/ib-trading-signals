"""
IB Trading App API Client

This module provides a Python client for interacting with the ib-trading-app API.
Handles authentication, account data retrieval, and position management.
"""

import requests
from typing import Dict, List, Optional
from datetime import datetime


class IBApiClient:
    """Client for interacting with IB Trading App API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the API client.

        Args:
            base_url: Base URL for the API (default: http://localhost:8000)
        """
        self.base_url = base_url.rstrip('/')
        self.token: Optional[str] = None
        self.session = requests.Session()

    def login(self, username: str, password: str) -> str:
        """
        Authenticate with the API and obtain a JWT token.

        Args:
            username: Username for authentication
            password: Password for authentication

        Returns:
            JWT token string

        Raises:
            Exception: If authentication fails
        """
        url = f"{self.base_url}/api/auth/login"
        payload = {
            "username": username,
            "password": password
        }

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()

            data = response.json()
            self.token = data.get('access_token')

            if not self.token:
                raise Exception("No access token in response")

            # Set authorization header for future requests
            self.session.headers.update({
                'Authorization': f'Bearer {self.token}'
            })

            print(f"✓ Authenticated as {username}")
            return self.token

        except requests.exceptions.RequestException as e:
            raise Exception(f"Login failed: {str(e)}")

    def get_account_summary(self, account: Optional[str] = None) -> Dict:
        """
        Fetch account summary data.

        Args:
            account: Optional account ID to filter by

        Returns:
            Dictionary containing account summary data with fields:
            - netLiquidation: Total account value
            - buyingPower: Available buying power
            - totalCashValue: Cash balance
            - grossPositionValue: Total cost of positions
            - unrealizedPnl, realizedPnl: Profit/loss
            - And 17 other account fields

        Raises:
            Exception: If request fails or not authenticated
        """
        if not self.token:
            raise Exception("Not authenticated. Call login() first.")

        url = f"{self.base_url}/api/account-summary"
        params = {"account": account} if account else {}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            print(f"✓ Retrieved account summary")
            return data

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch account summary: {str(e)}")

    def get_positions(self, account: Optional[str] = None) -> List[Dict]:
        """
        Fetch current open positions.

        Args:
            account: Optional account ID to filter by

        Returns:
            List of dictionaries, each containing:
            - symbol: Stock ticker
            - secType: Security type (STK, OPT, etc.)
            - position: Quantity (positive=long, negative=short)
            - avgCost: Average cost per share
            - marketPrice: Current market price
            - marketValue: Total market value
            - unrealizedPnl: Unrealized profit/loss
            - account: Account number

        Raises:
            Exception: If request fails or not authenticated
        """
        if not self.token:
            raise Exception("Not authenticated. Call login() first.")

        url = f"{self.base_url}/api/positions"
        params = {"account": account} if account else {}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            positions = data.get('positions', [])
            print(f"✓ Retrieved {len(positions)} positions")
            return positions

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch positions: {str(e)}")

    def get_portfolio_snapshot(
        self,
        force_refresh: bool = True,
        account: Optional[str] = None
    ) -> Dict:
        """
        Fetch complete portfolio snapshot (account + positions).

        Args:
            force_refresh: Always fetch fresh data from TWS (default: True)
            account: Optional account ID to filter by

        Returns:
            Dictionary containing:
            - snapshotTimestamp: Timestamp of snapshot
            - dataFreshness: Data quality indicators
            - account: Account summary data
            - currencyBalances: Currency breakdown
            - openTrades: Open positions

        Raises:
            Exception: If request fails or not authenticated
        """
        if not self.token:
            raise Exception("Not authenticated. Call login() first.")

        url = f"{self.base_url}/api/portfolio-snapshot"
        params = {
            "force_refresh": force_refresh,
            "account": account
        }
        params = {k: v for k, v in params.items() if v is not None}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            print(f"✓ Retrieved portfolio snapshot")
            return data

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch portfolio snapshot: {str(e)}")

    def check_connection(self) -> Dict:
        """
        Check API and IB Gateway connection status.

        Returns:
            Dictionary with connection status information

        Raises:
            Exception: If request fails
        """
        url = f"{self.base_url}/api/health"

        try:
            response = self.session.get(url)
            response.raise_for_status()

            data = response.json()
            print(f"✓ Connection status: {data.get('status', 'unknown')}")
            return data

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to check connection: {str(e)}")


# Convenience function for quick setup
def create_client(username: str, password: str, base_url: str = "http://localhost:8000") -> IBApiClient:
    """
    Create and authenticate an API client in one step.

    Args:
        username: Username for authentication
        password: Password for authentication
        base_url: Base URL for the API

    Returns:
        Authenticated IBApiClient instance

    Example:
        client = create_client("matthewwills", "password")
        account = client.get_account_summary()
    """
    client = IBApiClient(base_url)
    client.login(username, password)
    return client
