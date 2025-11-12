#!/usr/bin/env python3
"""
HFT Signal Generator for IB Trading Application

Scans Russell 1000 stocks and generates HFT long and short trading signals
based on mean-reversion within trend strategies. Outputs CSV file ready for
batch upload to the IB trading application.

Strategy Logic:
- HFT Long: Buy pullbacks (IBR < 0.3) in uptrends, limit = Low - 0.6*ATR
- HFT Short: Sell extensions (IBR > 0.7) in uptrends, limit = High + 0.3*ATR

Orders automatically close at market close if filled (AttachMOC=YES).
"""

import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
import pytz

import norgatedata
import ta

# Norgate Data Configuration
timeseriesformat = 'pandas-dataframe'
priceadjust = norgatedata.StockPriceAdjustmentType.CAPITAL
padding_setting = norgatedata.PaddingType.NONE


# Utility functions (inlined from oldSignalGenCode)
def getData(symbol, bars=250):
    """Fetches price data for a given symbol and number of bars."""
    return norgatedata.price_timeseries(
        symbol,
        stock_price_adjustment_setting=priceadjust,
        padding_setting=padding_setting,
        limit=bars,
        timeseriesformat=timeseriesformat,
    )


def IBR(H, L, C):
    """Calculate Internal Bar Range: (Close - Low) / (High - Low)"""
    ans = (C - L) / (H - L)
    return ans


def tickSize(l):
    """Determine appropriate tick size based on price level."""
    tick = 0.01
    if l < 2:
        tick = 0.005
    if l < 0.1:
        tick = 0.001
    return tick


class HFTSignalGenerator:
    """Generate HFT trading signals from Russell 1000 stocks."""

    def __init__(self, equity=100000, backend_url="http://localhost:8000"):
        self.backend_url = backend_url
        self.equity = equity
        self.leverage = 4.0
        self.long_allocation = 0.25
        self.short_allocation = 0.25
        self.max_positions = 15

        # Strategy parameters
        self.min_bars = 251
        self.ma_period = 250
        self.adx_period = 4
        self.adx_threshold = 35
        self.atr_period = 5
        self.volume_period = 50
        self.min_volume = 2_000_000

        # Entry filters
        self.long_min_price = 10
        self.long_max_price = 5000
        self.short_min_price = 20
        self.short_max_price = 5000

        # IBR thresholds
        self.long_ibr_max = 0.3
        self.short_ibr_min = 0.7

        # Stretch factors for limit prices
        self.long_stretch = 0.6
        self.short_stretch = 0.3

    def fetch_account_equity(self):
        """Fetch account equity from IB backend API."""
        print(f"Using equity: ${self.equity:,.2f}")
        return self.equity

    def calculate_indicators(self, data):
        """Calculate technical indicators for a stock."""
        if len(data) < self.min_bars:
            return None

        # Latest bar values
        close = data.Close.iloc[-1]
        high = data.High.iloc[-1]
        low = data.Low.iloc[-1]

        # Moving average
        ma = ta.trend.SMAIndicator(
            data.Close, self.ma_period, fillna=True
        ).sma_indicator().iloc[-1]

        # ADX
        adx = ta.trend.ADXIndicator(
            data.High, data.Low, data.Close, self.adx_period, fillna=True
        ).adx().iloc[-1]

        # ATR
        atr = ta.volatility.AverageTrueRange(
            data.High, data.Low, data.Close, self.atr_period, fillna=True
        ).average_true_range().iloc[-1]

        # Average volume
        avg_volume = ta.trend.EMAIndicator(
            data.Volume, self.volume_period, fillna=True
        ).ema_indicator().iloc[-1]

        # IBR (Internal Bar Range)
        ibr = IBR(high, low, close)

        # Volatility %
        volatility = (atr / close * 100) if close > 0 else 0

        return {
            'close': close,
            'high': high,
            'low': low,
            'ma': ma,
            'adx': adx,
            'atr': atr,
            'avg_volume': avg_volume,
            'ibr': ibr,
            'volatility': volatility
        }

    def check_long_signal(self, symbol, indicators):
        """Check if stock meets HFT long criteria."""
        # Price filter
        if indicators['close'] < self.long_min_price or indicators['close'] > self.long_max_price:
            return None

        # Volume filter
        if indicators['avg_volume'] < self.min_volume:
            return None

        # Trend filter: close above MA
        if indicators['close'] <= indicators['ma']:
            return None

        # Momentum filter: ADX > threshold
        if indicators['adx'] <= self.adx_threshold:
            return None

        # Mean reversion setup: IBR < threshold (closed near low)
        if indicators['ibr'] >= self.long_ibr_max:
            return None

        # Calculate limit price: Low - 0.6*ATR
        tick = tickSize(indicators['low'])
        entry_limit = round((indicators['low'] - self.long_stretch * indicators['atr']) / tick) * tick

        # Calculate position size
        position_value = self.equity * self.leverage * self.long_allocation / self.max_positions
        quantity = max(1, int(position_value / entry_limit))

        return {
            'symbol': symbol,
            'action': 'BUY',
            'quantity': quantity,
            'limit_price': entry_limit,
            'volatility': indicators['volatility'],
            'ibr': indicators['ibr'],
            'adx': indicators['adx']
        }

    def check_short_signal(self, symbol, indicators):
        """Check if stock meets HFT short criteria."""
        # Price filter
        if indicators['close'] < self.short_min_price or indicators['close'] > self.short_max_price:
            return None

        # Volume filter
        if indicators['avg_volume'] < self.min_volume:
            return None

        # Trend filter: close above MA
        if indicators['close'] <= indicators['ma']:
            return None

        # Momentum filter: ADX > threshold
        if indicators['adx'] <= self.adx_threshold:
            return None

        # Mean reversion setup: IBR > threshold (closed near high)
        if indicators['ibr'] <= self.short_ibr_min:
            return None

        # Calculate limit price: High + 0.3*ATR
        tick = tickSize(indicators['high'])
        entry_limit = round((indicators['high'] + self.short_stretch * indicators['atr']) / tick) * tick

        # Calculate position size
        position_value = self.equity * self.leverage * self.short_allocation / self.max_positions
        quantity = max(1, int(position_value / entry_limit))

        return {
            'symbol': symbol,
            'action': 'SELL',
            'quantity': quantity,
            'limit_price': entry_limit,
            'volatility': indicators['volatility'],
            'ibr': indicators['ibr'],
            'adx': indicators['adx']
        }

    def scan_universe(self):
        """Scan Russell 1000 for HFT signals."""
        print("Fetching Russell 1000 watchlist...")
        try:
            ticker_list = norgatedata.watchlist_symbols('Russell 1000')
            # Exclude GOOG as in original code
            ticker_list = [t for t in ticker_list if t != 'GOOG']
            print(f"Scanning {len(ticker_list)} symbols...")
        except Exception as e:
            print(f"Error fetching watchlist: {e}")
            return [], []

        long_candidates = []
        short_candidates = []

        for i, symbol in enumerate(ticker_list):
            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1}/{len(ticker_list)} symbols...")

            try:
                # Fetch data
                data = getData(symbol, self.min_bars)

                if data is None or len(data) < self.min_bars:
                    continue

                # Calculate indicators
                indicators = self.calculate_indicators(data)
                if indicators is None:
                    continue

                # Check for long signal
                long_signal = self.check_long_signal(symbol, indicators)
                if long_signal:
                    long_candidates.append(long_signal)

                # Check for short signal
                short_signal = self.check_short_signal(symbol, indicators)
                if short_signal:
                    short_candidates.append(short_signal)

            except Exception as e:
                # Skip symbols with data errors
                continue

        print(f"\nFound {len(long_candidates)} long candidates, {len(short_candidates)} short candidates")

        # Rank by volatility and select top 15
        long_candidates.sort(key=lambda x: x['volatility'], reverse=True)
        short_candidates.sort(key=lambda x: x['volatility'], reverse=True)

        long_signals = long_candidates[:self.max_positions]
        short_signals = short_candidates[:self.max_positions]

        print(f"Selected top {len(long_signals)} long signals, {len(short_signals)} short signals")

        return long_signals, short_signals

    def generate_csv(self, long_signals, short_signals, output_file):
        """Generate CSV file for batch order upload."""
        # Ensure output directory exists
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Calculate GTD expiration: today at 15:44 ET
        et_tz = pytz.timezone('America/New_York')
        now_et = datetime.now(et_tz)

        # If after 15:44, use next trading day
        expiration_time = now_et.replace(hour=15, minute=44, second=0, microsecond=0)
        if now_et.time() > expiration_time.time():
            expiration_time += timedelta(days=1)

        gtd_string = expiration_time.strftime('%Y-%m-%dT%H:%M')

        # Prepare rows
        rows = []

        for signal in long_signals:
            rows.append({
                'Symbol': signal['symbol'],
                'Action': signal['action'],
                'Quantity': signal['quantity'],
                'OrderType': 'LIMIT',
                'LimitPrice': f"{signal['limit_price']:.2f}",
                'StopPrice': '',
                'SecurityType': 'CFD',
                'Exchange': 'SMART',
                'Timezone': '',
                'TimeInForce': 'GTD',
                'GoodTillDate': gtd_string,
                'AttachMOC': 'YES',
                'Strategy': 'hft-long',
                'OutsideRTH': 'NO',
                'AllOrNone': 'NO',
                'Hidden': 'NO',
                'DisplaySize': '0',
                'DisplaySizeIsPercentage': 'NO'
            })

        for signal in short_signals:
            rows.append({
                'Symbol': signal['symbol'],
                'Action': signal['action'],
                'Quantity': signal['quantity'],
                'OrderType': 'LIMIT',
                'LimitPrice': f"{signal['limit_price']:.2f}",
                'StopPrice': '',
                'SecurityType': 'CFD',
                'Exchange': 'SMART',
                'Timezone': '',
                'TimeInForce': 'GTD',
                'GoodTillDate': gtd_string,
                'AttachMOC': 'YES',
                'Strategy': 'hft-short',
                'OutsideRTH': 'NO',
                'AllOrNone': 'NO',
                'Hidden': 'NO',
                'DisplaySize': '0',
                'DisplaySizeIsPercentage': 'NO'
            })

        # Create DataFrame and save
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)

        print(f"\n[SUCCESS] CSV file created: {output_file}")
        print(f"  Total orders: {len(rows)}")
        print(f"  Long orders: {len(long_signals)}")
        print(f"  Short orders: {len(short_signals)}")
        print(f"  Expiration: {gtd_string} ET")

        return df

    def print_summary(self, long_signals, short_signals):
        """Print summary of generated signals."""
        print("\n" + "="*80)
        print("HFT SIGNAL SUMMARY")
        print("="*80)

        print(f"\nAccount Equity: ${self.equity:,.2f}")
        print(f"Leverage: {self.leverage}x")
        print(f"Long Allocation: {self.long_allocation*100}%")
        print(f"Short Allocation: {self.short_allocation*100}%")

        if long_signals:
            print(f"\n--- LONG SIGNALS ({len(long_signals)}) ---")
            long_notional = sum(s['quantity'] * s['limit_price'] for s in long_signals)
            print(f"Total Notional: ${long_notional:,.2f}")
            print("\nTop 5:")
            for i, s in enumerate(long_signals[:5], 1):
                print(f"  {i}. {s['symbol']:6s} @ ${s['limit_price']:7.2f} x {s['quantity']:4d} "
                      f"(Vol: {s['volatility']:.2f}%, IBR: {s['ibr']:.3f}, ADX: {s['adx']:.1f})")

        if short_signals:
            print(f"\n--- SHORT SIGNALS ({len(short_signals)}) ---")
            short_notional = sum(s['quantity'] * s['limit_price'] for s in short_signals)
            print(f"Total Notional: ${short_notional:,.2f}")
            print("\nTop 5:")
            for i, s in enumerate(short_signals[:5], 1):
                print(f"  {i}. {s['symbol']:6s} @ ${s['limit_price']:7.2f} x {s['quantity']:4d} "
                      f"(Vol: {s['volatility']:.2f}%, IBR: {s['ibr']:.3f}, ADX: {s['adx']:.1f})")

        print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Generate HFT trading signals for IB batch upload'
    )
    parser.add_argument(
        '--output', '-o',
        default=f"orders/hft_orders_{datetime.now().strftime('%d%m%y')}.csv",
        help='Output CSV filename (default: orders/hft_orders_DDMMYY.csv)'
    )
    parser.add_argument(
        '--equity', '-e',
        type=float,
        help='Account equity (default: $100,000)'
    )
    parser.add_argument(
        '--backend-url', '-b',
        default='http://localhost:8000',
        help='IB backend API URL (default: http://localhost:8000)'
    )
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Print signals without creating CSV'
    )

    args = parser.parse_args()

    print("="*80)
    print("HFT SIGNAL GENERATOR")
    print("="*80)

    # Initialize generator
    if args.equity is not None:
        generator = HFTSignalGenerator(equity=args.equity, backend_url=args.backend_url)
    else:
        generator = HFTSignalGenerator(backend_url=args.backend_url)

    # Fetch equity
    generator.equity = generator.fetch_account_equity()

    # Scan for signals
    long_signals, short_signals = generator.scan_universe()

    # Print summary
    generator.print_summary(long_signals, short_signals)

    # Generate CSV
    if not args.dry_run:
        if long_signals or short_signals:
            generator.generate_csv(long_signals, short_signals, args.output)
            print(f"\n[SUCCESS] Ready to upload {args.output} to batch orders page")
        else:
            print("\n[WARNING] No signals generated, skipping CSV creation")
    else:
        print("\n[DRY RUN - No CSV created]")


if __name__ == '__main__':
    main()
