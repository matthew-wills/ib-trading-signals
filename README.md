# ib-trading-signals

Multi-strategy trading signal generator for ib-trading-app.

## Overview

This repository contains 8 trading strategies that generate daily order signals for Interactive Brokers trading through the ib-trading-app platform:

1. **MOMO** - Momentum stocks (NASDAQ 100) - 5% allocation
2. **GROWTH** - Growth ETFs (QQQ, SPY, IOO) - 10% allocation
3. **DEF** - Defensive ETFs (GLD, TLT) - 3% allocation
4. **BTC** - Bitcoin exposure (IBIT) - 2% allocation
5. **MR Long** - Mean reversion longs (S&P 500) - 15% allocation
6. **MR Short** - Mean reversion shorts (S&P 500) - 15% allocation
7. **HFT Long** - High frequency longs (Russell 1000) - 25% allocation
8. **HFT Short** - High frequency shorts (Russell 1000) - 25% allocation

## Features

- Fetches account balance and positions from ib-trading-app API
- Generates consolidated CSV file for all strategies
- Market condition filters enabled (bullish market checks)
- Supports bracketing orders (simultaneous long/short on same symbol)
- Capital allocation with 20% safety buffer
- Ready for manual upload to ib-trading-app

## Prerequisites

### Required Software
- Python 3.9+
- Norgate Data subscription
- ib-trading-app running in Docker

### ib-trading-app Setup
The ib-trading-app must be running and accessible:
```bash
cd ../ib-trading-app
docker-compose up -d
```

Verify it's running at http://localhost:3000

## Installation

1. **Clone the repository** (already done)
   ```bash
   cd C:\GitHub\ib-trading-signals
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set environment variables**
   Create a `.env` file or set environment variables:
   ```bash
   export IB_USERNAME="matthewwills"
   export IB_PASSWORD="your_password"
   ```

   Or update the script directly (lines 48-49 in generate_all_signals.py)

4. **Verify Norgate Data**
   Ensure Norgate Data is installed and your subscription is active

## Usage

### Generate Daily Signals

Run the main script:
```bash
python generate_all_signals.py
```

### What It Does

1. **Authenticates** with ib-trading-app API
2. **Fetches** account balance and buying power
3. **Calculates** usable capital (80% of total to maintain buffer)
4. **Retrieves** current open positions
5. **Validates** Norgate data freshness
6. **Runs** each strategy to generate buy/sell signals
7. **Outputs** consolidated CSV: `output/daily_orders_YYYY-MM-DD.csv`

### Upload to ib-trading-app

1. Open http://localhost:3000
2. Navigate to **Batch Orders** page
3. Upload the generated CSV file
4. Review orders
5. Submit to Interactive Brokers

## File Structure

```
ib-trading-signals/
├── generate_all_signals.py      # Main signal generator (3 strategies implemented)
├── ib_api_client.py              # IB Trading App API client
├── batch-orders-template.csv     # CSV format template
├── generate_hft_signals.py       # Original HFT-only generator (reference)
├── RUN_MWT_LIVE.py               # Legacy UTM system (reference)
├── utils/
│   ├── __init__.py               # Package initialization
│   ├── indicator_utils.py        # Technical indicators (IBR, ROC, tickSize)
│   ├── api_utils.py              # API integration functions
│   ├── data_utils.py             # Market data fetching (Norgate)
│   └── email_utils.py            # Email utilities (legacy, not used)
├── output/
│   └── daily_orders_*.csv        # Generated order files
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Strategy Details

### Implemented Strategies (3/8)

#### MOMO - Momentum Stocks
- **Universe**: NASDAQ 100
- **Entry**: Bullish market (NYSE H-L > 13-MA), Price > 100-SMA, Positive momentum
- **Ranking**: 0.5×ROC(120) + 0.5×ROC(240)
- **Position**: Hold top 3, exit if rank drops below 5
- **Order Type**: Market orders at open (OPG)

#### MR Long - Mean Reversion Longs
- **Universe**: S&P 500
- **Entry**: Price > $5, Volume > 200k, Price > 100-SMA, ADX(10) > 30, RSI(2) < 30
- **Entry Price**: Limit at Low - 0.5×ATR(10)
- **Exit Price**: Limit at previous day's high
- **Order Type**: Limit orders, GTC

#### HFT Long - High Frequency Longs
- **Universe**: Russell 1000
- **Entry**: $10-$5000, Volume > 2M, Price > 250-SMA, ADX(4) > 35, IBR < 0.3
- **Entry Price**: Limit at Low - 0.6×ATR(5)
- **Exit**: Attached Market-On-Close (auto-liquidate at 4pm)
- **Order Type**: Limit orders, GTD, CFD

### TODO: Remaining Strategies (5/8)

The following strategies need to be implemented following the same patterns as above:

- **GROWTH** - Growth ETF rotation (QQQ, SPY, IOO)
- **DEF** - Defensive ETF rotation (GLD, TLT)
- **BTC** - Bitcoin rotation (IBIT)
- **MR Short** - Mean reversion shorts (S&P 500)
- **HFT Short** - High frequency shorts (Russell 1000)

See `generate_all_signals.py` lines 601-606 for TODO comments and `RUN_MWT_LIVE.py` for reference implementation.

## Configuration

All strategy parameters are embedded in `generate_all_signals.py` (lines 47-150):

- Capital allocation percentages
- Position size limits
- Entry/exit indicator periods
- Market filter settings
- Price and volume thresholds

Modify these constants to adjust strategy behavior.

## CSV Output Format

Generated CSV files contain 18 columns matching the ib-trading-app batch upload format:

| Column | Description | Example |
|--------|-------------|---------|
| Symbol | Ticker | AAPL |
| Action | BUY, SELL, SELLSHORT, BUYTOCOVER | BUY |
| Quantity | Number of shares | 100 |
| OrderType | MARKET or LIMIT | LIMIT |
| LimitPrice | For limit orders | 150.50 |
| SecurityType | STK or CFD | STK |
| Exchange | SMART, NYSE, etc | SMART |
| TimeInForce | DAY, GTC, GTD | GTC |
| GoodTillDate | For GTD orders | 2025-11-12T15:44:00 |
| AttachMOC | Auto-close at market close | YES/NO |
| Strategy | Strategy name | momo |
| ... | 7 more columns | ... |

## Capital Allocation

Total: 100% with 20% safety buffer

- MOMO: 5%
- GROWTH: 10%
- DEF: 3%
- BTC: 2%
- MR Long: 15%
- MR Short: 15%
- HFT Long: 25%
- HFT Short: 25%

**Usable Capital** = (Buying Power + Gross Position Value) × 0.8

## Important Notes

### Bracketing Orders
IBKR now supports bracketing orders, so the conflict checking logic from the old UTM system has been removed. You can have simultaneous long and short positions in the same symbol.

### Position Tracking
Since the IB API doesn't track which strategy each position belongs to, the current implementation treats all positions generically. You may need to manually manage strategy-specific position tracking or enhance the position organization logic.

### Market Filters
All market condition filters are **ENABLED** by default:
- MOMO: Bullish market check
- GROWTH: Uptrend filter
- DEF: Uptrend filter
- BTC: Uptrend filter

### Data Requirements
- Norgate Data must be current (checked automatically)
- Monthly rotation strategies use last Friday of the month as data end date
- Daily strategies use most recent data

## Troubleshooting

### Authentication Errors
```
ERROR: Could not validate credentials
```
- Check IB_USERNAME and IB_PASSWORD are correct
- Verify ib-trading-app is running: `docker ps`
- Check you can access http://localhost:8000/docs

### Norgate Data Issues
```
WARNING: Norgate data may be stale
```
- Update Norgate Data via their client
- Check your subscription is active
- Verify data for test symbols like SPY

### No Orders Generated
```
No orders generated. All strategies returned empty signals.
```
- This is normal if market conditions don't meet entry criteria
- Check if market filters are too restrictive
- Verify you have capital available

### Module Import Errors
```
ModuleNotFoundError: No module named 'ta'
```
- Install dependencies: `pip install -r requirements.txt`

## Development

### Adding New Strategies

Follow the pattern in the three implemented strategies:

1. Define a function: `def run_strategy_name(usable_capital, current_positions_df):`
2. Calculate indicators using `ta` library and `utils.indicator_utils`
3. Generate orders using `create_order_row()` helper
4. Return list of order dictionaries
5. Call from `main()` with try/except error handling

### Testing

Test with paper trading account first:
1. Ensure ib-trading-app is connected to paper trading (TWS port 7497)
2. Generate signals: `python generate_all_signals.py`
3. Review CSV output carefully
4. Upload to ib-trading-app and review in UI before submitting

## References

- **ib-trading-app**: ../ib-trading-app
- **Legacy system**: RUN_MWT_LIVE.py (UTM-based)
- **HFT reference**: generate_hft_signals.py (working example)
- **Norgate Data**: https://norgatedata.com
- **Interactive Brokers API**: https://interactivebrokers.github.io

## Support

For issues or questions:
- ib-trading-app: Check ../ib-trading-app/README.md
- This repository: Review code comments and TODO sections
- Norgate Data: https://norgatedata.com/support

## License

Internal use only - Matthew Wills
