# email_utils.py
import os
import datetime as dt
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib

# Email Configuration
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USERNAME = "universaltrademanager@gmail.com"
EMAIL_PASSWORD = "ukob uxvc wgjy zuke"
FROM_EMAIL = "universaltrademanager@gmail.com"

# HTML Helpers

# Generate normal email header with Dark Green theme
def generate_email_header(name):
    """Generate a decorative HTML for the email header with a dark green theme."""
    header_html = f'''
    <div style="background-color: #e6ffe6; border: 2px solid #006400; padding: 15px; text-align: center; font-family: 'Arial', sans-serif; border-radius: 10px; margin-bottom: 20px;">
        <h1 style="font-size: 36px; color: #006400; margin-bottom: 5px;">UTM - Trading Signals Report</h1>
        <p style="font-size: 18px; color: #333; margin-top: 0;">{name}</p>
        <p style="font-size: 16px; color: #777;">{dt.date.today().strftime("%A, %B %d, %Y")}</p>
    </div>
    '''
    return header_html

# Update email footer to Dark Green theme
def generate_email_footer():
    """Generate a decorative HTML for the email footer with a comprehensive trading disclaimer in the Dark Green color scheme."""
    footer_html = f'''
    <div style="background-color: #f2f2f2; border-top: 2px solid #006400; padding: 15px; text-align: center; font-family: Arial, sans-serif; border-radius: 0 0 10px 10px; margin-top: 20px;">
        <p style="font-size: 14px; color: #333;">This report was generated automatically by UTM.</p>
        <p style="font-size: 12px; color: #333;">
            For more information, contact us at <a href="mailto:support@utm.com.au" style="color: #006400; text-decoration: none;">support@utm.com.au</a> 
            or visit our <a href="https://universaltrademanager.com/faq" style="color: #006400; text-decoration: none;">FAQ</a> section.
        </p>
        <p style="font-size: 12px; color: #333;">&copy; {dt.date.today().year} UTM Limited (Ltd). All rights reserved.</p>
        <p style="font-size: 10px; color: #555; margin-top: 10px;">
            Disclaimer: Trading involves significant risk and may result in the loss of your invested capital. 
            Trading on margin amplifies this risk as it allows you to trade with borrowed funds, potentially leading to greater losses than your initial investment. 
            You should only trade with money that you can afford to lose. Past performance is not indicative of future results. 
            It is crucial to understand the risks involved in trading and to seek advice from a professional financial advisor before making any trading decisions. 
            Automated trading strategies are tools that can assist in executing trades according to predefined rules. However, they do not eliminate the need for oversight. 
            It is your responsibility to regularly monitor your trading activities and make adjustments as necessary to align with your financial goals and risk tolerance.
        </p>
    </div>
    '''
    return footer_html

# Update status table to Dark Green theme
def create_status_table(norgate_data_ok, mongodb_update_ok):
    """Create an HTML table to display the status of Norgate Data and MongoDB Update with conditional color coding."""
    norgate_color = '#dc3545' if not norgate_data_ok else '#e6ffe6'  # Red for False, default green color for True
    mongodb_color = '#dc3545' if not mongodb_update_ok else '#e6ffe6'

    table_html = f'''
    <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
        <tr style="background-color: #006400; color: white; text-align: center;">
            <th colspan="2" style="padding: 12px; font-size: 20px;">System Checks</th>
        </tr>
        <tr style="background-color: #66cc66; color: white; text-align: center;">
            <th style="padding: 10px;">Check</th>
            <th style="padding: 10px;">Status</th>
        </tr>
        <tr style="background-color: {norgate_color};">
            <td style="padding: 8px; border: 1px solid #ddd;">Norgate Data Check</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">
                {'OK' if norgate_data_ok else '<strong style="color: white;">Error</strong>'}
            </td>
        </tr>
        <tr style="background-color: {mongodb_color};">
            <td style="padding: 8px; border: 1px solid #ddd;">UTM Database Update</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: center;">
                {'OK' if mongodb_update_ok else '<strong style="color: white;">Error</strong>'}
            </td>
        </tr>
    </table>
    '''
    return table_html

# Update account balance table to Dark Green theme
def create_account_balance_table(account_info):
    """Manually create an HTML table for the Account Balance Information with centered and larger headers."""
    table_html = f'''
    <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
        <tr style="background-color: #006400; color: white; text-align: center;">
            <th colspan="2" style="padding: 12px; font-size: 20px;">Account Balance Information</th>
        </tr>
        <tr style="background-color: #66cc66; color: white; text-align: center;">
            <th style="padding: 10px;">Field</th>
            <th style="padding: 10px;">Value</th>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Account ID</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{account_info['AccountID']}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Account Type</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{account_info['AccountType']}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Equity</td>
            <td style="padding: 8px; border: 1px solid #ddd;">${account_info['Equity']:,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Cash Balance</td>
            <td style="padding: 8px; border: 1px solid #ddd;">${account_info['CashBalance']:,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Market Value</td>
            <td style="padding: 8px; border: 1px solid #ddd;">${account_info['MarketValue']:,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Total Cost of Open Positions</td>
            <td style="padding: 8px; border: 1px solid #ddd;">${account_info['TotalCost']:,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Buying Power</td>
            <td style="padding: 8px; border: 1px solid #ddd;">${account_info['BuyingPower']:,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Usable Capital (BP + Cost)</td>
            <td style="padding: 8px; border: 1px solid #ddd;">${account_info['UsableCapital']:,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">Required Margin</td>
            <td style="padding: 8px; border: 1px solid #ddd;">${account_info['RequiredMargin']:,.2f}</td>
        </tr>
    </table>
    '''
    return table_html

# create open positions table
def create_open_positions_table(positions_list, table_title, strategy_name_map):
    """
    Generates a formatted HTML table for open positions with UTM styling.

    Parameters:
        positions_list (list): List of positions, each as:
            [symbol, strategy, trade_action, quantity, entry_price, last_close, open_pnl, pnl_pct, entry_date]
        table_title (str): Section title (e.g., "Open Positions")
        strategy_name_map (dict): Mapping of full strategy names to strategy types (e.g. {'MWT-LIVE-HFT-R1000-v1': 'HFT'})

    Returns:
        str: HTML-formatted string for email body.
    """

    if not positions_list:
        return f"<h3>{table_title}</h3><p>No open positions.</p>"

    # Calculate total PnL
    total_open_pnl = sum(pos[6] for pos in positions_list)

    # Table header
    html = f'''
    <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
        <tr style="background-color: #006400; color: white; text-align: center;">
            <th colspan="9" style="padding: 12px; font-size: 20px;">{table_title}</th>
        </tr>
        <tr style="background-color: #66cc66; color: white; text-align: center;">
            <th style="padding: 10px;">Strategy</th>
            <th style="padding: 10px;">Date</th>
            <th style="padding: 10px;">Symbol</th>
            <th style="padding: 10px;">Trade</th>
            <th style="padding: 10px;">Quantity</th>
            <th style="padding: 10px;">Entry Price</th>
            <th style="padding: 10px;">Last Close</th>
            <th style="padding: 10px;">PnL</th>
            <th style="padding: 10px;">PnL %</th>
        </tr>
    '''

    # Table rows
    for row in positions_list:
        symbol, strategy_full, trade_action, quantity, entry_price, last_close, open_pnl, pnl_pct, entry_date = row
        strategy_type = strategy_name_map.get(strategy_full, strategy_full)
        pnl_color = 'green' if open_pnl > 0 else 'red'
        formatted_date = pd.to_datetime(entry_date).strftime('%d/%m/%y')
    
        html += f'''
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">{strategy_type}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{formatted_date}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{symbol}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{trade_action}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{quantity}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{entry_price:.2f}</td>
            <td style="padding: 8px; border: 1px solid #ddd;">{last_close:.2f}</td>
            <td style="padding: 8px; border: 1px solid #ddd; color: {pnl_color};">{open_pnl:.2f}</td>
            <td style="padding: 8px; border: 1px solid #ddd; color: {pnl_color};">{pnl_pct:.2f}%</td>
        </tr>
        '''

    # Final row: Total PnL
    html += f'''
    <tr style="background-color: #e6ffe6; font-weight: bold;">
        <td colspan="7" style="padding: 10px; text-align: right;">Total Open PnL</td>
        <td colspan="2" style="padding: 10px; text-align: left; color: {"green" if total_open_pnl > 0 else "red"};">
            {total_open_pnl:.2f}
        </td>
    </tr>
    </table><br>
    '''

    return html


# Update orders table to Dark Green theme
def create_orders_table(orders_list, table_name):
    """Create an HTML table for orders with centered and larger headers."""
    table_html = f'''
    <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
        <tr style="background-color: #006400; color: white; text-align: center;">
            <th colspan="7" style="padding: 12px; font-size: 20px;">{table_name} Orders List</th>
        </tr>
    '''
    
    if len(orders_list) == 0:
        # If no orders, add a row stating no orders
        table_html += f'''
        <tr style="background-color: #e6ffe6; text-align: center;">
            <td colspan="4" style="padding: 10px; font-size: 16px; color: #333;">No orders for {table_name} strategy.</td>
        </tr>
        '''
    else:
        # Add the headers for orders
        table_html += '''
        <tr style="background-color: #66cc66; color: white; text-align: center;">
            <th style="padding: 10px;">Symbol</th>
            <th style="padding: 10px;">Trade Action</th>
            <th style="padding: 10px;">Quantity</th>
            <th style="padding: 10px;">Limit Price</th>
            <th style="padding: 10px;">Duration</th>
            <th style="padding: 10px;">Order Type</th>
            <th style="padding: 10px;">All or None</th>
        </tr>
        '''
        # Add rows for each order
        for order in orders_list:
            # Handle Limit Price format safely with 2 decimal places
            try:
                limit_price = f"${float(order[3]):,.2f}"  # Format as a float with 2 decimal places
            except (ValueError, TypeError):
                limit_price = str(order[3])  # Fall back to string if invalid

            table_html += f'''
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd;">{order[0]}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{order[1]}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{order[2]}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{limit_price}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{order[4]}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{order[5]}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{order[6]}</td>

            </tr>
            '''

    # Close the table
    table_html += '</table>'
    
    return table_html

# Email Generators
def generate_error_email_header(name):
    """Generate a decorative HTML for the error email header with a red border and title text."""
    header_html = f'''
    <div style="background-color: #ffe6e6; border: 2px solid #dc3545; padding: 15px; text-align: center; font-family: 'Arial', sans-serif; border-radius: 10px; margin-bottom: 20px;">
        <h1 style="font-size: 36px; color: #dc3545; margin-bottom: 5px;">UTM - Trading Signals Error Report</h1>
        <p style="font-size: 18px; color: #333; margin-top: 0;">{name}</p>
        <p style="font-size: 16px; color: #777;">{dt.date.today().strftime("%A, %B %d, %Y")}</p>
    </div>
    '''
    return header_html

def generate_error_email_footer():
    """Generate a decorative HTML for the email footer with a red color scheme for error notifications."""
    footer_html = f'''
    <div style="background-color: #f2f2f2; border-top: 2px solid #dc3545; padding: 15px; text-align: center; font-family: Arial, sans-serif; border-radius: 0 0 10px 10px; margin-top: 20px;">
        <p style="font-size: 14px; color: #333;">This error notification was generated automatically by UTM.</p>
        <p style="font-size: 12px; color: #333;">
            For assistance, contact us at <a href="mailto:support@utm.com.au" style="color: #dc3545; text-decoration: none;">support@utm.com.au</a> 
            or visit our <a href="https://universaltrademanager.com/faq" style="color: #dc3545; text-decoration: none;">FAQ</a> section.
        </p>
        <p style="font-size: 12px; color: #333;">&copy; {dt.date.today().year} UTM Limited (Ltd). All rights reserved.</p>
        <p style="font-size: 10px; color: #555; margin-top: 10px;">
            Disclaimer: Trading involves significant risk and may result in the loss of your invested capital. 
            Trading on margin amplifies this risk as it allows you to trade with borrowed funds, potentially leading to greater losses than your initial investment. 
            You should only trade with money that you can afford to lose. Past performance is not indicative of future results. 
            It is crucial to understand the risks involved in trading and to seek advice from a professional financial advisor before making any trading decisions.
        </p>
    </div>
    '''
    return footer_html

# Email Senders

def send_email(subject, body, recipients, attachments=[]):
    """Send an email."""
    msg = MIMEMultipart()
    msg['From'] = FROM_EMAIL
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    for filepath in attachments:
        try:
            with open(filepath, "rb") as f:
                attach = MIMEBase('application', 'octet-stream')
                attach.set_payload(f.read())
                encoders.encode_base64(attach)
                attach.add_header('Content-Disposition', f'attachment; filename={os.path.basename(filepath)}')
                msg.attach(attach)
        except FileNotFoundError:
            print(f"Attachment {filepath} not found. Skipping...")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.send_message(msg)
    print("Email sent successfully.")
    
def send_error_email(name, error_message, recipients):
    """
    Send a generic error email.

    Args:
        name (str): The title or context of the error.
        error_message (str): The detailed error message or log (HTML formatted).
        recipients (list): List of email recipients.
    """
    # Generate the header and footer for the email
    header = generate_error_email_header(name)
    footer = generate_error_email_footer()

    # Construct the email body
    body = f"{header}<div>{error_message}</div>{footer}"

    # Send the email
    send_email(
        subject=f"UTM - Error Report ({dt.date.today().strftime('%d/%m/%Y')})",
        body=body,
        recipients=recipients
    )

#%% OLD CODE DURING UPDATE

# def send_error_email(name, data_ok, mongo_ok, spy_date, expected_date, recipients):
#     """Send an error email."""
#     error_message = """<p style='color: #dc3545;'>The following critcal issues occurred:</p>"""
#     if not data_ok:
#         error_message += f'''
#         <p>Norgate Data is not up-to-date.</p>
#         <p>Data Date: {spy_date.strftime('%A, %B %d, %Y')}</p>
#         <p>Expected Date: {expected_date.strftime('%A, %B %d, %Y')}</p>
#         '''
#     if not mongo_ok:
#         error_message += "<p>Failed to update MongoDB.</p>"

#     header = generate_error_email_header(name)
#     footer = generate_error_email_footer()

#     body = f"{header}<div>{error_message}</div>{footer}"
#     send_email(
#         subject=f"UTM - Error Report ({dt.date.today().strftime('%d/%m/%Y')})",
#         body=body,
#         recipients=recipients
#     )


# # Update orders table to Dark Green theme
# def create_orders_table(orders_list, table_name):
#     """Create an HTML table for orders with centered and larger headers."""
#     if len(orders_list) == 0:
#         return f"<p>No {table_name} Orders</p>"

#     table_html = f'''
#     <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
#         <tr style="background-color: #006400; color: white; text-align: center;">
#             <th colspan="4" style="padding: 12px; font-size: 20px;">{table_name} Orders List</th>
#         </tr>
#         <tr style="background-color: #66cc66; color: white; text-align: center;">
#             <th style="padding: 10px;">Symbol</th>
#             <th style="padding: 10px;">Action</th>
#             <th style="padding: 10px;">Quantity</th>
#             <th style="padding: 10px;">Limit Price</th>
#         </tr>
#     '''
    
#     for order in orders_list:
#         # Handle Limit Price format safely with 2 decimal places
#         try:
#             limit_price = f"${float(order[3]):,.2f}"  # Format as a float with 2 decimal places
#         except (ValueError, TypeError):
#             limit_price = str(order[3])  # Fall back to string if invalid

#         table_html += f'''
#         <tr>
#             <td style="padding: 8px; border: 1px solid #ddd;">{order[0]}</td>
#             <td style="padding: 8px; border: 1px solid #ddd;">{order[1]}</td>
#             <td style="padding: 8px; border: 1px solid #ddd;">{order[2]}</td>
#             <td style="padding: 8px; border: 1px solid #ddd;">{order[3]}</td>
#             <td style="padding: 8px; border: 1px solid #ddd;">{limit_price}</td>
#         </tr>
#         '''
#     table_html += '</table>'
    
#     return table_html