import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_daily_metrics():
    """Fetch global daily metrics from The Graph"""
    url = "https://gateway.thegraph.com/api/35a11a8ff03ad3b8f19a3cecf1b73b58/subgraphs/id/C92DhHwGBxuUhuEvPRnNpMSRF3XEVSuS8acsNr3NvVFN"
    
    query = """
    {
        dailyMetrics {
            activeAccounts
            averageTransferAmount
            blacklistedAccounts
            fee
            hasFeeDecrease
            hasFeeIncrease
            id
            minTransferAmount
            maxTransferAmount
            newAccounts
            timestamp
            totalFees
            totalTransferValue
            totalTransfers
            transferCount
            uniqueReceivers
            uniqueSenders
            volume
        }
    }
    """
    
    try:
        response = requests.post(url, json={'query': query})
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            logger.error(f"GraphQL Errors: {data['errors']}")
            return pd.DataFrame()
            
        metrics = data['data']['dailyMetrics']
        logger.info(f"Fetched {len(metrics)} daily metrics records")
        return pd.DataFrame(metrics)
        
    except Exception as e:
        logger.error(f"Error fetching daily metrics: {str(e)}")
        return pd.DataFrame()

def fetch_user_metrics(address):
    """Fetch metrics for a specific user address"""
    url = "https://gateway.thegraph.com/api/35a11a8ff03ad3b8f19a3cecf1b73b58/subgraphs/id/C92DhHwGBxuUhuEvPRnNpMSRF3XEVSuS8acsNr3NvVFN"
    
    query = """
    {
        userDailyMetrics(
            where: {
                user: "%s"
            }
            orderBy: timestamp
            orderDirection: desc
            first: 1000
        ) {
            timestamp
            date
            endDayBalance
            totalReceived
            totalTransferred
            transferCount
            averageTransferAmount
            distinctReceivers
            distinctSenders
            feesPaid
            maxTransferAmount
            receivedCount
            user {
                address
                balance
            }
        }
    }
    """ % address
    
    try:
        response = requests.post(url, json={'query': query})
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            logger.error(f"GraphQL Errors: {data['errors']}")
            return pd.DataFrame()
            
        metrics = data['data']['userDailyMetrics']
        logger.info(f"Fetched {len(metrics)} records for address {address}")
        return pd.DataFrame(metrics)
        
    except Exception as e:
        logger.error(f"Error fetching user metrics: {str(e)}")
        return pd.DataFrame()

def process_daily_metrics(df):
    """Process global daily metrics data"""
    if df.empty:
        return df
        
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
    
    # Convert numeric columns
    numeric_cols = [
        'activeAccounts', 'averageTransferAmount', 'blacklistedAccounts',
        'fee', 'minTransferAmount', 'maxTransferAmount', 'newAccounts',
        'totalFees', 'totalTransferValue', 'totalTransfers', 'transferCount',
        'uniqueReceivers', 'uniqueSenders', 'volume'
    ]
    
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert boolean columns
    df['hasFeeDecrease'] = df['hasFeeDecrease'].astype(bool)
    df['hasFeeIncrease'] = df['hasFeeIncrease'].astype(bool)
    
    return df.sort_values('timestamp')

def process_user_metrics(df):
    """Process user-specific metrics data"""
    if df.empty:
        return df
    
    # Extract user fields
    df['user_address'] = df['user'].apply(lambda x: x['address'])
    df['user_balance'] = df['user'].apply(lambda x: float(x['balance']))
    df.drop('user', axis=1, inplace=True)
    
    # Convert timestamps
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
    df['date'] = pd.to_datetime(df['date'])
    
    # Convert numeric columns
    numeric_cols = [
        'endDayBalance', 'totalReceived', 'totalTransferred', 
        'transferCount', 'averageTransferAmount', 'distinctReceivers',
        'distinctSenders', 'feesPaid', 'maxTransferAmount', 'receivedCount'
    ]
    
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df.sort_values('date')

def get_metric_info():
    """Get information about available metrics and their descriptions"""
    return {
        'global_metrics': {
            'activeAccounts': {'name': 'Active Accounts', 'description': 'Number of accounts that made transactions'},
            'averageTransferAmount': {'name': 'Average Transfer', 'description': 'Average amount per transfer'},
            'totalTransferValue': {'name': 'Total Transfer Value', 'description': 'Total value transferred'},
            'transferCount': {'name': 'Transfer Count', 'description': 'Number of transfers'},
            'uniqueReceivers': {'name': 'Unique Receivers', 'description': 'Number of unique receiving addresses'},
            'uniqueSenders': {'name': 'Unique Senders', 'description': 'Number of unique sending addresses'},
            'volume': {'name': 'Volume', 'description': 'Total transaction volume'},
            'newAccounts': {'name': 'New Accounts', 'description': 'Number of new accounts created'}
        },
        'user_metrics': {
            'endDayBalance': {'name': 'End Day Balance', 'description': 'Balance at end of day'},
            'totalReceived': {'name': 'Total Received', 'description': 'Total amount received'},
            'totalTransferred': {'name': 'Total Transferred', 'description': 'Total amount transferred'},
            'transferCount': {'name': 'Transfer Count', 'description': 'Number of transfers made'},
            'averageTransferAmount': {'name': 'Average Transfer', 'description': 'Average amount per transfer'},
            'distinctReceivers': {'name': 'Unique Receivers', 'description': 'Number of unique addresses received from'},
            'distinctSenders': {'name': 'Unique Senders', 'description': 'Number of unique addresses sent to'},
            'feesPaid': {'name': 'Fees Paid', 'description': 'Total fees paid'}
        }
    }

def format_large_number(num):
    """Format large numbers for display"""
    if pd.isna(num):
        return "N/A"
    if abs(num) >= 1e9:
        return f"{num/1e9:.1f}B"
    if abs(num) >= 1e6:
        return f"{num/1e6:.1f}M"
    if abs(num) >= 1e3:
        return f"{num/1e3:.1f}K"
    return f"{num:.2f}"