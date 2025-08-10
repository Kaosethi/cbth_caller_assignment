"""
Business day and holiday checking utilities
"""
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import Dict, Any, Optional
import logging

from config.settings import TIMEZONE, MOCK_DATA_CONFIG

logger = logging.getLogger(__name__)

class BusinessDayChecker:
    """Handles holiday and weekend checking logic"""
    
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.holiday_data = None
        
    def load_holiday_data(self, holiday_source: Optional[str] = None) -> Dict[str, Any]:
        """
        Load holiday data from source (Google Sheets or mock CSV)
        
        Args:
            holiday_source: Path to holiday data source (optional, uses config default)
            
        Returns:
            Dict with holiday data loading results
        """
        try:
            if MOCK_DATA_CONFIG['use_mock_data']:
                # Load from mock CSV file
                file_path = holiday_source or MOCK_DATA_CONFIG['holiday_data_file']
                self.holiday_data = pd.read_csv(file_path)
                logger.info(f"Loaded holiday data from mock file: {file_path}")
            else:
                # TODO: Load from Google Sheets when credentials available
                logger.info("Loading holiday data from Google Sheets (TODO: implement)")
                # This will be implemented in io/google_sheets.py
                pass
                
            return {
                'success': True,
                'rows_loaded': len(self.holiday_data) if self.holiday_data is not None else 0,
                'source': 'mock_csv' if MOCK_DATA_CONFIG['use_mock_data'] else 'google_sheets'
            }
            
        except Exception as e:
            logger.error(f"Failed to load holiday data: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'source': 'mock_csv' if MOCK_DATA_CONFIG['use_mock_data'] else 'google_sheets'
            }
    
    def is_holiday_or_weekend(self, check_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Check if given date (or today) is a holiday or weekend
        
        Args:
            check_date: Date to check (defaults to today in Bangkok timezone)
            
        Returns:
            Dict with check results and details
        """
        if check_date is None:
            check_date = datetime.now(self.timezone)
        
        # Ensure date is timezone-aware
        if check_date.tzinfo is None:
            check_date = self.timezone.localize(check_date)
        
        # Convert to Bangkok timezone if needed
        check_date = check_date.astimezone(self.timezone)
        
        result = {
            'date': check_date.strftime('%Y-%m-%d'),
            'is_weekend': False,
            'is_holiday': False,
            'holiday_name': None,
            'should_skip': False,
            'reason': None
        }
        
        # Check if weekend (Saturday = 5, Sunday = 6)
        if check_date.weekday() >= 5:
            result['is_weekend'] = True
            result['should_skip'] = True
            result['reason'] = f"Weekend ({check_date.strftime('%A')})"
            logger.info(f"Date {result['date']} is a weekend: {result['reason']}")
            return result
        
        # Check if holiday
        if self.holiday_data is not None:
            date_str = check_date.strftime('%Y-%m-%d')
            holiday_row = self.holiday_data[self.holiday_data['date'] == date_str]
            
            if not holiday_row.empty:
                is_holiday = holiday_row.iloc[0].get('is_holiday', False)
                if is_holiday:
                    result['is_holiday'] = True
                    result['holiday_name'] = holiday_row.iloc[0].get('holiday_name', 'Unknown Holiday')
                    result['should_skip'] = True
                    result['reason'] = f"Holiday: {result['holiday_name']}"
                    logger.info(f"Date {result['date']} is a holiday: {result['reason']}")
                    return result
        
        # Not a holiday or weekend
        result['reason'] = "Business day - proceed with automation"
        logger.info(f"Date {result['date']} is a business day")
        return result
    
    def get_next_business_day(self, from_date: Optional[datetime] = None) -> datetime:
        """
        Get the next business day (non-holiday, non-weekend)
        
        Args:
            from_date: Starting date (defaults to today)
            
        Returns:
            Next business day as datetime
        """
        if from_date is None:
            from_date = datetime.now(self.timezone)
        
        current_date = from_date
        max_attempts = 10  # Prevent infinite loop
        attempts = 0
        
        while attempts < max_attempts:
            current_date += timedelta(days=1)
            check_result = self.is_holiday_or_weekend(current_date)
            
            if not check_result['should_skip']:
                logger.info(f"Next business day from {from_date.strftime('%Y-%m-%d')} is {current_date.strftime('%Y-%m-%d')}")
                return current_date
            
            attempts += 1
        
        # Fallback if we can't find a business day
        logger.warning(f"Could not find next business day within {max_attempts} attempts")
        return current_date

def create_mock_holiday_data():
    """Create sample holiday data for testing"""
    holidays = [
        {'date': '2025-01-01', 'is_holiday': True, 'holiday_name': 'New Year\'s Day'},
        {'date': '2025-02-14', 'is_holiday': True, 'holiday_name': 'Valentine\'s Day'},
        {'date': '2025-04-13', 'is_holiday': True, 'holiday_name': 'Songkran Festival'},
        {'date': '2025-04-14', 'is_holiday': True, 'holiday_name': 'Songkran Festival'},
        {'date': '2025-04-15', 'is_holiday': True, 'holiday_name': 'Songkran Festival'},
        {'date': '2025-05-01', 'is_holiday': True, 'holiday_name': 'Labour Day'},
        {'date': '2025-12-25', 'is_holiday': True, 'holiday_name': 'Christmas Day'},
        {'date': '2025-12-31', 'is_holiday': True, 'holiday_name': 'New Year\'s Eve'},
    ]
    
    return pd.DataFrame(holidays)

if __name__ == "__main__":
    # Test the business day checker
    checker = BusinessDayChecker()
    
    # Create mock data for testing
    mock_holidays = create_mock_holiday_data()
    mock_holidays.to_csv('data/mock/holiday_data.csv', index=False)
    print("Created mock holiday data")
    
    # Load and test
    result = checker.load_holiday_data()
    print(f"Holiday data load result: {result}")
    
    # Test today
    today_result = checker.is_holiday_or_weekend()
    print(f"Today check result: {today_result}")
    
    # Test specific dates
    test_dates = [
        datetime(2025, 1, 1),   # New Year (holiday)
        datetime(2025, 8, 10),  # Saturday (weekend)
        datetime(2025, 8, 11),  # Sunday (weekend)
        datetime(2025, 8, 12),  # Monday (business day)
    ]
    
    for test_date in test_dates:
        result = checker.is_holiday_or_weekend(test_date)
        print(f"Date {test_date.strftime('%Y-%m-%d')}: {result}")
