"""
Database client for extracting player data from PC and Mobile game databases
Supports both mock CSV data (for development) and real database connections
"""
import pandas as pd
import mysql.connector
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta
import pytz

from config.settings import DATABASE_CONFIG, MOCK_DATA_CONFIG, TIMEZONE, NO_LOGIN_WINDOWS

logger = logging.getLogger(__name__)

class GameDatabaseClient:
    """Client for extracting player data from game databases"""
    
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.pc_connection = None
        self.mobile_connection = None
        
    def connect_databases(self) -> Dict[str, Any]:
        """
        Establish connections to PC and Mobile databases
        
        Returns:
            Dict with connection results
        """
        if MOCK_DATA_CONFIG['use_mock_data']:
            logger.info("Using mock data - skipping database connections")
            return {
                'success': True,
                'pc_connected': False,
                'mobile_connected': False,
                'using_mock': True
            }
        
        results = {
            'success': True,
            'pc_connected': False,
            'mobile_connected': False,
            'using_mock': False,
            'errors': []
        }
        
        # Connect to PC database
        try:
            self.pc_connection = mysql.connector.connect(**DATABASE_CONFIG['pc_db'])
            results['pc_connected'] = True
            logger.info("Connected to PC database")
        except Exception as e:
            error_msg = f"Failed to connect to PC database: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            results['success'] = False
        
        # Connect to Mobile database
        try:
            self.mobile_connection = mysql.connector.connect(**DATABASE_CONFIG['mobile_db'])
            results['mobile_connected'] = True
            logger.info("Connected to Mobile database")
        except Exception as e:
            error_msg = f"Failed to connect to Mobile database: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            results['success'] = False
        
        return results
    
    def extract_candidates(self, 
                          no_login_window: Tuple[int, int] = NO_LOGIN_WINDOWS[0],
                          mode: str = 'FULL',
                          tier_scope: str = 'BOTH') -> Dict[str, Any]:
        """
        Extract candidate players from PC and Mobile databases
        
        Args:
            no_login_window: Tuple of (min_days, max_days) for no-login filter
            mode: 'FULL' for initial extraction, 'NONA_TOPUP' for capacity expansion
            tier_scope: 'BOTH', 'TIER_A', 'NON_A' - which tiers to extract
            
        Returns:
            Dict with extraction results and candidate lists
        """
        logger.info(f"Extracting candidates - Window: {no_login_window}, Mode: {mode}, Scope: {tier_scope}")
        
        if MOCK_DATA_CONFIG['use_mock_data']:
            return self._extract_from_mock_data(no_login_window, mode, tier_scope)
        else:
            return self._extract_from_databases(no_login_window, mode, tier_scope)
    
    def _extract_from_mock_data(self, 
                               no_login_window: Tuple[int, int],
                               mode: str,
                               tier_scope: str) -> Dict[str, Any]:
        """Extract candidates from mock CSV files"""
        try:
            # Load mock data
            pc_data = pd.read_csv(MOCK_DATA_CONFIG['pc_data_file'])
            mobile_data = pd.read_csv(MOCK_DATA_CONFIG['mobile_data_file'])
            
            # Add source column to track PC vs Mobile
            pc_data['source'] = 'PC'
            mobile_data['source'] = 'Mobile'
            
            # Combine datasets
            combined_data = pd.concat([pc_data, mobile_data], ignore_index=True)
            
            # Apply filters
            filtered_data = self._apply_extraction_filters(
                combined_data, no_login_window, mode, tier_scope
            )
            
            # Split into Tier A and Non-Tier-A
            tier_a_data = filtered_data[filtered_data['tier_flag'] == 'A'].copy()
            non_tier_a_data = filtered_data[filtered_data['tier_flag'] != 'A'].copy()
            
            # Remove duplicates by phone number (keep first occurrence)
            tier_a_data = tier_a_data.drop_duplicates(subset=['phone'], keep='first')
            non_tier_a_data = non_tier_a_data.drop_duplicates(subset=['phone'], keep='first')
            
            result = {
                'success': True,
                'extraction_mode': mode,
                'no_login_window': no_login_window,
                'tier_scope': tier_scope,
                'total_raw_records': len(combined_data),
                'total_after_filters': len(filtered_data),
                'tier_a_count': len(tier_a_data),
                'non_tier_a_count': len(non_tier_a_data),
                'tier_a_candidates': tier_a_data.to_dict('records'),
                'non_tier_a_candidates': non_tier_a_data.to_dict('records'),
                'exclusions': self._generate_exclusion_report(combined_data, filtered_data),
                'data_source': 'mock_csv'
            }
            
            logger.info(f"Mock extraction complete - Tier A: {result['tier_a_count']}, Non-Tier-A: {result['non_tier_a_count']}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract from mock data: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'data_source': 'mock_csv'
            }
    
    def _extract_from_databases(self, 
                               no_login_window: Tuple[int, int],
                               mode: str,
                               tier_scope: str) -> Dict[str, Any]:
        """Extract candidates from real databases"""
        # TODO: Implement real database extraction
        logger.info("Real database extraction not yet implemented")
        
        # Placeholder query structure for when DB credentials are available
        base_query = """
        SELECT 
            username,
            phone,
            email,
            last_login_at,
            tier_flag,
            reward_tier,
            redemption_history,
            registration_date,
            total_deposits,
            last_deposit_date
        FROM players 
        WHERE 
            last_login_at BETWEEN DATE_SUB(NOW(), INTERVAL {max_days} DAY) 
                              AND DATE_SUB(NOW(), INTERVAL {min_days} DAY)
            AND phone IS NOT NULL 
            AND phone != ''
            AND tier_flag IN ({tier_filter})
        ORDER BY last_login_at DESC
        """
        
        return {
            'success': False,
            'error': 'Real database extraction not implemented yet',
            'data_source': 'database',
            'todo': 'Implement SQL queries for PC and Mobile databases'
        }
    
    def _apply_extraction_filters(self, 
                                 data: pd.DataFrame,
                                 no_login_window: Tuple[int, int],
                                 mode: str,
                                 tier_scope: str) -> pd.DataFrame:
        """Apply business rule filters to raw data"""
        min_days, max_days = no_login_window
        current_time = datetime.now(self.timezone)
        
        # Convert last_login_at to datetime if it's a string
        if 'last_login_at' in data.columns:
            data['last_login_at'] = pd.to_datetime(data['last_login_at'])
        
        # Filter by no-login window
        cutoff_min = current_time - timedelta(days=max_days)
        cutoff_max = current_time - timedelta(days=min_days)
        
        filtered_data = data[
            (data['last_login_at'] >= cutoff_min) & 
            (data['last_login_at'] <= cutoff_max)
        ].copy()
        
        # Filter by repeat count (≥2 in current month)
        # TODO: Implement repeat count calculation from Compile tab
        # For now, assume all records meet this criteria
        
        # Filter by tier scope
        if tier_scope == 'TIER_A':
            filtered_data = filtered_data[filtered_data['tier_flag'] == 'A']
        elif tier_scope == 'NON_A':
            filtered_data = filtered_data[filtered_data['tier_flag'] != 'A']
        # 'BOTH' includes all tiers
        
        # Remove records with empty phone numbers
        filtered_data = filtered_data[
            (filtered_data['phone'].notna()) & 
            (filtered_data['phone'] != '')
        ]
        
        return filtered_data
    
    def _generate_exclusion_report(self, 
                                  raw_data: pd.DataFrame,
                                  filtered_data: pd.DataFrame) -> Dict[str, Any]:
        """Generate report of excluded records and reasons"""
        total_raw = len(raw_data)
        total_filtered = len(filtered_data)
        total_excluded = total_raw - total_filtered
        
        # TODO: Implement detailed exclusion tracking
        # For now, provide summary counts
        
        return {
            'total_excluded': total_excluded,
            'exclusion_reasons': {
                'outside_login_window': 0,  # TODO: Calculate actual counts
                'insufficient_repeat_count': 0,
                'missing_phone': 0,
                'duplicate_phone': 0,
                'cooldown_period': 0
            }
        }
    
    def close_connections(self):
        """Close database connections"""
        if self.pc_connection:
            self.pc_connection.close()
            logger.info("Closed PC database connection")
        
        if self.mobile_connection:
            self.mobile_connection.close()
            logger.info("Closed Mobile database connection")

def create_mock_player_data():
    """Create sample player data for testing"""
    import random
    from datetime import datetime, timedelta
    
    # Generate mock PC data
    pc_players = []
    for i in range(100):
        last_login = datetime.now() - timedelta(days=random.randint(1, 30))
        pc_players.append({
            'username': f'pc_player_{i:03d}',
            'phone': f'08{random.randint(10000000, 99999999)}',
            'email': f'pc_player_{i:03d}@example.com',
            'last_login_at': last_login.strftime('%Y-%m-%d %H:%M:%S'),
            'tier_flag': random.choice(['A', 'B', 'C']),
            'reward_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'redemption_history': random.randint(0, 10),
            'registration_date': (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
            'total_deposits': random.randint(1000, 50000),
            'last_deposit_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d')
        })
    
    # Generate mock Mobile data
    mobile_players = []
    for i in range(80):
        last_login = datetime.now() - timedelta(days=random.randint(1, 30))
        mobile_players.append({
            'username': f'mobile_player_{i:03d}',
            'phone': f'09{random.randint(10000000, 99999999)}',
            'email': f'mobile_player_{i:03d}@example.com',
            'last_login_at': last_login.strftime('%Y-%m-%d %H:%M:%S'),
            'tier_flag': random.choice(['A', 'B', 'C']),
            'reward_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'redemption_history': random.randint(0, 10),
            'registration_date': (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
            'total_deposits': random.randint(1000, 50000),
            'last_deposit_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d')
        })
    
    return pd.DataFrame(pc_players), pd.DataFrame(mobile_players)

if __name__ == "__main__":
    # Create mock data for testing
    pc_data, mobile_data = create_mock_player_data()
    
    pc_data.to_csv('data/mock/pc_data.csv', index=False)
    mobile_data.to_csv('data/mock/mobile_data.csv', index=False)
    
    print(f"Created mock PC data: {len(pc_data)} records")
    print(f"Created mock Mobile data: {len(mobile_data)} records")
    
    # Test extraction
    client = GameDatabaseClient()
    result = client.extract_candidates()
    
    print(f"Extraction result: {result['success']}")
    if result['success']:
        print(f"Tier A candidates: {result['tier_a_count']}")
        print(f"Non-Tier-A candidates: {result['non_tier_a_count']}")
        print(f"Total exclusions: {result['exclusions']['total_excluded']}")
