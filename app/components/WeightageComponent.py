import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Set, FrozenSet, Optional, Union, Any

DEFAULT_WEIGHTAGE_FILE = r'citizen_weightage.xlsx'

logger = logging.getLogger(__name__)

class InvalidConditionError(Exception):
    """Raised when no valid conditions are found for an entity."""
    pass

class WeightCalculator:
    """
    A class to calculate weights based on entity and condition combinations
    from a predefined Excel file.
    """
    FIELD_MAPPING = {
        # key = field , value = Name of Excel Column 
        'citizenship_no': 'Citizenship Number',
        'grandfathers_name': 'Grandfathers Name',
        'fathers_name': 'Fathers Name',
        'name': 'Name',
        'dob': 'DOB',
        'account_no': 'Account Number',
        'pan_no': 'PAN',
        'spouse_name': 'Spouse Name',
        'registration_no': 'Registration',
    }
    
    ENTITY_CONDITIONS = {
        'institutional': ['name', 'pan_no', 'registration_no'],
        'account': ['name', 'account_no'],
        'individual': ['name', 'fathers_name', 'dob', 'citizenship_no', 'grandfathers_name', 'spouse_name']
    }

    REQUIRED_COLUMNS = ['Entity', 'Condition']
    
    def __init__(self, file_path: Optional[str] = None):
        """
        Initialize with Excel file containing weight distributions.
        """
        self.weightage_file_path = file_path or DEFAULT_WEIGHTAGE_FILE
        self.df = self._load_weightage_table()
        self.weight_columns = list(self.FIELD_MAPPING.values())
        
        if not self.df.empty:
            self._preprocess_data()
    
    def _load_weightage_table(self) -> pd.DataFrame:
        """
        Load and validate the weightage table from the Excel file.
        
        Returns:
            DataFrame containing the weightage data or empty DataFrame if loading fails.
        """
        try:
            df = pd.read_excel(self.weightage_file_path)
            missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns in weightage file: {missing_columns}")
                return pd.DataFrame()
            
            weight_columns = [col for col in self.FIELD_MAPPING.values() if col in df.columns]
            if not weight_columns:
                logger.error("No weight columns found in the weightage file")
                return pd.DataFrame()
                
            return df
            
        except FileNotFoundError:
            logger.error(f"Weightage file not found: {self.weightage_file_path}")
        except pd.errors.EmptyDataError:
            logger.error(f"Weightage file is empty: {self.weightage_file_path}")
        except pd.errors.ParserError:
            logger.error(f"Error parsing Excel file: {self.weightage_file_path}")
        except Exception as e:
            logger.exception(f"Unexpected error loading weightage table: {e}")
            
        return pd.DataFrame()

    def _preprocess_data(self) -> None:
        """
        1) Preprocess the condition column in Excel to extract 
        Standard field names into sorted frozen sets for matching.
        2) Identify duplicate entries in the data.
        """
        self.df['Condition_Set'] = self.df['Condition'].apply(
            lambda x: self._normalize_condition(x) if isinstance(x, str) else frozenset()
        )
        
        duplicates = self.df[self.df.duplicated(['Entity', 'Condition_Set'], keep=False)]
        if not duplicates.empty:
            duplicate_count = len(duplicates)
            logger.warning(f"Found {duplicate_count} duplicate entries in weightage data")
            
            if duplicate_count > 0:
                sample = duplicates.head(min(3, duplicate_count))
                logger.debug(f"Sample duplicates:\n{sample}")
    
    @staticmethod
    def _normalize_condition(condition_str: str) -> FrozenSet[str]:
        if not condition_str or not isinstance(condition_str, str):
            return frozenset()
            
        return frozenset(
            field.strip().lower() 
            for field in condition_str.split(',')
            if field.strip()
        )
    
    def get_weights(self, entity: str, conditions: List[str]) -> Dict[str, float]:
        """
        Core Function to get the weight distribution for a specific entity and its conditions.
        """
        if self.df.empty:
            logger.error("Weightage table is empty")
            raise ValueError("Weightage table is empty or could not be loaded")
            
        if not conditions:
            logger.error("No conditions provided")
            raise ValueError("No conditions provided")
            
        if not entity or not isinstance(entity, str):
            logger.error(f"Invalid entity: {entity}")
            raise ValueError(f"Invalid entity: {entity}")
        
        entity = entity.strip().lower()
        
        if entity not in self.ENTITY_CONDITIONS:
            logger.error(f"Unknown entity type: {entity}")
            raise ValueError(f"Unknown entity type: {entity}. Valid entities are: {list(self.ENTITY_CONDITIONS.keys())}")
        
        entity_specific_conditions = self.ENTITY_CONDITIONS.get(entity, [])
        
        valid_conditions = [cond for cond in conditions if cond in self.FIELD_MAPPING and cond in entity_specific_conditions]
        
        if not valid_conditions:
            entity_valid_fields = self.ENTITY_CONDITIONS.get(entity, [])
            provided_fields = [f"'{cond}'" for cond in conditions]
            
            error_msg = (
                f"No valid conditions found for entity '{entity}'. "
                f"Provided: {', '.join(provided_fields)}. "
                f"Valid conditions for '{entity}' are: {', '.join([f'{c!r}' for c in entity_valid_fields])}"
            )
            logger.error(error_msg)
            raise InvalidConditionError(error_msg)
        
        excel_conditions = [self.FIELD_MAPPING[cond] for cond in valid_conditions]
        condition_set = self._normalize_condition(','.join(excel_conditions))
        
        matches = self.df[
            (self.df['Entity'].str.lower() == entity) &
            (self.df['Condition_Set'] == condition_set)
        ]
        
        if matches.empty:
            logger.debug(f"No matching weights found for entity '{entity}' with conditions {condition_set}")
            return self._get_equal_weights(valid_conditions)
        
        row = matches.iloc[0]
        
        raw_weights = {}
        total_weight = 0
        
        for key in valid_conditions:
            excel_key = self.FIELD_MAPPING[key]
            
            if excel_key in row:
                weight = row[excel_key]
                
                if isinstance(weight, (np.integer, np.floating, int, float)):
                    weight = float(weight)
                    
                if pd.notna(weight) and weight > 0: 
                    raw_weights[key] = weight
                    total_weight += weight
        
        if total_weight == 0:
            logger.debug(f"Total weight is zero for entity '{entity}' with conditions {condition_set}")
            return self._get_equal_weights(valid_conditions)
        
        normalized_weights = {
            key: float(round(weight / total_weight, 2)) 
            for key, weight in raw_weights.items()
        }
        
        for key in conditions:
            if key in entity_specific_conditions and key not in normalized_weights:
                normalized_weights[key] = 0.0
        
        self._adjust_for_rounding(normalized_weights, valid_conditions)
        
        return normalized_weights
    
    def _adjust_for_rounding(self, weights: Dict[str, float], valid_fields: List[str]) -> None:
        if not weights or not valid_fields:
            return
        
        weight_sum = sum(weights.values())
        
        if weight_sum == 0 or round(weight_sum, 2) == 1.0:
            return
            
        diff = round(1.0 - weight_sum, 2)
        
        valid_weight_fields = [k for k in weights.keys() if k in valid_fields and weights.get(k, 0) > 0]
        
        if valid_weight_fields:
            max_key = max(valid_weight_fields, key=lambda k: weights[k])
            weights[max_key] = float(round(weights[max_key] + diff, 2))
    
    def _get_equal_weights(self, conditions: List[str]) -> Dict[str, float]:
        if not conditions:
            return {}
        
        count = len(conditions)
        weight = float(round(1.0 / count, 2))
        result = {cond: weight for cond in conditions}
        
        self._adjust_for_rounding(result, conditions)
        
        return result
    
    def get_all_entities(self) -> List[str]:
        if self.df.empty:
            return []
            
        return self.df['Entity'].unique().tolist()
    
    def get_all_conditions(self, entity: Optional[str] = None) -> List[str]:
        if self.df.empty:
            return []
        
        if entity:
            entity = entity.lower()
            filtered_df = self.df[self.df['Entity'].str.lower() == entity]
            return filtered_df['Condition'].unique().tolist()
            
        return self.df['Condition'].unique().tolist()