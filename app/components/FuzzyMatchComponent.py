import os
import gc
import logging
import traceback
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process, utils

# Assuming these are defined in your project's utility and constants files
from app.Utils import standardize_date_format, convert_bs_to_ad
from app.Constants import PREFILTER_THRESHOLD, MAX_CANDIDATES_AFTER_PREFILTER, INDIVIDUAL_FIELD_MAPPING, INSTITUTION_FIELD_MAPPING , MAX_MATCHES_TO_STORE
from app.components.WeightageComponent import WeightCalculator

from qrlib.QRComponent import QRComponent
from qrlib.QRUtils import display

logger = logging.getLogger(__name__)

class FuzzyMatcherComponent(QRComponent):
    """
    Component for fuzzy matching with dynamic weightage, now refactored to handle
    both individual and institution entities seamlessly.
    """

    def __init__(self):
        super().__init__()
        self.weightage_manager = WeightCalculator()

    # ==============================================================================
    # Similarity and Preprocessing Methods
    # ==============================================================================

    def _preprocess_text(self, text: str) -> str:
        """Clean and normalize text for comparison."""
        if pd.isna(text) or text is None:
            return ""
        try:      
            cleaned_text = str(text).strip().replace(" ", "")
            return utils.default_process(cleaned_text)
        except Exception as e:
            logger.error(f"Error preprocessing text: {e}", exc_info=True)
            return ""

    def _preprocess_dataframe_column(self, series: pd.Series) -> pd.Series:
        """Vectorized preprocessing of a DataFrame column."""
        if series.empty:
            return series
        return series.fillna("").astype(str).apply(self._preprocess_text)

    def _calculate_batch_text_similarity(self, series: pd.Series, query_value: str) -> pd.Series:
        """Calculate similarity between a series of values and a query value."""
        if series.empty or pd.isna(query_value) or not query_value:
            return pd.Series(0.0, index=series.index)
        try:
            processed_query = self._preprocess_text(query_value)
            if not processed_query:
                return pd.Series(0.0, index=series.index)
            processed_series = self._preprocess_dataframe_column(series)
            similarities = process.cdist(
                processed_series, [processed_query], scorer=fuzz.ratio,
                dtype=np.uint8, workers=max(1, os.cpu_count() // 2)
            )
            return pd.Series(similarities.flatten() / 100.0, index=series.index)
        except Exception as e:
            logger.error(f"Error in batch text similarity: {e}", exc_info=True)
            return pd.Series(0.0, index=series.index)

    def _calculate_batch_date_similarity(self, date_series: pd.Series, query_date: str) -> pd.Series:
        """Calculate similarity between a series of dates and a query date."""
        if date_series.empty or pd.isna(query_date) or not query_date:
            return pd.Series(0.0, index=date_series.index)
        try:
            std_query_date = standardize_date_format(query_date)
            if std_query_date is None:
                return pd.Series(0.0, index=date_series.index)
            std_date_series = date_series.apply(standardize_date_format).fillna("")
            similarities = process.cdist(
                std_date_series, [std_query_date], scorer=fuzz.ratio,
                dtype=np.uint8, workers=max(1, os.cpu_count() // 2)
            )
            return pd.Series(similarities.flatten() / 100.0, index=date_series.index)
        except Exception as e:
            logger.info(f"Batch date comparison failed: {e}")
            return pd.Series(0.0, index=date_series.index)

    # ==============================================================================
    # Helper Methods for Criteria, Entity Type, and Weights
    # ==============================================================================

    def _determine_entity_type(self, record: Dict[str, Any]) -> str:
        """Determine the entity type from the source record."""
        try:
            # Primary method: check for the 'entity_type' key from the model
            entity_type = record.get("entity_type")
            if entity_type and isinstance(entity_type, str):
                return entity_type.lower()

            # Fallback logic if 'entity_type' is not present
            if record.get("company_registration_number") or record.get("company_name"):
                return "institution"
            if record.get("citizenship_number") or record.get("fathers_name") or record.get("person_name"):
                return "individual"
            
            return "individual"  # Default if no specific fields are found
        except Exception as e:
            logger.error(f"Error determining entity type: {e}", exc_info=True)
            return "individual"

    def _get_available_criteria(self, mapped_record: Dict[str, Any]) -> List[str]:
        """Determine which matching criteria are available in the mapped record."""
        return [
            criterion for criterion, value in mapped_record.items()
            if not pd.isna(value) and value
        ]

    def _get_normalized_weights(self, record: Dict[str, Any], available_criteria: List[str]) -> Dict[str, float]:
        """Get normalized weights for the matching criteria based on entity type."""
        try:
            entity_type = self._determine_entity_type(record)

            logger.info(f"Getting weights for entity type: {entity_type} with criteria: {available_criteria}")
            display(f"Getting weights for entity type: {entity_type} with criteria: {available_criteria}")

            weights = self.weightage_manager.get_weights(entity_type, available_criteria)
            logger.info(f"Normalized weights: {weights}")
            return weights
        
        except Exception as e:
            logger.error(f"Error getting normalized weights: {e}", exc_info=True)
            # Fallback to equal weights
            return {crit: 1.0 / len(available_criteria) for crit in available_criteria} if available_criteria else {}



    def match(
        self,
        cbs_data: pd.DataFrame,
        source_record: Dict[str, Any],
        final_threshold: float = 0.9,
    ) -> pd.DataFrame:
        """CORE function to Match a single source record against CBS data using weighted fuzzy matching"""
        display(f"Initiating match for source record. CBS data shape: {cbs_data.shape}")
        logger.info(f"Source Record (raw): {source_record}")

        try:
            if cbs_data.empty:
                logger.warning("CBS data is empty. No matching possible.")
                return pd.DataFrame()

            # 1. Determine entity type and select mapping
            entity_type = self._determine_entity_type(source_record)
            field_mapping = INDIVIDUAL_FIELD_MAPPING if entity_type == 'individual' else INSTITUTION_FIELD_MAPPING
            display(f"Determined Entity Type: '{entity_type}'. Using corresponding field mapping.")

            # 2. flatten nested source record for easy mapping
            flat_source_record = source_record.copy()  # Start with top-level keys
            if entity_type == 'individual' and isinstance(source_record.get('individual_details'), dict):
                # Update with the nested details. Nested values will overwrite top-level ones if keys conflict.
                flat_source_record.update(source_record['individual_details'])
            elif entity_type == 'institution' and isinstance(source_record.get('institution_details'), dict):
                flat_source_record.update(source_record['institution_details'])

            display(f"Flattened source record for mapping: {flat_source_record}")

            # 3. Map the FLATTENED record to standard fields
            source_dict_lower = {str(k).lower(): k for k in flat_source_record.keys() if k}
            mapped_record = {}
            for std_field, (_, src_field) in field_mapping.items():
                if src_field.lower() in source_dict_lower:
                    original_key = source_dict_lower[src_field.lower()]
                    mapped_record[std_field] = flat_source_record.get(original_key)
            
            display(f"Mapped Record (from flattened data): {mapped_record}")

            # 4. Get available criteria and weights
            available_criteria = self._get_available_criteria(mapped_record)
            if not available_criteria:
                display("No matching criteria available in the source record after mapping. Aborting match.")
                return pd.DataFrame()
            
            weights = self._get_normalized_weights(source_record, available_criteria)
            display(f"Available Criteria: {available_criteria}\nCalculated Weights: {weights}")

            # 5. Prefiltering Stage
            potential_matches = cbs_data.copy()

            display(f"Potential Matches Columns: {potential_matches.columns}")

            account_cbs_field, _ = field_mapping.get("account_no", (None, None))
            if account_cbs_field and account_cbs_field in potential_matches.columns:
                # Convert to string and pad with leading zeros to make it 16 digits
                potential_matches[account_cbs_field] = potential_matches[account_cbs_field].astype(str).str.zfill(16)


            prefilter_fields = [("account_no", 0.7), ("citizenship_no", 0.4), ("registration_no", 0.5), ("name", 0.5)]

            display(f"Starting pre-filtering with {len(potential_matches)} total records.")
            for field, threshold in prefilter_fields:
                if field not in available_criteria or potential_matches.empty:
                    continue

                cbs_field, _ = field_mapping.get(field, (None, None))
                source_value = mapped_record.get(field)

                # if not cbs_field or pd.isna(source_value) or source_value == '' or cbs_field not in potential_matches.columns:
                #     display(f"Skipping prefiltering on '{field}' due to missing CBS field or source value.")
                #     continue

                missing_reasons = []
                if not cbs_field:
                    missing_reasons.append("CBS field is None or empty")
                if pd.isna(source_value) or source_value == '':
                    missing_reasons.append("Source value is NaN or empty")
                if cbs_field and cbs_field not in potential_matches.columns:
                    missing_reasons.append(f"CBS field '{cbs_field}' not found in potential_matches columns")
                if missing_reasons:
                    display(f"Skipping prefiltering on '{field}' due to: {', '.join(missing_reasons)}")
                    continue

                if field == "account_no":
                    source_value = str(source_value).zfill(16)

                count_before = len(potential_matches)
                logger.info(f"Prefiltering on '{field}' (CBS field: '{cbs_field}') with threshold >= {threshold}")
                
                similarities = self._calculate_batch_text_similarity(potential_matches[cbs_field], str(source_value))
                potential_matches = potential_matches[similarities >= threshold].copy()
                count_after = len(potential_matches)
                
                display(f"Prefiltering on '{field}': {count_before} -> {count_after} records remaining.")

                if count_after <= MAX_CANDIDATES_AFTER_PREFILTER:
                    logger.info("Candidate count is low, stopping further pre-filtering.")
                    break

            if potential_matches.empty:
                display("No potential matches found after the pre-filtering stage. Aborting match.")
                return pd.DataFrame()
            
            display(f"Prefiltering complete. {len(potential_matches)} candidates remaining for final scoring.")

            similarity_scores = {}
            for criterion in available_criteria:
                cbs_field, _ = field_mapping.get(criterion, (None, None))

                source_value = mapped_record.get(criterion)
                if not cbs_field or pd.isna(source_value) or source_value == '' or cbs_field not in potential_matches.columns:
                    continue

                if criterion in ["citizenship_issue_date", "registration_date", "pan_issue_date", "dob"]:
                    similarity_scores[criterion] = self._calculate_batch_date_similarity(potential_matches[cbs_field], source_value)
                else:
                    if criterion == "account_no":
                        source_value = str(source_value).zfill(16)
                        
                    similarity_scores[criterion] = self._calculate_batch_text_similarity(potential_matches[cbs_field], str(source_value))

            # 7. Final Scoring and Thresholding
            total_scores = pd.Series(0.0, index=potential_matches.index)
            for criterion, weight in weights.items():
                if criterion in similarity_scores:
                    total_scores += similarity_scores[criterion] * weight
            
            display(f"Final scores calculated. Applying threshold: {final_threshold}\n"
                    f"Score Distribution:\n{total_scores.describe()}")

            matches = potential_matches[total_scores >= final_threshold].copy()

            if matches.empty:
                display("No records met the final score threshold. Returning empty DataFrame.")
                return pd.DataFrame()

            # 8. Format and Return Results
            matches["total_score"] = total_scores[matches.index]
            matches["match_status"] = "Matched"
            matches["criteria"] = str(weights)
            for criterion in available_criteria:
                if criterion in similarity_scores:
                    matches[f"{criterion}_score"] = similarity_scores[criterion][matches.index]

            display(f"Match successful. Found {len(matches)} records meeting the threshold.")
            return matches.sort_values("total_score", ascending=False).head(MAX_MATCHES_TO_STORE)

        except Exception as e:
            logger.error(f"FATAL ERROR in fuzzy matching: {str(e)}", exc_info=True)
            logger.error(traceback.format_exc())
            return pd.DataFrame()
        finally:
            gc.collect()


    def get_ticket_matches(
        self,
        cbs_data: pd.DataFrame,
        source_data: Dict,
        ticket_id: Optional[str] = "0",
        final_threshold: float = 0.85,
    ) -> Tuple[str, pd.DataFrame, str]:
        """Match a ticket against CBS data and return a DataFrame with combined results."""

        try:
            source_data["entity_type"] = self._determine_entity_type(source_data)
            name_field = 'person_name' if source_data['entity_type'] == 'individual' else 'company_name'
            
            # Flatten the source data to get the correct name for the ticket
            flat_source_record = source_data.copy()
            if source_data['entity_type'] == 'individual' and isinstance(source_data.get('individual_details'), dict):
                flat_source_record.update(source_data['individual_details'])
            elif source_data['entity_type'] == 'institution' and isinstance(source_data.get('institution_details'), dict):
                flat_source_record.update(source_data['institution_details'])

            ticket_name = f"ticket_{ticket_id}_{str(flat_source_record.get(name_field, '')).replace(' ', '')}"

            matches = self.match(cbs_data, source_data, final_threshold)

            if matches.empty:
                # If no matches, create a DataFrame from the flattened source data
                result_df = pd.DataFrame([flat_source_record])
                result_df["match_status"] = "Unmatched"
                result_df["total_score"] = 0.0
                result_df["ticket_name"] = ticket_name
                return "Unmatched", pd.DataFrame(), ticket_name

            # For "Matched" case, combine source data with each matched CBS record
            result_df = matches.copy()


            """Optional : Append source data to matches"""
            # for key, value in flat_source_record.items():
            #     if isinstance(value, (list, dict)):
            #         result_df[f"source_{key}"] = str(value)
            #     else:
            #         result_df[f"source_{key}"] = value 

            result_df["ticket_name"] = ticket_name
            return "Matched", result_df, ticket_name

        except Exception as e:
            logger.error(f"Error in get_ticket_matches: {str(e)}", exc_info=True)
            logger.error(traceback.format_exc())
            error_df = pd.DataFrame([source_data])
            error_df["match_status"] = "Error"
            ticket_name = f"ticket_{ticket_id}"
            error_df["ticket_name"] = ticket_name
            return "Error", pd.DataFrame(), ticket_name
        finally:
            gc.collect()












# import os
# import re
# import logging
# import traceback
# from typing import Dict, List, Optional, Tuple, Any

# import openpyxl
# import pandas as pd
# from datetime import datetime
# from rapidfuzz import fuzz, process, utils
# from openpyxl.worksheet.datavalidation import DataValidation

# from app.Utils import *
# from app.Constants import *
# from app.components.CbsViewComponent import OracleComponent
# from app.components.WeightageComponent import WeightCalculator

# from qrlib.QRComponent import QRComponent

# logger = logging.getLogger(__name__)


# class FuzzyMatcherComponent(QRComponent):
#     """Component for fuzzy matching with dynamic weightage criteria based on available fields."""

#     def __init__(self):
#         super().__init__()
#         self.weightage_manager = WeightCalculator()
#         # self.oracle_db = OracleComponent()

#     def _preprocess_text(self, text: str) -> str:
#         """Clean and normalize text for comparison."""
#         if pd.isna(text) or text is None:
#             return ""

#         try:
#             return utils.default_process(str(text).replace(" ", ""))
#         except Exception as e:
#             logger.error(f"Error preprocessing text: {e}")
#             return ""

#     def _preprocess_dataframe_column(self, series: pd.Series) -> pd.Series:
#         """Vectorized preprocessing of a DataFrame column."""
#         if series.empty:
#             return series
#         return series.fillna("").astype(str).apply(self._preprocess_text)

#     def _calculate_text_similarity(self, text1: str, text2: str) -> float:
#         """Calculate similarity between two text strings."""
#         if pd.isna(text1) or pd.isna(text2) or text1 is None or text2 is None:
#             return 0.0
#         try:
#             text1 = self._preprocess_text(text1)
#             text2 = self._preprocess_text(text2)

#             if not text1 or not text2:
#                 return 0.0

#             return fuzz.ratio(text1, text2) / 100.0
#         except Exception as e:
#             logger.error(f"Error calculating text similarity: {e}")
#             return 0.0

#     def _calculate_batch_text_similarity(
#         self, series: pd.Series, query_value: str
#     ) -> pd.Series:
#         """Calculate similarity between a series of values and a query value in a vectorized manner."""
#         if series.empty or pd.isna(query_value) or not query_value:
#             return pd.Series(0.0, index=series.index)

#         try:
#             processed_query = self._preprocess_text(query_value)
#             if not processed_query:
#                 return pd.Series(0.0, index=series.index)

#             processed_series = self._preprocess_dataframe_column(series)
#             similarities = process.cdist(
#                 processed_series,
#                 [processed_query],
#                 scorer=fuzz.ratio,
#                 dtype=np.uint8,
#                 workers=max(1, os.cpu_count() // 2),
#             )
#             return pd.Series(similarities.flatten() / 100.0, index=series.index)

#         except Exception as e:
#             logger.error(f"Error in batch text similarity calculation: {e}")
#             return pd.Series(0.0, index=series.index)

#     def _calculate_date_similarity(self, date1: str, date2: str) -> float:
#         """Calculate similarity between two dates."""
#         if pd.isna(date1) or pd.isna(date2) or date1 is None or date2 is None:
#             return 0.0

#         try:
#             d1 = standardize_date_format(date1)
#             d2 = standardize_date_format(date2)

#             if d1 is None or d2 is None:
#                 return 0.0

#             standard_ratio = fuzz.ratio(d1, d2) / 100.0
#             return standard_ratio
#         except Exception as e:
#             logger.info(f"Date comparison failed: {e}")
#             return 0.0

#     def _calculate_batch_date_similarity(
#         self, date_series: pd.Series, query_date: str
#     ) -> pd.Series:
#         """Calculate similarity between a series of dates and a query date in a vectorized manner."""
#         if date_series.empty or pd.isna(query_date) or not query_date:
#             return pd.Series(0.0, index=date_series.index)

#         try:
#             std_query_date = standardize_date_format(query_date)
#             if std_query_date is None:
#                 return pd.Series(0.0, index=date_series.index)

#             std_date_series = date_series.apply(standardize_date_format).fillna("")

#             similarities = process.cdist(
#                 std_date_series,
#                 [std_query_date],
#                 scorer=fuzz.ratio,
#                 dtype=np.uint8,
#                 workers=max(1, os.cpu_count() // 2),
#             )

#             return pd.Series(similarities.flatten() / 100.0, index=date_series.index)

#         except Exception as e:
#             logger.info(f"Batch date comparison failed: {e}")
#             return pd.Series(0.0, index=date_series.index)

#     def _get_available_criteria(self, record: Dict[str, Any]) -> List[str]:
#         """Determine which matching criteria are available in the record."""
#         available_criteria = []

#         try:
#             record_keys_lower = {k.lower(): k for k in record.keys()}

#             field_checks = [
#                 ("name", "name"),
#                 ("pan_no", "pan_no"),
#                 ("grandfathers_name", "grandfathers_name"),
#                 ("registration_no", "registration_no"),
#                 ("fathers_name", "fathers_name"),
#                 ("dob", "dob"),
#                 ("citizenship_no", "citizenship_no"),
#                 ("account_no", "account_no"),
#             ]
#             for field, criteria in field_checks:
#                 field_lower = field.lower()
#                 if field_lower in record_keys_lower:
#                     original_key = record_keys_lower[field_lower]
#                     if not pd.isna(record[original_key]) and record[original_key]:
#                         available_criteria.append(criteria)
#         except Exception as e:
#             logger.error(f"Error determining available criteria: {e}")

#         return available_criteria

#     def _determine_entity_type(self, record: Dict[str, Any]) -> str:
#         """Determine if the record is for an individual, institution, or account."""
#         try:
#             entity_type = record.get("entity_type")

#             if entity_type:
#                 return str(entity_type).lower()

#             if (
#                 "account_no" in record
#                 and not pd.isna(record["account_no"])
#                 and record["account_no"]
#             ):
#                 return "account"

#             if (
#                 "registration_no" in record
#                 and not pd.isna(record["registration_no"])
#                 and record["registration_no"]
#             ) or (
#                 "pan_no" in record
#                 and not pd.isna(record["pan_no"])
#                 and record["pan_no"]
#             ):
#                 return "institution"

#             return "individual"
#         except Exception as e:
#             logger.error(f"Error determining entity type: {e}")
#             return "individual"

#     def _get_normalized_weights(
#         self, record: Dict[str, Any], available_criteria: List[str]
#     ) -> Dict[str, float]:
#         """Get normalized weights for the matching criteria based on entity type."""
#         try:
#             entity_type = self._determine_entity_type(record)

#             logger.info(f"Entity type determined as: {entity_type}")
#             logger.info(f"Available criteria: {available_criteria}")

#             weights = self.weightage_manager.get_weights(
#                 entity_type, available_criteria
#             )
#             logger.info(f"Normalized weights: {weights}")
#             return weights
#         except Exception as e:
#             logger.error(f"Error getting normalized weights: {e}")
#             if available_criteria:
#                 return {
#                     crit: 1.0 / len(available_criteria) for crit in available_criteria
#                 }
#             return {}

#     def match(
#         self,
#         cbs_data: pd.DataFrame,
#         source_record: Dict[str, Any],
#         final_threshold: float = 0.9,
#         field_mapping: Optional[Dict[str, Tuple[str, str]]] = None,
#     ) -> pd.DataFrame:
#         """CORE function to Match a single source record against CBS data using weighted fuzzy matching."""
#         try:
#             if cbs_data.empty:
#                 logger.info("CBS data is empty, no matching possible")
#                 return pd.DataFrame()

#             if field_mapping is None:
#                 field_mapping = FIELD_MAPPING

#             source_dict = source_record
#             source_dict_lower = {k.lower(): k for k in source_dict.keys()}

#             mapped_record = {}
#             for k, (_, source_field) in field_mapping.items():
#                 source_field_lower = source_field.lower()
#                 if source_field_lower in source_dict_lower:
#                     original_key = source_dict_lower[source_field_lower]
#                     mapped_record[k] = source_dict.get(original_key)

#             logger.info(f"Mapped records: {mapped_record}")
#             available_criteria = self._get_available_criteria(mapped_record)

#             if not available_criteria:
#                 logger.info(f"No matching criteria available for source record")
#                 return pd.DataFrame()

#             weights = self._get_normalized_weights(mapped_record, available_criteria)

#             potential_matches = cbs_data.copy()
#             original_count = len(potential_matches)

#             prefilter_fields = [
#                 ("citizenship_no", PREFILTER_THRESHOLD),
#                 ("account_no", PREFILTER_THRESHOLD),
#                 ("name", PREFILTER_THRESHOLD),
#             ]

#             # CALCULATE SIMILARITY ON PREFILTER FIELDS TO FILTER OUT DATA
#             for field, threshold in prefilter_fields:
#                 if field not in available_criteria or potential_matches.empty:
#                     continue

#                 cbs_field, source_field = field_mapping.get(field, (None, None))
#                 if not cbs_field or not source_field or source_field not in source_dict:
#                     continue

#                 source_value = source_dict.get(source_field, "")

#                 if source_dict.get(source_field) == "account_no":
#                     source_value = source_value.zfill(
#                         16
#                     )  # Fill account number with leading zeroes upto 16 digits

#                 if not source_value:
#                     continue

#                 logger.info(f"Running batch similarity for {field} against CBS data")
#                 display(f"Running batch similarity for {field} against CBS data")
#                 similarities = self._calculate_batch_text_similarity(
#                     potential_matches[cbs_field], source_value
#                 )
#                 # filter rows
#                 potential_matches = potential_matches[similarities >= threshold].copy()
#                 logger.info(
#                     f"Filtered from {original_count} to {len(potential_matches)} records based on {field} similarity >= {threshold}"
#                 )

#                 # do not prefilter further if conditate count is already low
#                 if len(potential_matches) <= MAX_CANDIDATES_AFTER_PREFILTER:
#                     break

#             if potential_matches.empty:
#                 logger.warning("No potential matches found after prefiltering")
#                 return pd.DataFrame()

#             # CALCULATE SIMILARITY ON REMAINING FIELDS
#             similarity_scores = {}
#             for criterion in available_criteria:
#                 cbs_field, source_field = field_mapping.get(criterion, (None, None))

#                 if not cbs_field or not source_field or source_field not in source_dict:
#                     continue

#                 source_value = source_dict.get(source_field, "")

#                 if criterion in ["citizenship_issue_date"]:
#                     similarity_scores[criterion] = (
#                         self._calculate_batch_date_similarity(
#                             potential_matches[cbs_field], convert_bs_to_ad(source_value)
#                         )
#                     )
#                 elif criterion in ["dob"]:
#                     similarity_scores[criterion] = (
#                         self._calculate_batch_date_similarity(
#                             potential_matches[cbs_field], source_value
#                         )
#                     )
#                 # for others run Text similarity
#                 else:
#                     similarity_scores[criterion] = (
#                         self._calculate_batch_text_similarity(
#                             potential_matches[cbs_field], source_value
#                         )
#                     )

#             # initialize match score to zero for all data rows
#             total_scores = pd.Series(0.0, index=potential_matches.index)
#             for criterion in available_criteria:
#                 if criterion in similarity_scores:
#                     # perform weighted similarity matching
#                     total_scores += similarity_scores[criterion] * weights.get(
#                         criterion, 0
#                     )

#             # filter by final threshold
#             matches = potential_matches[total_scores >= final_threshold].copy()

#             if matches.empty:
#                 return pd.DataFrame()

#             matches["total_score"] = total_scores[matches.index]
#             matches["match_status"] = "Matched"
#             matches["criteria"] = str(weights)

#             for criterion in available_criteria:
#                 if criterion in similarity_scores:
#                     logger.info(f"{criterion}_score")
#                     matches[f"{criterion}_score"] = similarity_scores[criterion][
#                         matches.index
#                     ]

#             matches = matches.sort_values("total_score", ascending=False)
#             return matches

#         except Exception as e:
#             logger.error(f"Error in fuzzy matching: {str(e)}")
#             import traceback

#             logger.error(traceback.format_exc())
#             return pd.DataFrame()
#         finally:
#             import gc

#             gc.collect()

#     """ OTHER OPTIONAL HELPER FUNCTIONS """

#     def determine_action(self, row, final_threshold: float = 0.75):
#         """Determine the action to take based on the match score and enforcement request."""
#         try:
#             actions = []

#             if 'enforcement_request' not in row or pd.isna(row['enforcement_request']):
#                 return None

#             enforcement_request = str(row['enforcement_request']).lower()
#             if row['total_score'] > final_threshold:
#                 if 'block' in enforcement_request or 'freeze' in enforcement_request:
#                     actions.append('B')
#                 elif enforcement_request == 'unfreeze' or 'release' in enforcement_request:
#                     actions.append('R')
#                 elif "information" in enforcement_request:
#                     actions.append('I')
#                 elif 'debit' in enforcement_request and ('freeze' in enforcement_request or 'block' in enforcement_request):
#                     actions.append('B')
#                 elif 'credit' in enforcement_request and ('freeze' in enforcement_request or 'block' in enforcement_request):
#                     actions.append('C')
#                 else:
#                     actions.append('')

#             return ", ".join(actions) if actions else None

#         except Exception as e:
#             logger.error(f"Error determining action: {e}")
#             logger.error(traceback.print_exc())
#             return None

#     def determine_remarks(self, row) -> str:
#         """Generate remarks based on the institution, chalani number, and letter date."""
#         remarks = "/RPA"

#         if 'institution' in row and 'chalani_no' in row and 'letter_date' in row:
#             institution = str(row.get('institution', ''))
#             if institution in CODE_3:
#                 institution_code = CODE_3[institution]
#                 chalani_no = str(row.get('chalani_no', ''))
#                 letter_date = row.get('letter_date', '')

#                 if isinstance(letter_date, datetime):
#                     letter_date = letter_date.strftime('%Y-%m-%d')
#                 else:
#                     try:
#                         if letter_date:
#                             letter_date = datetime.strptime(letter_date, '%Y-%m-%d').strftime('%Y-%m-%d')
#                         else:
#                             letter_date = str(letter_date)
#                     except ValueError:
#                         letter_date = ''

#                 if chalani_no and letter_date:
#                     remarks = f"/RPA {institution_code} CH NO {chalani_no} DTD {letter_date}"
#         return remarks

#     def add_additional_columns(self, matches: pd.DataFrame) -> pd.DataFrame:
#         """
#         Adds additional columns like SCHM_TYPE, PRODUCT_NAME, AVAILABLE_AMT, etc.
#         for each CIF_ID in  Datthe matchesaFrame by fetching data from an external source.
#         """
#         if matches.empty:
#             return matches

#         additional_columns = [
#             'SCHM_TYPE', 'SCHM_CODE', 'PRODUCT_NAME', 'DEBITCARD', 'CREDITCARD', 'ECOMECARD',
#             'INSTANTCARD', 'CIPS', 'TERMDEPOSIT', 'LOAN', 'MOBILEBANKING', 'PHONELOAN',
#             'AVAILABLE_AMT', 'ACCT_STATUS', 'FREZ_CODE', 'FREZ_REASON_CODE', 'FREZ_REASON_CODE_2',
#             'FREZ_REASON_CODE_3', 'FREZ_REASON_CODE_4', 'FREZ_REASON_CODE_5',
#             'FREEZE_RMKS', 'FREEZE_RMKS2', 'FREEZE_RMKS3', 'FREEZE_RMKS4', 'FREEZE_RMKS5'
#         ]

#         for col in additional_columns:
#             if col not in matches.columns:
#                 matches[col] = ''

#         for index, row in matches.iterrows():
#             acct_num = str(row['ACCT_NUMBER']).zfill(16)
#             logger.info(f"Fetching data for ACCT_NUMBER: {acct_num}")

#             with self.oracle_db as oadb:
#                 customer_data = oadb.fetch_customer_service_data(acct_num)
#                 freeze_data = oadb.fetch_freeze_data(acct_num)

#             if not customer_data:
#                 logger.warning(f"No customer data found for ACCT_NUMBER: {acct_num}")
#                 continue

#             if not freeze_data:
#                 freeze_data = {}

#             for col in additional_columns:
#                 matches.at[index, col] = customer_data.get(col) or freeze_data.get(col) or ''
#         return matches

#     def get_ticket_matches(
#         self,
#         cbs_data: pd.DataFrame,
#         source_data: Dict,
#         ticket_id: Optional[str] = "0",
#         final_threshold: float = 0.85,
#         field_mapping: Optional[Dict[str, Tuple[str, str]]] = None,
#     ) -> Tuple[str, pd.DataFrame, str]:
#         """Match a ticket in source data against CBS data and return a DataFrame with match results."""
#         try:
#             if "entity_type" not in source_data:
#                 source_data["entity_type"] = self._determine_entity_type(source_data)

#             source_record = source_data
#             ticket_name = f"ticket_{ticket_id}"
#             if (
#                 "name" in source_record
#                 and not pd.isna(source_record.get("name"))
#                 and source_record.get("name")
#             ):
#                 ticket_name += f"_{source_record['name'].replace(' ', '')}"

#             matches = self.match(
#                 cbs_data, source_record, final_threshold, field_mapping
#             )
#             is_matched = (
#                 not matches.empty
#                 and "match_status" in matches.columns
#                 and any(matches["match_status"] == "Matched")
#             )

#             # format matches as a df
#             empty_df = pd.DataFrame(columns=COLUMN_ORDER)
#             for col in source_data:
#                 empty_df[f"{col}"] = source_record.get(col, "")
#             matches = empty_df
#             matches["ticket_name"] = ticket_name
#             match_status = "Matched" if is_matched else "Unmatched"
#             return match_status, matches, ticket_name

#         except Exception as e:
#             logger.error(f"Error in match_all_tickets: {str(e)}")
#             import traceback

#             logger.error(traceback.format_exc())
#             empty_df = pd.DataFrame(columns=COLUMN_ORDER)
#             ticket_name = f"ticket_{ticket_id}"
#             if (
#                 "name" in source_data
#                 and not pd.isna(source_data.get("name"))
#                 and source_data.get("name")
#             ):
#                 ticket_name += f"_{source_data['name'].replace(' ', '')}"
#             return "Error", empty_df, ticket_name

#         finally:
#             import gc

#             gc.collect()

#     def match_all_tickets(self, cbs_data: pd.DataFrame, source_data: Dict, ticket_id: Optional[str] = "0",
#                         final_threshold: float = 0.95,
#                         field_mapping: Optional[Dict[str, Tuple[str, str]]] = None) -> Tuple[str, pd.DataFrame, str]:
#         """Match a ticket in  CBS data source data againstand return a DataFrame with match results."""
#         try:
#             if 'entity_type' not in source_data:
#                 source_data['entity_type'] = self._determine_entity_type(source_data)

#             source_record = source_data
#             ticket_name = f"ticket_{ticket_id}"
#             if 'name' in source_record and not pd.isna(source_record.get('name')) and source_record.get('name'):
#                 ticket_name += f"_{source_record['name'].replace(' ', '')}"

#             matches = self.match(
#                 cbs_data,
#                 source_record,
#                 final_threshold,
#                 field_mapping
#             )
#             is_matched = not matches.empty and 'match_status' in matches.columns and any(matches['match_status'] == 'Matched')

#             if not matches.empty:
#                 for col in source_data:
#                     matches[f'{col}'] = source_record.get(col, "")

#                 for col in COLUMN_ORDER:
#                     if col not in matches.columns:
#                         matches[col] = ""

#                 available_columns = [col for col in COLUMN_ORDER if col in matches.columns]
#                 matches = matches[available_columns]

#                 matches['action'] = matches.apply(
#                     lambda row: self.determine_action(row, 0.98),
#                     axis=1
#                 )
#                 has_action = matches['action'].notna() & ~matches['action'].isin(['', 'I', 'R'])
#                 matches.loc[~has_action, 'remarks'] = ''
#                 matches.loc[has_action, 'remarks'] = matches[has_action].apply(
#                     self.determine_remarks,
#                     axis=1
#                 )

#                 # matches = self.add_additional_columns(matches)
#             else:
#                 empty_df = pd.DataFrame(columns=COLUMN_ORDER)
#                 for col in source_data:
#                     empty_df[f'{col}'] = source_record.get(col, "")

#                 matches = empty_df

#             matches['ticket_name'] = ticket_name
#             match_status = "Matched" if is_matched else "Unmatched"
#             return match_status, matches, ticket_name

#         except Exception as e:
#             logger.error(f"Error in match_all_tickets: {str(e)}")
#             import traceback
#             logger.error(traceback.format_exc())
#             empty_df = pd.DataFrame(columns=COLUMN_ORDER)
#             ticket_name = f"ticket_{ticket_id}"
#             if 'name' in source_data and not pd.isna(source_data.get('name')) and source_data.get('name'):
#                 ticket_name += f"_{source_data['name'].replace(' ', '')}"
#             return "Error", empty_df, ticket_name

#         finally:
#             import gc
#             gc.collect()


# def process_tickets_from_excel(
#     cbs_data: pd.DataFrame,
#     tickets_data: Dict,
#     ticket_id: str,
#     final_threshold: float = 0.85,
#     patra_number: Optional[str] = None,
#     chalani_number: Optional[str] = None,
#     output_filename: Optional[str] = None,
#     output_dir: Optional[str] = None
# ) -> Tuple[str, str, str]:
#     """
#     Process a ticket and generate matched results and write to Excel file with enhanced chalani group support
#     """
#     try:
#         field_mapping = FIELD_MAPPING
#         matcher = FuzzyMatcherComponent()
#         is_patra_group = bool(patra_number or output_filename)
#         unmatched_columns = [
#             'ticket_id', 'patra_number', 'chalani_number', 'name', 'fathers_name','grandfathers_name',
#             'dob', 'citizenship_no',
#             'pan_no', 'registration_no', 'account_no', 'nid', 'entity_type',
#             'enforcement_request', 'subject', 'letter_head', 'chalani_no',
#             'letter_date', 'institution'
#         ]

#         match_status, matches_df, ticket_name = matcher.match_all_tickets(
#             cbs_data=cbs_data,
#             source_data=tickets_data,
#             ticket_id=ticket_id,
#             final_threshold=final_threshold,
#             field_mapping=field_mapping
#         )

#         output_dir = 'matched_results'
#         matched_dir = os.path.join(output_dir)
#         os.makedirs(matched_dir, exist_ok=True)

#         if is_patra_group:
#             filename = os.path.join(matched_dir, output_filename)

#             if os.path.exists(filename):
#                 try:
#                     matched_df = pd.read_excel(filename, sheet_name='Matched', dtype={"ACCT_NUMBER": str, "account_no": str})
#                 except:
#                     matched_df = pd.DataFrame()
#                 try:
#                     unmatched_df = pd.read_excel(filename, sheet_name='Unmatched', dtype={"account_no": str})
#                 except:
#                     unmatched_df = pd.DataFrame(columns=unmatched_columns)
#             else:
#                 matched_df = pd.DataFrame(columns=COLUMN_ORDER)
#                 unmatched_df = pd.DataFrame(columns=unmatched_columns)

#             if not matches_df.empty:
#                 matches_df = matches_df.reindex(columns=COLUMN_ORDER)

#                 if matched_df.empty:
#                     matched_df = matches_df.copy()
#                 else:
#                     matched_df = matched_df.reindex(columns=COLUMN_ORDER)

#                     for col in COLUMN_ORDER:
#                         if col in matched_df.columns and col in matches_df.columns:
#                             matched_df[col] = matched_df[col].astype(str)
#                             matches_df[col] = matches_df[col].astype(str)

#                     matched_df = pd.concat([matched_df, matches_df], ignore_index=True)
#             else:
#                 unmatched_row = {
#                     'ticket_id': ticket_id,
#                     'patra_number': patra_number,
#                     'name': tickets_data.get('name'),
#                     'fathers_name': tickets_data.get('fathers_name'),
#                     'grandfathers_name': tickets_data.get('grandfathers_name'),
#                     'dob': tickets_data.get('dob'),
#                     'citizenship_no': tickets_data.get('citizenship_no'),
#                     'pan_no': tickets_data.get('pan_no'),
#                     'registration_no': tickets_data.get('registration_no'),
#                     'account_no': tickets_data.get('account_no'),
#                     'nid': tickets_data.get('nid'),
#                     'entity_type': tickets_data.get('entity_type'),
#                     'enforcement_request': tickets_data.get('enforcement_request'),
#                     'subject': tickets_data.get('subject'),
#                     'letter_head': tickets_data.get('letter_head'),
#                     'chalani_no': tickets_data.get('chalani_no'),
#                     'letter_date': tickets_data.get('letter_date'),
#                     'institution': tickets_data.get('institution'),
#                 }
#                 for col in unmatched_row:
#                     if col not in unmatched_df.columns:
#                         unmatched_df[col] = ""

#                 temp_df = pd.DataFrame([{col: unmatched_row.get(col, "") for col in unmatched_df.columns}])
#                 unmatched_df = pd.concat([unmatched_df, temp_df], ignore_index=True)

#             with pd.ExcelWriter(filename, engine='openpyxl') as writer:
#                 matched_df.to_excel(writer, sheet_name='Matched', index=False)
#                 unmatched_df.to_excel(writer, sheet_name='Unmatched', index=False)

#                 workbook = writer.book
#                 matched_sheet = writer.sheets['Matched']

#                 acct_col_letter = None
#                 acct_number_letter = None
#                 action_col_letter = None
#                 reason_code_letter = None
#                 for idx, header in enumerate(matches_df.columns, 1):
#                     if header == 'account_no':
#                         acct_col_letter = openpyxl.utils.get_column_letter(idx)
#                     if header == 'ACCT_NUMBER':
#                         acct_number_letter = openpyxl.utils.get_column_letter(idx)
#                     if header == 'action':
#                         action_col_letter = openpyxl.utils.get_column_letter(idx)
#                     if header == 'reason_code':
#                         reason_code_letter = openpyxl.utils.get_column_letter(idx)

#                 if acct_col_letter:
#                     for row in range(2, matched_sheet.max_row + 1):
#                         cell = matched_sheet[f"{acct_col_letter}{row}"]
#                         cell.number_format = '@'

#                 if acct_number_letter:
#                     for row in range(2, matched_sheet.max_row + 1):
#                         cell = matched_sheet[f"{acct_number_letter}{row}"]
#                         cell.number_format = '@'


#                 if action_col_letter:
#                     last_row = matched_sheet.max_row
#                     if last_row < 2:
#                         last_row = 2
#                     cell_range = f"{action_col_letter}2:{action_col_letter}{last_row}"
#                     dv = DataValidation(type="list", formula1='"T,B,C,R,I"', allow_blank=True)
#                     matched_sheet.add_data_validation(dv)
#                     dv.add(cell_range)

#                 if reason_code_letter:
#                     last_row = matched_sheet.max_row
#                     if last_row < 2:
#                         last_row = 2
#                     reason_code = DataValidation(type="list", formula1='"INVST,NRBBL,NRBRQ"', allow_blank=True)
#                     reason_code_cell = f"{reason_code_letter}2:{reason_code_letter}{last_row}"
#                     matched_sheet.add_data_validation(reason_code)
#                     reason_code.add(reason_code_cell)

#             return match_status, filename, ticket_name
#         else:
#             filename = os.path.join(matched_dir, f"{ticket_name}.xlsx")
#             if not matches_df.empty:
#                 with pd.ExcelWriter(filename, engine='openpyxl') as writer:
#                     matches_df.to_excel(writer, sheet_name='Matched', index=False)
#                     workbook = writer.book
#                     matches_sheet = writer.sheets['Matched']

#                     acct_col_letter = None
#                     acct_number_letter = None
#                     action_col_letter = None
#                     reason_code_letter = None
#                     for idx, header in enumerate(matches_df.columns, 1):
#                         if header == 'account_no':
#                             acct_col_letter = openpyxl.utils.get_column_letter(idx)
#                         if header == 'ACCT_NUMBER':
#                             acct_number_letter = openpyxl.utils.get_column_letter(idx)
#                         if header == 'action':
#                             action_col_letter = openpyxl.utils.get_column_letter(idx)
#                         if header == 'reason_code':
#                             reason_code_letter = openpyxl.utils.get_column_letter(idx)

#                     if acct_col_letter:
#                         for row in range(2, matches_sheet.max_row + 1):
#                             cell = matches_sheet[f"{acct_col_letter}{row}"]
#                             cell.number_format = '@'

#                     if acct_number_letter:
#                         for row in range(2, matches_sheet.max_row + 1):
#                             cell = matches_sheet[f"{acct_number_letter}{row}"]
#                             cell.number_format = '@'

#                     if action_col_letter:
#                         last_row = matches_sheet.max_row
#                         if last_row < 2:
#                             last_row = 2
#                         cell_range = f"{action_col_letter}2:{action_col_letter}{last_row}"
#                         dv = DataValidation(type="list", formula1='"T,B,C,R,I"', allow_blank=True)
#                         matches_sheet.add_data_validation(dv)
#                         dv.add(cell_range)

#                     if reason_code_letter:
#                         last_row = matches_sheet.max_row
#                         if last_row < 2:
#                             last_row = 2
#                         reason_code = DataValidation(type="list", formula1='"INVST,NRBBL,NRBRQ"', allow_blank=True)
#                         reason_code_cell = f"{reason_code_letter}2:{reason_code_letter}{last_row}"
#                         matches_sheet.add_data_validation(reason_code)
#                         reason_code.add(reason_code_cell)

#                 return match_status, filename, ticket_name
#             else:
#                 logger.warning(f"No matches found for ticket {ticket_id}. Skipping Excel file generation.")
#                 return "Unmatched", "", ticket_name

#     except Exception as e:
#         logger.error(f"Error in get_ticket_matches: {str(e)}")
#         import traceback
#         logger.error(traceback.format_exc())
#         return "Error", "", ""
