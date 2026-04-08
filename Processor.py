# =============================================================================

# LFI Quarterly Fraud Report Processor

# =============================================================================

# Central Bank of UAE — Fraud Prevention Department

# 

# PURPOSE:

# Reads quarterly fraud submission Excel files from 50+ LFIs stored in an

# HDFS-backed Dataiku managed folder. Each file contains 5 sheets. This

# recipe processes each sheet, transforms it (wide → long where applicable),

# and writes 5 combined output datasets — one per sheet type — plus a

# processing audit log.

# 

# INPUTS:

# - Dataiku Managed Folder (HDFS): one .xlsx file per LFI

# 

# OUTPUTS:

# - agg_issuer_card_volumes      : aggregated issuer card fraud volumes

# - txn_issuer_card_fraud        : transaction-level issuer card fraud

# - txn_acquirer_card_fraud      : transaction-level acquirer card fraud

# - agg_transfers_ip_volumes     : aggregated transfers and IP volumes

# - txn_transfers_ip_fraud       : transaction-level transfers and IP fraud

# - lfi_processing_status        : audit log — one row per sheet per LFI

# 

# DESIGN NOTES:

# - Files are read once from HDFS and reused across all sheet processors

# - Output datasets are written incrementally (streaming) to avoid

# accumulating all dataframes in memory before writing

# - Each sheet failure is isolated — one bad sheet does not affect others

# - The reporting_period variable must be set in Dataiku’s flow variables

# =============================================================================

import dataiku
import pandas as pd
import logging
import io
import traceback
from dataclasses import dataclass, field
from typing import Callable, Optional
from datetime import datetime

# ── Logging ───────────────────────────────────────────────────────────────────

# Dataiku surfaces these logs in the recipe job diagnostics panel.

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

# =============================================================================

# CONFIGURATION

# =============================================================================

# Edit this section when the reporting template changes or LFI list is updated.

# Nothing below this section should need to change for routine quarterly runs.

# Dataiku managed folder name (HDFS-backed)

FOLDER_NAME = “lfi_quarterly_reports”

# Output dataset for the processing audit log

STATUS_OUTPUT_DATASET = “lfi_processing_status”

# Reporting period — pulled from Dataiku flow variables.

# Set this in the flow before each quarterly run (e.g. “Q1_2025”).

# Used to stamp all output rows independently of what LFIs put in their files.

REPORTING_PERIOD = dataiku.get_custom_variables().get(“reporting_period”, “UNKNOWN”)

# Known LFI codes — used to validate filenames and catch malformed submissions.

# Add or remove codes as your LFI universe changes.

KNOWN_LFIS = {
“FAB”, “ADCB”, “ENBD”, “DIB”, “MASHREQ”,
“CBD”, “RAK”, “NBAD”, “SIB”, “ADIB”,
# Add remaining LFIs here
}

# =============================================================================

# STATUS TRACKING CONSTANTS

# =============================================================================

STATUS_SUCCESS = “SUCCESS”
STATUS_FAILED  = “FAILED”
STATUS_MISSING = “MISSING_SHEET”

STAGE_FILE_READ = “FILE_READ”    # Could not read or parse the Excel file at all
STAGE_LOAD      = “LOAD”         # File read OK but sheet could not be loaded
STAGE_PROCESS   = “PROCESS”      # Sheet loaded but transformation failed
STAGE_SAVE      = “SAVE”         # Processing OK but Dataiku write failed

# =============================================================================

# SHEET CONFIGURATION

# =============================================================================

@dataclass
class SheetConfig:
“””
Defines everything the processor needs to know about one sheet type.

```
id_vars:    columns to preserve as identifier columns.
value_vars: columns to melt into long format. If None, the sheet is
            transaction-level and rows are stacked as-is without melting.
post_process: optional function(df) -> df for any sheet-specific cleaning
              that does not generalise to other sheets.
"""
sheet_name:     str
output_dataset: str
id_vars:        list
value_vars:     Optional[list] = None
var_name:       str = "metric"
value_name:     str = "value"
post_process:   Optional[Callable] = None
```

# One entry per sheet. This is the only place to change column definitions

# when the reporting template is updated.

SHEET_CONFIGS = [
SheetConfig(
sheet_name     = “Agg_Issuer_Card_Volumes”,
output_dataset = “agg_issuer_card_volumes”,
id_vars        = [“lfi_name”, “reporting_period”, “report_date”, “card_scheme”],
value_vars     = [“total_txn_count”, “total_txn_value”,
“fraud_txn_count”, “fraud_txn_value”],
),
SheetConfig(
sheet_name     = “Txn_Issuer_Card_Fraud”,
output_dataset = “txn_issuer_card_fraud”,
id_vars        = [“lfi_name”, “reporting_period”, “report_date”,
“txn_id”, “card_scheme”, “fraud_type”],
value_vars     = None,   # Transaction-level: stack rows, no melt
),
SheetConfig(
sheet_name     = “Txn_Acquirer_Card_Fraud”,
output_dataset = “txn_acquirer_card_fraud”,
id_vars        = [“lfi_name”, “reporting_period”, “report_date”,
“txn_id”, “merchant_category”, “fraud_type”],
value_vars     = None,
),
SheetConfig(
sheet_name     = “Agg_Transfers_IP_Volumes”,
output_dataset = “agg_transfers_ip_volumes”,
id_vars        = [“lfi_name”, “reporting_period”, “report_date”, “transfer_type”],
value_vars     = [“total_txn_count”, “total_txn_value”,
“fraud_txn_count”, “fraud_txn_value”],
),
SheetConfig(
sheet_name     = “Txn_Transfers_IP_Fraud”,
output_dataset = “txn_transfers_ip_fraud”,
id_vars        = [“lfi_name”, “reporting_period”, “report_date”,
“txn_id”, “transfer_type”, “fraud_type”],
value_vars     = None,
),
]

# =============================================================================

# STATUS TRACKER

# =============================================================================

@dataclass
class SheetStatus:
“”“One record in the audit log — represents one sheet from one LFI file.”””
lfi_name:         str
file_name:        str
sheet_name:       str
output_dataset:   str
status:           str
stage:            str
reporting_period: str
row_count:        Optional[int] = None
error_message:    Optional[str] = None
error_detail:     Optional[str] = None   # Full traceback for debugging
processed_at:     str = field(
default_factory=lambda: datetime.utcnow().isoformat()
)

class StatusTracker:
“””
Collects processing outcomes for every sheet across every LFI file.
Written as a single audit table at the end of the batch run.

```
Captures three distinct failure modes:
  FILE_READ  — the Excel file itself could not be opened or parsed
  LOAD       — file OK but the specific sheet is missing or unreadable
  PROCESS    — sheet loaded but the transformation raised an error
  SAVE       — processing OK but the Dataiku dataset write failed
"""

def __init__(self, output_dataset: str, reporting_period: str):
    self.output_dataset   = output_dataset
    self.reporting_period = reporting_period
    self._records: list[SheetStatus] = []

def _record(self, status: SheetStatus) -> None:
    self._records.append(status)

def log_success(self, lfi_name: str, file_name: str, sheet_name: str,
                output_dataset: str, stage: str,
                row_count: int = None) -> None:
    self._record(SheetStatus(
        lfi_name         = lfi_name,
        file_name        = file_name,
        sheet_name       = sheet_name,
        output_dataset   = output_dataset,
        status           = STATUS_SUCCESS,
        stage            = stage,
        reporting_period = self.reporting_period,
        row_count        = row_count,
    ))

def log_missing_sheet(self, lfi_name: str, file_name: str,
                      sheet_name: str, output_dataset: str) -> None:
    self._record(SheetStatus(
        lfi_name         = lfi_name,
        file_name        = file_name,
        sheet_name       = sheet_name,
        output_dataset   = output_dataset,
        status           = STATUS_MISSING,
        stage            = STAGE_LOAD,
        reporting_period = self.reporting_period,
        error_message    = f"Sheet '{sheet_name}' not found in file.",
    ))

def log_failure(self, lfi_name: str, file_name: str, sheet_name: str,
                output_dataset: str, stage: str, exc: Exception) -> None:
    self._record(SheetStatus(
        lfi_name         = lfi_name,
        file_name        = file_name,
        sheet_name       = sheet_name,
        output_dataset   = output_dataset,
        status           = STATUS_FAILED,
        stage            = stage,
        reporting_period = self.reporting_period,
        error_message    = str(exc),
        error_detail     = traceback.format_exc(),
    ))

def log_file_read_failure(self, lfi_name: str, file_name: str,
                           exc: Exception) -> None:
    """
    Called when the file itself cannot be read or parsed.
    Marks all 5 expected sheets as failed so the audit log is complete —
    i.e. every LFI always has a row for every sheet, regardless of outcome.
    """
    for cfg in SHEET_CONFIGS:
        self.log_failure(
            lfi_name       = lfi_name,
            file_name      = file_name,
            sheet_name     = cfg.sheet_name,
            output_dataset = cfg.output_dataset,
            stage          = STAGE_FILE_READ,
            exc            = exc,
        )

def to_dataframe(self) -> pd.DataFrame:
    return pd.DataFrame([vars(r) for r in self._records])

def write(self) -> None:
    df = self.to_dataframe()
    ds = dataiku.Dataset(self.output_dataset)
    ds.write_with_schema(df)
    logger.info(f"Audit log written → {self.output_dataset} ({len(df)} records)")

def print_summary(self) -> None:
    """Prints a summary to the job log after all files are processed."""
    df      = self.to_dataframe()
    success = (df["status"] == STATUS_SUCCESS).sum()
    failed  = (df["status"] == STATUS_FAILED).sum()
    missing = (df["status"] == STATUS_MISSING).sum()

    logger.info("")
    logger.info("=" * 55)
    logger.info("  BATCH PROCESSING SUMMARY")
    logger.info(f"  Reporting period     : {self.reporting_period}")
    logger.info(f"  Total sheet attempts : {len(df)}")
    logger.info(f"  Successful           : {success}")
    logger.info(f"  Failed               : {failed}")
    logger.info(f"  Missing sheets       : {missing}")
    logger.info("=" * 55)

    # Surface any failures prominently so they're easy to spot in job logs
    failures = df[df["status"] != STATUS_SUCCESS][
        ["lfi_name", "sheet_name", "stage", "status", "error_message"]
    ]
    if not failures.empty:
        logger.warning("  FILES / SHEETS REQUIRING ATTENTION:")
        for _, row in failures.iterrows():
            logger.warning(
                f"    [{row['status']}] {row['lfi_name']} | "
                f"{row['sheet_name']} | Stage: {row['stage']} | "
                f"{row['error_message']}"
            )
    logger.info("")
```

# =============================================================================

# SHEET PROCESSOR

# =============================================================================

class SheetProcessor:
“””
Handles extraction, transformation, and incremental writing for one sheet
type across all LFI files.

```
Uses Dataiku's streaming writer (get_writer) instead of accumulating all
dataframes in memory. This is important for transaction-level sheets which
can be large — each processed sheet is written immediately after extraction
rather than held in RAM until all 50 files are done.

Lifecycle:
    open()      — call once before the file loop starts
    extract()   — call once per LFI file
    close()     — call once after all files are processed
"""

def __init__(self, config: SheetConfig):
    self.config       = config
    self._writer      = None
    self._total_rows  = 0
    self._files_ok    = 0
    self._files_failed= 0

def open(self) -> None:
    """
    Opens the Dataiku streaming writer for this output dataset.
    Must be called before the file processing loop begins.
    The writer holds an open connection to the dataset — Dataiku
    commits and closes it only when close() is called.
    """
    ds           = dataiku.Dataset(self.config.output_dataset)
    self._writer = ds.get_writer()
    logger.info(f"Opened writer → {self.config.output_dataset}")

def extract(self, file_bytes: bytes, lfi_name: str, file_name: str,
            available_sheets: list, tracker: StatusTracker) -> None:
    """
    Processes one sheet from one LFI file.

    Steps:
      1. Check the sheet exists in the file
      2. Load the sheet into a dataframe
      3. Transform (normalise columns, stamp identifiers, melt if needed)
      4. Write immediately to the output dataset via the streaming writer
    """

    # Step 1: Check sheet presence
    if self.config.sheet_name not in available_sheets:
        tracker.log_missing_sheet(
            lfi_name       = lfi_name,
            file_name      = file_name,
            sheet_name     = self.config.sheet_name,
            output_dataset = self.config.output_dataset,
        )
        self._files_failed += 1
        return

    # Step 2: Load the sheet
    try:
        df = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name = self.config.sheet_name,
        )
    except Exception as exc:
        tracker.log_failure(
            lfi_name       = lfi_name,
            file_name      = file_name,
            sheet_name     = self.config.sheet_name,
            output_dataset = self.config.output_dataset,
            stage          = STAGE_LOAD,
            exc            = exc,
        )
        self._files_failed += 1
        return

    # Step 3: Transform
    try:
        df = self._transform(df, lfi_name)
    except Exception as exc:
        tracker.log_failure(
            lfi_name       = lfi_name,
            file_name      = file_name,
            sheet_name     = self.config.sheet_name,
            output_dataset = self.config.output_dataset,
            stage          = STAGE_PROCESS,
            exc            = exc,
        )
        self._files_failed += 1
        return

    # Step 4: Write immediately — no accumulation in memory
    try:
        self._writer.write_dataframe(df)
        self._total_rows  += len(df)
        self._files_ok    += 1
        tracker.log_success(
            lfi_name       = lfi_name,
            file_name      = file_name,
            sheet_name     = self.config.sheet_name,
            output_dataset = self.config.output_dataset,
            stage          = STAGE_PROCESS,
            row_count      = len(df),
        )
        logger.info(
            f"  OK  [{self.config.sheet_name}] "
            f"{lfi_name}: {len(df)} rows written"
        )
    except Exception as exc:
        tracker.log_failure(
            lfi_name       = lfi_name,
            file_name      = file_name,
            sheet_name     = self.config.sheet_name,
            output_dataset = self.config.output_dataset,
            stage          = STAGE_SAVE,
            exc            = exc,
        )
        self._files_failed += 1

def _transform(self, df: pd.DataFrame, lfi_name: str) -> pd.DataFrame:
    """
    Applies standard transformations to a loaded sheet:
      - Normalise column names (strip whitespace, lowercase, underscores)
      - Stamp lfi_name and reporting_period as explicit columns
      - Validate all expected columns are present
      - Melt wide → long for aggregated sheets
      - Apply any sheet-specific post-processing
    """
    df = df.copy()

    # Normalise column names so minor formatting differences in the
    # submitted Excel don't cause column validation failures
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # Stamp source identifiers — these are our columns, not the LFI's
    df["lfi_name"]         = lfi_name
    df["reporting_period"] = REPORTING_PERIOD

    # Validate required columns are present after normalisation
    required = self.config.id_vars + (self.config.value_vars or [])
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns after normalisation: {missing}. "
            f"Actual columns found: {list(df.columns)}"
        )

    # Aggregated sheets: melt wide → long
    # Transaction-level sheets: stack rows as-is
    if self.config.value_vars:
        df = df.melt(
            id_vars    = self.config.id_vars,
            value_vars = self.config.value_vars,
            var_name   = self.config.var_name,
            value_name = self.config.value_name,
        )
    else:
        # Keep id columns first, then all remaining columns
        other_cols = [c for c in df.columns if c not in self.config.id_vars]
        df = df[self.config.id_vars + other_cols]

    # Sheet-specific post-processing (e.g. date parsing, currency conversion)
    if self.config.post_process:
        df = self.config.post_process(df)

    return df

def close(self, tracker: StatusTracker) -> None:
    """
    Closes the streaming writer and logs the final save outcome.
    Must be called after all LFI files have been processed.
    """
    if self._writer is None:
        return

    try:
        self._writer.close()
        tracker.log_success(
            lfi_name       = "ALL_LFIS",
            file_name      = "COMBINED",
            sheet_name     = self.config.sheet_name,
            output_dataset = self.config.output_dataset,
            stage          = STAGE_SAVE,
            row_count      = self._total_rows,
        )
        logger.info(
            f"Closed writer → {self.config.output_dataset} | "
            f"Total rows: {self._total_rows} | "
            f"Files OK: {self._files_ok} | "
            f"Files failed: {self._files_failed}"
        )
    except Exception as exc:
        tracker.log_failure(
            lfi_name       = "ALL_LFIS",
            file_name      = "COMBINED",
            sheet_name     = self.config.sheet_name,
            output_dataset = self.config.output_dataset,
            stage          = STAGE_SAVE,
            exc            = exc,
        )
```

# =============================================================================

# FILE HELPERS

# =============================================================================

def parse_lfi_name(file_name: str) -> str:
“””
Extracts the LFI code from the submitted filename.

```
Assumes LFI code is the first underscore-delimited token in uppercase.
e.g. "FAB_Q1_2025_v2.xlsx" → "FAB"
     "ADCB.xlsx"           → "ADCB"

Logs a warning if the parsed code is not in the known LFI list —
the file is still processed but flagged for review.
"""
base     = file_name.replace(".xlsx", "").replace(".XLSX", "")
lfi_code = base.split("_")[0].upper()

if lfi_code not in KNOWN_LFIS:
    logger.warning(
        f"Unrecognised LFI code '{lfi_code}' parsed from '{file_name}'. "
        f"File will be processed but should be reviewed."
    )

return lfi_code
```

def read_file_bytes(folder: dataiku.Folder, path: str) -> bytes:
“””
Reads a file from an HDFS-backed Dataiku managed folder into memory.

```
Dataiku's get_download_stream() abstracts the HDFS read — the calling
code does not need to handle HDFS clients directly. The file is read
into a bytes buffer once and reused across all sheet processors,
avoiding multiple HDFS reads for the same file.
"""
with folder.get_download_stream(path) as stream:
    return stream.read()
```

def list_excel_files(folder: dataiku.Folder) -> list:
“””
Returns all .xlsx file paths in the managed folder.
On HDFS-backed folders, list_paths_in_partition() traverses the
HDFS directory tree that Dataiku manages for this folder.
“””
return [
p for p in folder.list_paths_in_partition()
if p.lower().endswith(”.xlsx”)
]

# =============================================================================

# BATCH RUNNER

# =============================================================================

def run_batch(folder_name: str,
processors: list,
tracker: StatusTracker) -> None:
“””
Main batch loop. For each LFI file in the managed folder:
1. Read the raw bytes from HDFS (once per file)
2. Parse the Excel structure to get available sheet names
3. Run all sheet processors against the file
4. Log any file-level failures to the tracker

```
After all files are processed:
  5. Close all streaming writers
  6. Write the audit log
  7. Print a summary to the job log
"""

folder = dataiku.Folder(folder_name)
paths  = list_excel_files(folder)

logger.info("")
logger.info(f"Starting batch — {len(paths)} files found in '{folder_name}'")
logger.info(f"Reporting period: {REPORTING_PERIOD}")
logger.info("")

# Open all streaming writers before the loop
for processor in processors:
    processor.open()

# ── File loop ─────────────────────────────────────────────────────────────
for i, path in enumerate(paths, start=1):
    file_name = path.split("/")[-1]
    lfi_name  = parse_lfi_name(file_name)

    logger.info(f"[{i}/{len(paths)}] Processing: {lfi_name} ({file_name})")

    # Step 1: Read raw bytes from HDFS
    try:
        file_bytes = read_file_bytes(folder, path)
    except Exception as exc:
        logger.warning(f"  HDFS read failed for {file_name}: {exc}")
        tracker.log_file_read_failure(lfi_name, file_name, exc)
        continue

    # Step 2: Parse Excel structure — get sheet names without loading data.
    # pd.ExcelFile reads only the workbook index, keeping memory usage low.
    try:
        xl               = pd.ExcelFile(io.BytesIO(file_bytes))
        available_sheets = xl.sheet_names
        logger.info(f"  Sheets available: {available_sheets}")
    except Exception as exc:
        logger.warning(f"  Could not parse Excel structure: {exc}")
        tracker.log_file_read_failure(lfi_name, file_name, exc)
        continue

    # Step 3: Run each sheet processor against this file
    for processor in processors:
        processor.extract(
            file_bytes       = file_bytes,
            lfi_name         = lfi_name,
            file_name        = file_name,
            available_sheets = available_sheets,
            tracker          = tracker,
        )

# ── Post-loop ─────────────────────────────────────────────────────────────
# Close all writers — this commits the data to Dataiku
for processor in processors:
    processor.close(tracker)

# Write audit log and print summary
tracker.write()
tracker.print_summary()
```

# =============================================================================

# MAIN

# =============================================================================

processors = [SheetProcessor(cfg) for cfg in SHEET_CONFIGS]
tracker    = StatusTracker(STATUS_OUTPUT_DATASET, REPORTING_PERIOD)

run_batch(FOLDER_NAME, processors, tracker)
