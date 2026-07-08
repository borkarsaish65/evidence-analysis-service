"""
Report Service
Handles report generation, data retrieval, and export
"""
import csv
import re
from datetime import datetime, timedelta
from io import StringIO
import logging
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from core.config import settings
from core.constants import (
    RELEVANCE_TYPES,
    RELEVANCE_TAG_RELEVANT,
    RELEVANCE_TAG_PARTIAL,
    RELEVANCE_TAG_IRRELEVANT,
    RELEVANCE_TAG_NOT_VALIDATED,
)
from models.execution import Execution
from models.schemas import ReportDataPageResponse, ReportDownloadResponse, ReportResponse
from services.storage_service import StorageService

logger = logging.getLogger(__name__)

REQUIRED_REPORT_COLUMNS = [
    "UUID",
    "Declared State",
    "District",
    "Block",
    "School Name",
    "Tasks",
    "Project ID",
    "Project start date of the user",
    "Project completion date of the user",
    "Relevance Tag",
]


class ReportCsvNotFoundError(Exception):
    """Raised when execution/report CSV is not available."""


class ReportCsvConflictError(Exception):
    """Raised when execution is not in report-ready state."""


class ReportCsvValidationError(Exception):
    """Raised when CSV content is invalid for report rendering."""


class ReportService:
    """Service for report operations"""
    
    def __init__(self, db: Session):
        self.db = db
        self.storage_service = StorageService()

    def _get_execution(self, execution_id: UUID, user_id: str) -> Optional[Execution]:
        return self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id
        ).first()
    
    def get_report(self, execution_id: UUID, user_id: str) -> Optional[ReportResponse]:
        """Get report data for completed execution"""
        execution = self._get_execution(execution_id, user_id)
        
        if not execution or execution.status != 'completed':
            return None
        
        # Load output CSV data
        output_data = self._load_output_data(execution.output_file_url)
        input_data = self._load_input_data(execution.input_file_url)
        
        metadata = {
            "execution_name": execution.name,
            "created_at": execution.created_at.isoformat(),
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            "total_rows": execution.total_rows,
            "processed_rows": execution.processed_rows,
            "average_processing_time": float(execution.average_processing_time) if execution.average_processing_time else None,
            "ai_model": execution.ai_model_id,
            "status": execution.status
        }
        
        return ReportResponse(
            execution_id=execution.id,
            input_data=input_data,
            output_data=output_data,
            metadata=metadata
        )
    
    def _load_output_data(self, file_url: str) -> Optional[Dict[str, Any]]:
        """Load and parse output CSV"""
        if not file_url:
            return None
        
        try:
            # This is a placeholder - actual implementation will parse CSV
            return {
                "file_url": file_url,
                "summary": "Output data summary"
            }
        except Exception as e:
            logger.error(f"Failed to load output data: {str(e)}")
            return None
    
    def _load_input_data(self, file_url: str) -> Optional[Dict[str, Any]]:
        """Load and parse input CSV"""
        if not file_url:
            return None
        
        try:
            return {
                "file_url": file_url,
                "summary": "Input data summary"
            }
        except Exception as e:
            logger.error(f"Failed to load input data: {str(e)}")
            return None
    
    async def get_output_download_url(self, execution_id: UUID, user_id: str) -> Optional[ReportDownloadResponse]:
        """Return a short-lived signed download URL for the output CSV."""
        execution = self._get_execution(execution_id, user_id)
        
        if not execution or not execution.output_file_url:
            return None

        signed_download = await self.storage_service.generate_download_url(
            file_path=execution.output_file_url,
            expiration=settings.SIGNED_DOWNLOAD_URL_EXPIRY_SECONDS,
            response_filename=f"execution_{execution_id}_output.csv",
        )
        return ReportDownloadResponse(
            download_url=signed_download["url"],
            expires_in_seconds=signed_download["expires_in_seconds"],
        )

    async def get_output_csv_content(self, execution_id: UUID, user_id: str) -> str:
        """Download output CSV from cloud/local storage and validate report structure."""
        execution = self._get_execution(execution_id, user_id)
        if not execution:
            raise ReportCsvNotFoundError("Report not found")

        if execution.status != "completed":
            raise ReportCsvConflictError("Execution is not completed yet")

        if not execution.output_file_url:
            raise ReportCsvNotFoundError("Output CSV file is not available")

        file_bytes = await self.storage_service.download_file(execution.output_file_url)
        if not file_bytes:
            raise ReportCsvNotFoundError("Output CSV file not found in storage")

        csv_text: Optional[str] = None
        decode_errors: list[str] = []
        for encoding in ("utf-8-sig", "utf-8"):
            try:
                csv_text = file_bytes.decode(encoding)
                break
            except UnicodeDecodeError as exc:
                decode_errors.append(str(exc))

        if csv_text is None:
            logger.error(
                "Failed decoding report CSV for execution_id=%s: %s",
                execution_id,
                " | ".join(decode_errors),
            )
            raise ReportCsvValidationError("Output CSV is corrupted or not UTF-8 encoded")

        self._validate_report_csv_structure(csv_text)
        return csv_text

    def _validate_report_csv_structure(self, csv_text: str) -> None:
        try:
            reader = csv.DictReader(StringIO(csv_text))
        except csv.Error as exc:
            logger.error("Invalid CSV structure while reading header: %s", exc)
            raise ReportCsvValidationError("Invalid CSV structure")

        raw_fieldnames = list(reader.fieldnames or [])
        if not raw_fieldnames:
            raise ReportCsvValidationError("CSV header row is missing")

        normalized_headers: list[str] = []
        for index, name in enumerate(raw_fieldnames):
            cleaned = (name or "").strip()
            if index == 0:
                cleaned = cleaned.lstrip("\ufeff")
            normalized_headers.append(cleaned)

        missing_columns = [column for column in REQUIRED_REPORT_COLUMNS if column not in normalized_headers]
        if missing_columns:
            raise ReportCsvValidationError(
                f"Missing required CSV columns: {', '.join(missing_columns)}"
            )

        has_non_empty_row = False
        try:
            for row in reader:
                if any(str(value or "").strip() for value in row.values()):
                    has_non_empty_row = True
                    break
        except csv.Error as exc:
            logger.error("Invalid CSV row structure: %s", exc)
            raise ReportCsvValidationError("Invalid CSV structure")

        if not has_non_empty_row:
            raise ReportCsvValidationError("CSV contains no data rows")
    
    async def get_report_data_page(
        self,
        execution_id: UUID,
        user_id: str,
        page: int,
        page_size: int,
        filters: Optional[Dict[str, str]] = None,
    ) -> ReportDataPageResponse:
        """
        Return pre-aggregated summary + paginated raw rows for report rendering.
        Replaces the full-CSV download so the browser never holds 100k rows.
        """
        execution = self._get_execution(execution_id, user_id)
        if not execution:
            raise ReportCsvNotFoundError("Report not found")
        if execution.status != "completed":
            raise ReportCsvConflictError("Execution is not completed yet")
        if not execution.output_file_url:
            raise ReportCsvNotFoundError("Output CSV file is not available")

        file_bytes = await self.storage_service.download_file(execution.output_file_url)
        if not file_bytes:
            raise ReportCsvNotFoundError("Output CSV file not found in storage")

        csv_text: Optional[str] = None
        for encoding in ("utf-8-sig", "utf-8"):
            try:
                csv_text = file_bytes.decode(encoding)
                break
            except UnicodeDecodeError:
                pass
        if csv_text is None:
            raise ReportCsvValidationError("Output CSV is corrupted or not UTF-8 encoded")

        reader = csv.DictReader(StringIO(csv_text))
        headers = [h.strip().lstrip("﻿") for h in (reader.fieldnames or [])]

        # Load all rows (needed to compute global summary + filter options)
        all_rows = [{k.strip(): (v or "").strip() for k, v in row.items()} for row in reader]

        # Build active filters map
        active = {k: v.strip() for k, v in (filters or {}).items() if (v or "").strip()}

        def _matches(row: Dict[str, str]) -> bool:
            if active.get("state") and row.get("Declared State", "") != active["state"]:
                return False
            if active.get("district") and row.get("District", "") != active["district"]:
                return False
            if active.get("block") and row.get("Block", "") != active["block"]:
                return False
            if active.get("school") and row.get("School Name", "") != active["school"]:
                return False
            if active.get("relevance") and row.get("Relevance Tag", "") != active["relevance"]:
                return False
            return True

        filtered_rows = [r for r in all_rows if _matches(r)]
        total_all = len(all_rows)
        total_filtered = len(filtered_rows)

        offset = (page - 1) * page_size
        page_rows = filtered_rows[offset: offset + page_size]

        filter_options = self._compute_filter_options(all_rows, active)
        summary = self._compute_report_summary(filtered_rows, filter_options)

        return ReportDataPageResponse(
            page=page,
            page_size=page_size,
            total_filtered=total_filtered,
            total_all=total_all,
            rows=page_rows,
            headers=headers,
            summary=summary,
        )

    @staticmethod
    def _compute_filter_options(
        all_rows: list[Dict[str, str]], active: Dict[str, str]
    ) -> Dict[str, list[str]]:
        """Compute cascading filter option lists from all rows (one pass)."""
        state_set: set[str] = set()
        district_set: set[str] = set()
        block_set: set[str] = set()
        school_set: set[str] = set()

        active_state = active.get("state", "")
        active_district = active.get("district", "")
        active_block = active.get("block", "")

        for row in all_rows:
            s = row.get("Declared State", "")
            d = row.get("District", "")
            b = row.get("Block", "")
            sc = row.get("School Name", "")

            if s:
                state_set.add(s)
            if not active_state or s == active_state:
                if d:
                    district_set.add(d)
                if not active_district or d == active_district:
                    if b:
                        block_set.add(b)
                    if not active_block or b == active_block:
                        if sc:
                            school_set.add(sc)

        return {
            "states": sorted(state_set),
            "districts": sorted(district_set),
            "blocks": sorted(block_set),
            "schools": sorted(school_set),
        }

    @staticmethod
    def _compute_report_summary(
        rows: list[Dict[str, str]], filter_options: Dict[str, list[str]]
    ) -> Dict[str, Any]:
        """
        Python equivalent of StandardReportRenderer.jsx::computeReportData().
        Returns the same aggregated structure the frontend expects.
        """
        # notValidated = row never sent to the AI because the relevant-evidence cap was reached
        # for this (UUID, Tasks) pair. (Evidence-type-excluded rows are dropped entirely by the
        # pre-processor and never reach this output, so they don't appear here at all.) It's a
        # distinct bucket: counted in "total" but never folded into Relevant/Partially
        # Relevant/Irrelevant, and excluded from rel_score's numerator and denominator.
        # "null" = row has no Relevance Tag at all — the processor's no-question-found skip path
        # (task not in the questions sheet) appends None rather than tagging the row, so it never
        # reached the AI either. Same treatment as notValidated: counted in "total", never folded
        # into Relevant/Partially Relevant/Irrelevant, excluded from rel_score.
        RELEVANCE_NULL_BUCKET = "null"
        MAX_TOP = 15

        def create_node() -> Dict[str, Any]:
            return {
                "total": 0,
                RELEVANCE_TAG_RELEVANT: 0,
                RELEVANCE_TAG_PARTIAL: 0,
                RELEVANCE_TAG_IRRELEVANT: 0,
                RELEVANCE_TAG_NOT_VALIDATED: 0,
                RELEVANCE_NULL_BUCKET: 0,
            }

        def update_node(node: Dict[str, Any], tag: str) -> None:
            node["total"] += 1
            if tag in RELEVANCE_TYPES:
                node[tag] += 1
            else:
                node[RELEVANCE_NULL_BUCKET] += 1

        def rel_score(node: Dict[str, Any]) -> float:
            # notValidated and null rows were never evaluated (relevant-cap reached, or no
            # question found for the task) — excluding them from the denominator keeps the
            # score scoped to evidence the AI actually evaluated. "total" itself is left
            # untouched; it must still include these rows everywhere else.
            evaluated = node["total"] - node.get(RELEVANCE_TAG_NOT_VALIDATED, 0) - node.get(RELEVANCE_NULL_BUCKET, 0)
            return ((node[RELEVANCE_TAG_RELEVANT] + node[RELEVANCE_TAG_PARTIAL] * 0.5) / evaluated * 100) if evaluated else 0.0

        def parse_subject(task: str) -> str:
            if "विज्ञान" in task:
                return "Science"
            if "गणित" in task:
                return "Math"
            if "अंग्रेजी" in task or "English" in task:
                return "English"
            return "Other"

        def parse_grade(task: str) -> str:
            m = re.search(r"कक्षा\s*(\d{1,2})", task)
            return f"Grade {m.group(1)}" if m else "Unknown"

        def parse_dt(date_str: str) -> Optional[datetime]:
            if not date_str:
                return None
            # fromisoformat handles +05:30 / -05:00 timezone offsets (Python 3.7+).
            # Normalize trailing Z → +00:00 for Python < 3.11 compatibility.
            s = date_str.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s).replace(tzinfo=None)
            except ValueError:
                pass
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    pass
            return None

        def get_group_key(date_str: str, unit: str) -> Optional[str]:
            dt = parse_dt(date_str)
            if not dt:
                return None
            if unit == "day":
                return dt.strftime("%Y-%m-%d")
            if unit == "month":
                return dt.strftime("%Y-%m")
            # week: start on Sunday (JS compat)
            js_day = (dt.weekday() + 1) % 7  # 0=Sun, …6=Sat
            start = dt - timedelta(days=js_day)
            return start.strftime("%Y-%m-%d")

        def get_timeline_unit(min_dt: Optional[datetime], max_dt: Optional[datetime]) -> str:
            if not min_dt or not max_dt:
                return "month"
            if min_dt.month == max_dt.month and min_dt.year == max_dt.year:
                return "day"
            diff_days = abs((max_dt - min_dt).days)
            return "week" if diff_days <= 183 else "month"

        # Accumulation structures
        relevance_counts = {
            RELEVANCE_TAG_RELEVANT: 0,
            RELEVANCE_TAG_PARTIAL: 0,
            RELEVANCE_TAG_IRRELEVANT: 0,
            RELEVANCE_TAG_NOT_VALIDATED: 0,
            RELEVANCE_NULL_BUCKET: 0,
        }
        states_set: set[str] = set()
        users_set: set[str] = set()
        schools_set: set[str] = set()
        districts_set: set[str] = set()
        blocks_set: set[str] = set()
        subject_counts: Dict[str, int] = {}
        grade_counts: Dict[str, int] = {}
        task_counts: Dict[str, int] = {}

        timeline: Dict[str, Dict[str, Dict[str, int]]] = {
            u: {"starts": {}, "completions": {}, "submissions": {}, "relevant": {}}
            for u in ("day", "week", "month")
        }

        district_hierarchy: Dict[str, Any] = {}
        state_summaries: Dict[str, Any] = {}
        state_district_stats: Dict[str, Any] = {}
        teacher_map: Dict[str, Any] = {}
        enrollment: Dict[str, Any] = {}
        has_enrollment_data = False
        custom_task_hierarchy: Dict[str, Any] = {}
        has_custom_tasks = False
        min_date: Optional[datetime] = None
        max_date: Optional[datetime] = None

        for row in rows:
            tag = row.get("Relevance Tag", "")
            district = row.get("District", "") or "Unknown"
            block = row.get("Block", "") or "Unknown"
            school = row.get("School Name", "") or "Unknown"
            uuid = row.get("UUID", "") or "Unknown User"
            task = row.get("Tasks", "") or "Unknown Task"
            state = row.get("Declared State", "")
            state_name = state or "Unknown State"

            if state:
                states_set.add(state.upper())
            if tag in RELEVANCE_TYPES:
                relevance_counts[tag] += 1
            else:
                relevance_counts[RELEVANCE_NULL_BUCKET] += 1
            if uuid:
                users_set.add(uuid)
            if school:
                schools_set.add(school)
            if district:
                districts_set.add(district)
            if block:
                blocks_set.add(block)

            if state_name not in state_summaries:
                state_summaries[state_name] = create_node()
            update_node(state_summaries[state_name], tag)

            if state_name not in state_district_stats:
                state_district_stats[state_name] = {}
            if district not in state_district_stats[state_name]:
                state_district_stats[state_name][district] = create_node()
            update_node(state_district_stats[state_name][district], tag)

            subject_counts[parse_subject(task)] = subject_counts.get(parse_subject(task), 0) + 1
            grade_counts[parse_grade(task)] = grade_counts.get(parse_grade(task), 0) + 1
            task_counts[task] = task_counts.get(task, 0) + 1

            start_str = row.get("Project start date of the user", "")
            end_str = row.get("Project completion date of the user", "")

            for ds in (start_str, end_str):
                dt = parse_dt(ds)
                if dt:
                    if not min_date or dt < min_date:
                        min_date = dt
                    if not max_date or dt > max_date:
                        max_date = dt

            for unit in ("day", "week", "month"):
                sk = get_group_key(start_str, unit)
                ck = get_group_key(end_str, unit)
                t = timeline[unit]
                if sk:
                    t["starts"][sk] = t["starts"].get(sk, 0) + 1
                if ck:
                    t["completions"][ck] = t["completions"].get(ck, 0) + 1
                    t["submissions"][ck] = t["submissions"].get(ck, 0) + 1
                    if tag == RELEVANCE_TAG_RELEVANT:
                        t["relevant"][ck] = t["relevant"].get(ck, 0) + 1

            if district not in district_hierarchy:
                district_hierarchy[district] = {**create_node(), "blocks": {}}
            update_node(district_hierarchy[district], tag)
            dh = district_hierarchy[district]["blocks"]
            if block not in dh:
                dh[block] = {**create_node(), "schools": {}}
            update_node(dh[block], tag)
            if school not in dh[block]["schools"]:
                dh[block]["schools"][school] = create_node()
            update_node(dh[block]["schools"][school], tag)

            tk = f"{district}||{block}||{school}||{uuid}"
            if tk not in teacher_map:
                teacher_map[tk] = {**create_node(), "district": district, "block": block, "school": school, "teacher": uuid}
            update_node(teacher_map[tk], tag)

            # Enrollment
            def _flt(key: str) -> Optional[float]:
                try:
                    v = float(row.get(key, "") or "")
                    return v if v == v else None  # exclude NaN
                except (ValueError, TypeError):
                    return None

            e24, e25, eg = _flt("Enrollment_2024"), _flt("Enrollment_2025"), _flt("Enrollment_Increase_Percentage")
            if e24 is not None or e25 is not None or eg is not None:
                has_enrollment_data = True
                for d_key, b_key, s_key in [(district, None, None), (district, block, None), (district, block, school)]:
                    if b_key is None:
                        node = enrollment.setdefault(d_key, {"enrollment2024": 0, "enrollment2025": 0, "count2024": 0, "count2025": 0, "growthSum": 0, "growthCount": 0, "blocks": {}})
                    elif s_key is None:
                        node = enrollment[d_key]["blocks"].setdefault(b_key, {"enrollment2024": 0, "enrollment2025": 0, "count2024": 0, "count2025": 0, "growthSum": 0, "growthCount": 0, "schools": {}})
                    else:
                        node = enrollment[d_key]["blocks"][b_key]["schools"].setdefault(s_key, {"enrollment2024": 0, "enrollment2025": 0, "count2024": 0, "count2025": 0, "growthSum": 0, "growthCount": 0})
                    if e24 is not None:
                        node["enrollment2024"] += e24; node["count2024"] += 1
                    if e25 is not None:
                        node["enrollment2025"] += e25; node["count2025"] += 1
                    if eg is not None:
                        node["growthSum"] += eg; node["growthCount"] += 1

            # Custom tasks
            if row.get("Task Type", "") == "User-Owned":
                has_custom_tasks = True
                if district not in custom_task_hierarchy:
                    custom_task_hierarchy[district] = {**create_node(), "blocks": {}}
                update_node(custom_task_hierarchy[district], tag)
                cb = custom_task_hierarchy[district]["blocks"]
                if block not in cb:
                    cb[block] = {**create_node(), "schools": {}}
                update_node(cb[block], tag)
                if school not in cb[block]["schools"]:
                    cb[block]["schools"][school] = create_node()
                update_node(cb[block]["schools"][school], tag)

        # Post-process
        tl_unit = get_timeline_unit(min_date, max_date)
        tl = timeline[tl_unit]
        tl_labels = sorted(set(list(tl["starts"]) + list(tl["completions"])))
        sub_labels = sorted(set(list(tl["submissions"]) + list(tl["relevant"])))

        task_entries = sorted(
            [(t, c) for t, c in task_counts.items() if t],
            key=lambda x: -x[1],
        )[:15]

        # sortedDistricts: list of [district, districtData] pairs, sorted alphabetically
        sorted_districts = [[d, data] for d, data in sorted(district_hierarchy.items())]

        # Build spatial teacher index for topHierarchy
        teachers_by_school: Dict[str, list] = {}
        for t_data in teacher_map.values():
            sk = f"{t_data['district']}||{t_data['block']}||{t_data['school']}"
            teachers_by_school.setdefault(sk, []).append(t_data)

        # Build topHierarchy (top 5 districts by relevance score)
        top_hierarchy = []
        for dist, d_data in sorted(district_hierarchy.items(), key=lambda x: -rel_score(x[1]))[:5]:
            top_blocks = []
            for blk, b_data in sorted(d_data["blocks"].items(), key=lambda x: -rel_score(x[1]))[:MAX_TOP]:
                top_schools = []
                for sch, s_data in sorted(b_data["schools"].items(), key=lambda x: -rel_score(x[1]))[:MAX_TOP]:
                    raw_teachers = teachers_by_school.get(f"{dist}||{blk}||{sch}", [])
                    top_teachers = sorted(
                        [{**t, "relevancePercent": rel_score(t)} for t in raw_teachers],
                        key=lambda x: -x["relevancePercent"],
                    )[:MAX_TOP]
                    top_schools.append({**s_data, "school": sch, "relevancePercent": rel_score(s_data), "teachers": top_teachers})
                top_blocks.append({**b_data, "block": blk, "relevancePercent": rel_score(b_data), "schools": top_schools, "blocks": None})
            top_hierarchy.append({**d_data, "district": dist, "relevancePercent": rel_score(d_data), "blocks": top_blocks})

        return {
            "totalEvidence": len(rows),
            "relevanceCounts": relevance_counts,
            "usersCount": len(users_set),
            "schoolsCount": len(schools_set),
            "districtsCount": len(districts_set),
            "blocksCount": len(blocks_set),
            "statesUpper": list(states_set),
            "subjectCounts": subject_counts,
            "gradeCounts": grade_counts,
            "timelineLabels": tl_labels,
            "timelineStarts": [tl["starts"].get(lbl, 0) for lbl in tl_labels],
            "timelineCompletions": [tl["completions"].get(lbl, 0) for lbl in tl_labels],
            "submissionLabels": sub_labels,
            "submissionTotal": [tl["submissions"].get(lbl, 0) for lbl in sub_labels],
            "submissionRelevant": [tl["relevant"].get(lbl, 0) for lbl in sub_labels],
            "taskEntries": task_entries,
            "districtHierarchy": district_hierarchy,
            "stateSummaries": state_summaries,
            "stateDistrictStats": state_district_stats,
            "sortedDistricts": sorted_districts,
            "topHierarchy": top_hierarchy,
            "hasEnrollmentData": has_enrollment_data,
            "enrollment": enrollment,
            "hasCustomTasks": has_custom_tasks,
            "customTaskHierarchy": custom_task_hierarchy,
            "filterOptions": filter_options,
        }

    def generate_html_report(self, execution_id: UUID, user_id: str) -> Optional[str]:
        """Generate HTML report (evidence-analysis UI parity)"""
        execution = self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id
        ).first()
        
        if not execution or execution.status != 'completed':
            return None
        
        # Placeholder HTML - will be enhanced with actual report template
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Execution Report - {execution.name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #1976d2; color: white; padding: 20px; }}
                .content {{ margin-top: 20px; }}
                .metric {{ display: inline-block; margin: 10px; padding: 15px; background: #f5f5f5; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{execution.name}</h1>
                <p>Status: {execution.status}</p>
            </div>
            <div class="content">
                <div class="metric">
                    <strong>Total Rows:</strong> {execution.total_rows}
                </div>
                <div class="metric">
                    <strong>Processed:</strong> {execution.processed_rows}
                </div>
                <div class="metric">
                    <strong>Avg Time:</strong> {execution.average_processing_time}s
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
