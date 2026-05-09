# src/civic_ledger/api/tools.py

import uuid
from datetime import datetime, timezone
from fastapi import APIRouter
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.api.models import (
    ToolResponse, SubmitCaseRequest, StartAgentSessionRequest,
    CategorizeCaseRequest, RecordUrgencyRequest,
    PolicyCheckRequest, PolicyCheckResultRequest,
    GenerateRecommendationRequest, HumanReviewRequest,
    EscalateRequest, PublishRequest, IntegrityCheckRequest
)

router = APIRouter(prefix="/tools", tags=["mcp-tools"])


@router.post("/submit_case", response_model=ToolResponse)
def submit_case(req: SubmitCaseRequest):
    store = EventStore()
    payload = {
        "case_id": req.case_id,
        "source": req.source,
        "description": req.description,
        "location": req.location,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }
    event = store.append(
        stream_id=f"case-{req.case_id}",
        expected_last_position=0,
        event_type="CaseSubmitted",
        payload_dict=payload,
        metadata_dict={"correlation_id": req.correlation_id or str(uuid.uuid4()), "source": req.source}
    )
    return ToolResponse(event_id=str(event["event_id"]), stream_id=event["stream_id"],
                        version=event["stream_position"], message="Case submitted")


@router.post("/start_agent_session", response_model=ToolResponse)
def start_agent_session(req: StartAgentSessionRequest):
    from src.civic_ledger.commands.handlers import StartAgentSessionCommand, handle_start_agent_session
    cmd = StartAgentSessionCommand(**req.model_dump())
    event = handle_start_agent_session(cmd, EventStore())
    return ToolResponse(
        event_id=str(event["event_id"]),
        stream_id=event["stream_id"],
        version=event["stream_position"],
        message="Agent session started"
    )


@router.post("/categorize_case", response_model=ToolResponse)
def categorize_case(req: CategorizeCaseRequest):
    from src.civic_ledger.commands.handlers import CaseCategorizedCommand, handle_case_categorized
    cmd = CaseCategorizedCommand(**req.model_dump())
    result = handle_case_categorized(cmd, EventStore())
    last_event = result["case_events"][-1]
    return ToolResponse(event_id=str(last_event["event_id"]), stream_id=f"case-{req.case_id}",
                        version=last_event["stream_position"], message="Case categorized")


@router.post("/record_urgency", response_model=ToolResponse)
def record_urgency(req: RecordUrgencyRequest):
    from src.civic_ledger.commands.handlers import UrgencyScoredCommand, handle_urgency_scored
    cmd = UrgencyScoredCommand(**req.model_dump())
    result = handle_urgency_scored(cmd, EventStore())
    return ToolResponse(event_id=str(result["case_event"]["event_id"]), stream_id=f"case-{req.case_id}",
                        version=result["case_event"]["stream_position"], message="Urgency scored")


@router.post("/request_policy_check", response_model=ToolResponse)
def request_policy_check(req: PolicyCheckRequest):
    from src.civic_ledger.commands.handlers import handle_record_policy_check
    event = handle_record_policy_check(EventStore(), req.case_id, req.rule_id,
                                       req.regulation_version, req.correlation_id)
    return ToolResponse(event_id=str(event["event_id"]) , stream_id=event["stream_id"],
                        version=event["stream_position"], message="Policy check requested")


@router.post("/record_policy_check_result", response_model=ToolResponse)
def record_policy_check_result(req: PolicyCheckResultRequest):
    from src.civic_ledger.commands.handlers import handle_record_policy_result
    event = handle_record_policy_result(EventStore(), req.case_id, req.rule_id,
                                        req.passed, req.detail, req.correlation_id)
    return ToolResponse(event_id=str(event["event_id"]), stream_id=event["stream_id"],
                        version=event["stream_position"], message="Policy check result recorded")


@router.post("/generate_recommendation", response_model=ToolResponse)
def generate_recommendation(req: GenerateRecommendationRequest):
    from src.civic_ledger.commands.handlers import GenerateRecommendationCommand, handle_generate_recommendation
    cmd = GenerateRecommendationCommand(**req.model_dump())
    result = handle_generate_recommendation(cmd, EventStore())
    return ToolResponse(event_id=str(result["case_event"]["event_id"]), stream_id=f"case-{req.case_id}",
                        version=result["case_event"]["stream_position"], message="Recommendation generated")


@router.post("/human_review", response_model=ToolResponse)
def human_review(req: HumanReviewRequest):
    from src.civic_ledger.commands.handlers import HumanReviewCommand, handle_human_review
    cmd = HumanReviewCommand(**req.model_dump())
    result = handle_human_review(cmd, EventStore())
    return ToolResponse(event_id=str(result["case_event"]["event_id"]), stream_id=f"case-{req.case_id}",
                        version=result["case_event"]["stream_position"], message="Human review recorded")


@router.post("/escalate", response_model=ToolResponse)
def escalate(req: EscalateRequest):
    from src.civic_ledger.commands.handlers import EscalateCommand, handle_escalate
    cmd = EscalateCommand(**req.model_dump())
    result = handle_escalate(cmd, EventStore())
    return ToolResponse(event_id=str(result["case_event"]["event_id"]), stream_id=f"case-{req.case_id}",
                        version=result["case_event"]["stream_position"], message="Case escalated")


@router.post("/publish", response_model=ToolResponse)
def publish(req: PublishRequest):
    from src.civic_ledger.commands.handlers import PublishCommand, handle_publish
    cmd = PublishCommand(**req.model_dump())
    result = handle_publish(cmd, EventStore())
    return ToolResponse(event_id=str(result["case_event"]["event_id"]), stream_id=f"case-{req.case_id}",
                        version=result["case_event"]["stream_position"], message="Case published")


@router.post("/run_integrity_check", response_model=ToolResponse)
def run_integrity_check(req: IntegrityCheckRequest):
    store = EventStore()
    stream_id = f"{req.entity_type}-{req.entity_id}"
    ok = store.verify_stream_integrity(stream_id)
    message = "Integrity verified" if ok else "Tampering detected"
    return ToolResponse(event_id=None, stream_id=stream_id, version=None, message=message)
